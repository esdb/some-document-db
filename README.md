# quokka

document database built on top of mysql

![architecture](https://docs.google.com/drawings/d/1_UjPesdIb8zaIfdkcs-qRwkZVQYxzLvuqRkXuK6uMuI/pub?w=820&amp;h=585)

There are two main parts

* the main storage
* the views

The main storage will store the authentic copy of data in a reliable way. It will also need to handle any conflict
of business rule (such as balance never become negative) in a thread safe way.

The views will represent the data in different kind of indices to answer queries faster.
It might also merge multiple sources into one view.

The goal is to build a document database that will have these two parts acting as one, with these features:

* Good old technology: only depends on mysql to function properly
* Schema free: the main storage takes JSON
* Handle update in a idempotent way: free user from doing this hard work himself
* Keep business rule on data integrity: it should work like database unique constraint.
* High performance on concurrent update to same entity: more than 10k tps on same entity 
* View can be easily implemented: do not impose limit on view storage, mysql/redis/elasticsearch they all can be view storage.
* Reliable view update: the view can not have loss update, every change should be synchronized
* Low latency view update: if user want, the view can be updated in the same request handing the main storage
* Long term stability: historic data should be automatically archived properly

# Main storage correctness

the main storage table is defined by this sql (assuming the entity is called account)

```sql
CREATE TABLE `v2pro`.`account` (
  `event_id`     BIGINT       NOT NULL       AUTO_INCREMENT,
  `entity_id`    CHAR(20)     NOT NULL,
  `version`      BIGINT       NOT NULL,
  `command_id`   VARCHAR(256) NOT NULL,
  `command_name` VARCHAR(256) NOT NULL,
  `request`      JSON         NULL,
  `response`     JSON         NOT NULL,
  `state`        JSON         NOT NULL,
  `committed_at` DATETIME     NOT NULL       DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`event_id`),
  UNIQUE KEY `unique_version` (`entity_id`, `version`),
  UNIQUE KEY `unique_command` (`entity_id`, `command_id`)
);
```

The process to update one entity

* load the old state
* handle the request, generate response and new state
* save the new state with version + 1

If there are concurrent update the same entity, the unique_version constraint will prevent the later one being saved.
The user defined handler can enforce any kind of business rule to avoid state being updated to a unexpected new state.

Another key concern is to be idempotent. As long as the command id is same, the response should be the same.
This is achieved by unique_command constraint. If same command being handled twice, the first response will be always used.

As long as mysql unique constraint is working properly, we are able to keep our promises.
We do not rely on etcd to shard the traffic to corresponding application server reliably.
We do not do any decision solely based on application server local cache.
We do not store any persistent local state on the application server.
We choose to keep the application server stateless, and use the good old mysql to ensure transaction safety.
All of that is based on two unique indices.

# Main storage performance

Mysql can not provide more than 10k tps update on same entity. We need to do something more.
In essence, wee need to make the application server "stateful" so that we can

* queue on the application server instead of pushing the load to the database
* batch on the application server, so we can write in bigger chunk for better throughput

However, this "stateful" is just a performance optimization. We do not rely on the state to keep the logic right.
Because we are not relying the "state" on application server, when we lost the state or we screwed up the sharding,
data integrity will not be compromised. Everything still works if traffic not sharded, just slower.

So, we shard the application server, so that request to same entity always hit same application server.
Then, we queue the requests up and batch process them. The internal queue is just a golang channel. 
Batching is implemented by "select" on the channel.

```golang
func (worker *worker) fetchCommands() []*command {
	done := false
	commands := []*command{}
	for !done {
		select {
		case cmd := <-worker.commandQ:
			commands = append(commands, cmd)
		default:
			done = true
		}
		if len(commands) > 1000 {
			break
		}
	}
	return commands
}
```

The sharding is done by this simple logic:

* use etcd to elect one server as the leader: this is optional, we can choose to use a static topology
* every server keep a heart beat to the elected leader
* when doing heart beat, leader will piggy back the latest shard allocation, every server locally cache it
* any request can hit any server, the server will redirect it to the right shard
* when server received a redirected request, it will handle it unconditionally

The key design decisions are:
 
* we only use etcd to elect a leader, we do not use it to allocate shard. 
losing leader will not stop the traffic. only the topology is no longer the latest, 
which might result in more lock contention.
* request will only be redirected once, there is no harm when two servers are handling same shard, 
only performance will be downgraded (due to optimistic lock contention)
* every server is both "gateway" and "command handler"

# Reliable view update

We do not use mysql binlog to synchronize view. Instead the entity table is partitioned like kafka partition.
event_id is a auto increment big int works like kafka offset. 
In essence, every entity table partition is a event queue.
The view updater will scan the event table and write update to the view side. There two things to watch for

* How to ensure idempotence on view update, if the synchronization is retried?
* How to remember the last updated offset?

The updated offset can be recorded as a mysql table. Every entity table partition will have a highest sync offset.
We batch process the update, and only update the offset record every second.

Because the offset is not recorded in the view side with transaction, and committed on every write,
the update to the view will be retried. We record the entity version on the view side. 
When we update the view, we compare the event (which contains entity version) with the applied version.
If one event has already been applied, we skip it.

Having a separate view updater run in the background every 100ms, 
we can ensure we do not lose the update on the view side.
However, the table polling will result in high latency.

# Low latency view update

We allow synchronous view update directly from command handler, for latency sensitive view. 
When a command is committed, the view update is done immediately afterwards by same worker goroutine.
This way we can cut down the hand over time to nearly zero, there is no queue can beat this.
However, there things to care about 

* update view directly from command handler means there will be concurrent updates.
* update view might fail.

The concurrent update will be handled by the optimistic lock on the view side, same as view updater retry.
Update view might fail. We might lose update if only rely on direct update from command handler.
Luckily, we have separate view updater working in "pull" mode to ensure integrity.
It is just like a patch for "push" mode view update.

# Closing thought

Both the main storage and views are built upon optimistic lock. 
And sharding & queueing is a optimization to reduce the lock contention.