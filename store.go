package quokka

import (
	"time"
	"fmt"
	"database/sql/driver"
	"runtime/debug"
	"github.com/v2pro/plz/sql"
)

type HandleCommand func(request interface{}, state interface{}) (response interface{}, newState interface{}, err error)

type entityStore struct {
	cfg                 *frozenConfig
	entityName          string
	insertSql           sql.Translated
	getLatestStateSql   sql.Translated
	getEventSql         sql.Translated
	commandHandlers     map[string]HandleCommand
	commandRequestTypes map[string]func() interface{}
	stateType           func() interface{}
}

type command struct {
	entityId        string
	commandId       string
	commandName     string
	request         []byte
	replied         string
	responsePromise chan interface{}
}

func (cmd *command) reply(response interface{}) {
	if cmd.replied != "" {
		panic("already replied: " + cmd.replied)
	}
	cmd.replied = string(debug.Stack())
	cmd.responsePromise <- response
}

func (cmd *command) delayReply(response interface{}) func() {
	return func() {
		cmd.reply(response)
	}
}

type worker struct {
	store       *entityStore
	conn        sql.Conn
	commandQ    chan *command
	entityCache map[string]*Entity
}

func StoreOf(entityName string) *entityStore {
	return ConfigDefault.StoreOf(entityName)
}

func (cfg *frozenConfig) StoreOf(entityName string) *entityStore {
	insertSql := sql.Translate(
		"INSERT "+entityName+" :INSERT_COLUMNS",
		"entity_id", "version", "command_id", "command_name", "request", "response", "state")
	getLatestStateSql := sql.Translate(
		"SELECT * FROM " + entityName + " WHERE entity_id=:entity_id ORDER BY version DESC LIMIT 1")
	getEventSql := sql.Translate(
		"SELECT * FROM " + entityName + " WHERE entity_id=:entity_id AND command_id=:command_id")
	return &entityStore{
		cfg:                 cfg,
		entityName:          entityName,
		insertSql:           insertSql,
		getLatestStateSql:   getLatestStateSql,
		getEventSql:         getEventSql,
		commandHandlers:     map[string]HandleCommand{},
		commandRequestTypes: map[string]func() interface{}{},
	}
}

func (store *entityStore) StateType(stateType func() interface{}) *entityStore {
	store.stateType = stateType
	return store
}

func (store *entityStore) Command(commandName string, requestType func() interface{}, handleCommand HandleCommand) *entityStore {
	store.commandRequestTypes[commandName] = requestType
	store.commandHandlers[commandName] = handleCommand
	return store
}

type Entity struct {
	EntityId  string
	Version   int64
	StateJson []byte
	State     interface{}
	UpdatedAt time.Time
}

func (store *entityStore) Get(conn sql.Conn, entityId string) (*Entity, error) {
	stmt := conn.Statement(store.getLatestStateSql)
	defer stmt.Close()
	rows, err := stmt.Query("entity_id", entityId)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	err = rows.Next()
	if err != nil {
		return nil, err
	}
	stateJson := []byte(rows.GetString(rows.C("state")))
	state := store.stateType()
	err = store.cfg.jsonApi.Unmarshal(stateJson, state)
	if err != nil {
		return nil, err
	}
	entity := &Entity{
		EntityId:  entityId,
		Version:   rows.GetInt64(rows.C("version")),
		StateJson: stateJson,
		State:     state,
		UpdatedAt: rows.GetTime(rows.C("committed_at")),
	}
	return entity, nil
}

func (store *entityStore) StartWorker(conn sql.Conn) *worker {
	worker := &worker{
		store:       store,
		entityCache: map[string]*Entity{},
		conn:        conn,
		commandQ:    make(chan *command, 10000)}
	go worker.work()
	return worker
}

func (worker *worker) HandleAsync(entityId string, commandId string, commandName string, request []byte) chan interface{} {
	responsePromise := make(chan interface{}, 1)
	command := &command{
		entityId:        entityId,
		commandId:       commandId,
		commandName:     commandName,
		request:         request,
		responsePromise: responsePromise,
	}
	worker.commandQ <- command
	return command.responsePromise
}

func (worker *worker) Handle(entityId string, commandId string, commandName string, request []byte) ([]byte, error) {
	responseQ := worker.HandleAsync(entityId, commandId, commandName, request)
	respObj := <-responseQ
	switch resp := respObj.(type) {
	case []byte:
		return resp, nil
	case error:
		return nil, resp
	default:
		return nil, fmt.Errorf("unknown: %v", respObj)
	}
}

func (worker *worker) work() {
	for {
		commands := worker.fetchCommands()
		fmt.Println("fetched", len(commands))
		if len(commands) == 0 {
			time.Sleep(time.Second)
		} else {
			err := worker.batchProcess(commands)
			if err != nil {
				for _, cmd := range commands {
					err := worker.batchProcess([]*command{cmd})
					if err != nil {
						cmd.reply(err)
					}
				}
			}
		}
	}
}

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

func (worker *worker) batchProcess(commands []*command) (err error) {
	store := worker.store
	rows := []driver.Value{}
	delayedReplies := []func(){}
	for _, command := range commands {
		row, response, err := worker.tryHandleOne(command)
		if err != nil {
			delayedReplies = append(delayedReplies, command.delayReply(err))
		} else {
			rows = append(rows, row)
			delayedReplies = append(delayedReplies, command.delayReply(response))
		}
	}
	if len(rows) == 0 {
		for _, delayedReply := range delayedReplies {
			delayedReply()
		}
		return nil
	}
	stmt := worker.conn.TranslateStatement("INSERT "+worker.store.entityName+" :BATCH_INSERT_COLUMNS",
		sql.BatchInsertColumns(len(rows),
			"entity_id", "version", "command_id", "command_name", "request", "response", "state"))
	defer stmt.Close()
	_, insertErr := stmt.Exec(rows...)
	if insertErr == nil {
		for _, delayedReply := range delayedReplies {
			delayedReply()
		}
		return nil
	}
	// the cache might be stale, in case other contention worker
	for _, command := range commands {
		delete(worker.entityCache, command.entityId)
	}
	if len(commands) == 1 {
		onlyCommand := commands[0]
		stmt := worker.conn.Statement(store.getEventSql)
		defer stmt.Close()
		rows, err := stmt.Query("entity_id", onlyCommand.entityId, "command_id", onlyCommand.commandId)
		if err != nil {
			onlyCommand.reply(err)
			return nil
		}
		defer rows.Close()
		err = rows.Next()
		if err == nil {
			response := rows.GetByteArray(rows.C("response"))
			onlyCommand.reply(response)
			return nil
		}
	}
	return insertErr
}

func (worker *worker) tryHandleOne(command *command) (row []driver.Value, response []byte, err error) {
	store := worker.store
	commandName := command.commandName
	entityId := command.entityId
	request := command.request
	commandId := command.commandId
	handleCommand := store.commandHandlers[commandName]
	if handleCommand == nil {
		return nil, nil, fmt.Errorf("no handler defined for command: %v", commandName)
	}
	var entity *Entity
	if commandName == "create" {
		entity = &Entity{
			EntityId:  entityId,
			Version:   0,
			StateJson: nil,
			State:     nil,
		}
	} else {
		entity = worker.entityCache[entityId]
		if entity == nil {
			entity, err = store.Get(worker.conn, entityId)
			if err != nil {
				return nil, nil, err
			}
			worker.entityCache[entityId] = entity
		}
	}
	requestObj := store.commandRequestTypes[commandName]()
	if requestObj != nil {
		err = store.cfg.jsonApi.Unmarshal(request, requestObj)
		if err != nil {
			return nil, nil, err
		}
	}
	responseObj, newState, err := handleCommand(requestObj, entity.State)
	if err != nil {
		return nil, nil, err
	}
	response, err = store.cfg.jsonApi.Marshal(responseObj)
	if err != nil {
		return nil, nil, err
	}
	var newStateJson []byte
	if newState == nil {
		newStateJson = entity.StateJson
	} else {
		newStateJson, err = store.cfg.jsonApi.Marshal(newState)
		if err != nil {
			return nil, nil, err
		}
	}
	row = sql.BatchInsertRow(
		"entity_id", entityId,
		"version", entity.Version+1,
		"command_id", commandId,
		"command_name", commandName,
		"request", request,
		"response", response,
		"state", newStateJson)
	entity.State = newState
	entity.StateJson = newStateJson
	entity.Version += 1
	worker.entityCache[entityId] = entity
	return row, response, nil
}
