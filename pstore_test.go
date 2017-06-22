package quokka

import (
	"testing"
	"github.com/go-sql-driver/mysql"
	"github.com/v2pro/psql"
	"github.com/json-iterator/go/require"
	"fmt"
	"github.com/json-iterator/go"
	"time"
	"strconv"
	"database/sql/driver"
)

type Account struct {
	UsableBalance int64
	FrozenBalance int64
}

type ResponseMessage struct {
	Errno  int
	Errmsg string
}

var accounts = StoreOf("account").
	StateType(
	func() interface{} {
		return &Account{}
	}).
	Command("create",
	func() interface{} { return nil },
	func(request interface{}, state interface{}) (response interface{}, newState interface{}, err error) {
		return ResponseMessage{
			Errno: 0,
		}, &Account{}, nil
	}).
	Command("transfer1pc",
	func() interface{} {
		var val int64
		return &val
	},
	func(request interface{}, state interface{}) (response interface{}, newState interface{}, err error) {
		amount := *(request.(*int64))
		account := state.(*Account)
		oldBalance := account.UsableBalance
		account.UsableBalance += amount
		if account.UsableBalance < 0 {
			return ResponseMessage{
				Errno:  1,
				Errmsg: fmt.Sprintf("account balance can not be negative: %v => %v", oldBalance, account.UsableBalance),
			}, nil, err
		} else {
			return ResponseMessage{
				Errno: 0,
			}, account, err
		}
	})

func Test_create(t *testing.T) {
	should := require.New(t)
	drv := mysql.MySQLDriver{}
	conn, err := psql.Open(drv, "root:123456@tcp(127.0.0.1:3306)/v2pro")
	should.Nil(err)
	defer conn.Close()
	accountId := NewID().String()
	worker := accounts.StartWorker(conn)
	_, err = worker.Handle(accountId, "create", "create", nil)
	should.Nil(err)
	account, err := accounts.Get(conn, accountId)
	should.Nil(err)
	should.Equal(accountId, account.EntityId)
}

func Test_batch_insert(t *testing.T) {
	drv := mysql.MySQLDriver{}
	conn, _ := psql.Open(drv, "root:123456@tcp(127.0.0.1:3306)/v2pro")
	rows := []driver.Value{
		psql.BatchInsertRow(
			"entity_id", "b555t48t87413c8g6kgg",
			"version", int64(1),
			"command_id", "create1",
			"command_name", "create1",
			"request", "{}",
			"response", "{}",
			"state", "{}"),
		psql.BatchInsertRow(
			"entity_id", "b555t48t87413c8g6kgg",
			"version", int64(2),
			"command_id", "create2",
			"command_name", "create2",
			"request", "{}",
			"response", "{}",
			"state", "{}"),
	}
	stmt := conn.TranslateStatement("INSERT account :BATCH_INSERT_COLUMNS",
		psql.BatchInsertColumns(len(rows),
			"entity_id", "version", "command_id", "command_name", "request", "response", "state"))
	defer stmt.Close()
	_, err := stmt.Exec(rows...)
	fmt.Println(err)
}

func Test_create_should_be_idempotent(t *testing.T) {
	should := require.New(t)
	drv := mysql.MySQLDriver{}
	conn, err := psql.Open(drv, "root:123456@tcp(127.0.0.1:3306)/v2pro")
	should.Nil(err)
	defer conn.Close()
	accountId := NewID().String()
	worker := accounts.StartWorker(conn)
	_, err = worker.Handle(accountId, "create", "create", nil)
	should.Nil(err)
	_, err = worker.Handle(accountId, "create", "create", nil)
	should.Nil(err)
}

func Test_update(t *testing.T) {
	should := require.New(t)
	drv := mysql.MySQLDriver{}
	conn, err := psql.Open(drv, "root:123456@tcp(127.0.0.1:3306)/v2pro")
	should.Nil(err)
	defer conn.Close()
	accountId := NewID().String()
	worker := accounts.StartWorker(conn)
	_, err = worker.Handle(accountId, "create", "create", nil)
	should.Nil(err)
	response, err := worker.Handle(accountId, "xxx-001", "transfer1pc", []byte("100"))
	should.Nil(err)
	should.Equal(0, jsoniter.Get(response, "errno").ToInt())
	account, err := accounts.Get(conn, accountId)
	should.Nil(err)
	should.Equal(int64(100), account.State.(*Account).UsableBalance)
}


func Test_update_should_be_idempotent(t *testing.T) {
	should := require.New(t)
	drv := mysql.MySQLDriver{}
	conn, err := psql.Open(drv, "root:123456@tcp(127.0.0.1:3306)/v2pro")
	should.Nil(err)
	defer conn.Close()
	accountId := NewID().String()
	worker := accounts.StartWorker(conn)
	_, err = worker.Handle(accountId, "create", "create", nil)
	should.Nil(err)
	response, err := worker.Handle(accountId, "xxx-001", "transfer1pc", []byte("100"))
	should.Nil(err)
	should.Equal(0, jsoniter.Get(response, "Errno").MustBeValid().ToInt())
	response, err = worker.Handle(accountId, "xxx-001", "transfer1pc", []byte("100"))
	should.Nil(err)
	should.Equal(0, jsoniter.Get(response, "Errno").MustBeValid().ToInt())
}

func Test_update_should_not_violate_command_constraint(t *testing.T) {
	should := require.New(t)
	drv := mysql.MySQLDriver{}
	conn, err := psql.Open(drv, "root:123456@tcp(127.0.0.1:3306)/v2pro")
	should.Nil(err)
	defer conn.Close()
	accountId := NewID().String()
	worker := accounts.StartWorker(conn)
	_, err = worker.Handle(accountId, "create", "create", nil)
	should.Nil(err)
	response, err := worker.Handle(accountId, "xxx-001", "transfer1pc", []byte("-100"))
	should.Nil(err)
	should.Equal(1, jsoniter.Get(response, "Errno").MustBeValid().ToInt())
}

func Test_10000_run(t *testing.T) {
	// when there is no contention
	drv := mysql.MySQLDriver{}
	conn, err := psql.Open(drv, "root:123456@tcp(127.0.0.1:3306)/v2pro")
	if err != nil {
		t.Error(err)
	}
	defer conn.Close()
	accountId := NewID().String()
	worker := accounts.StartWorker(conn)
	worker.Handle(accountId, "create", "create", nil)
	before := time.Now()
	responsePromises := []chan interface{}{}
	for i := 0; i < 1000000; i++ {
		responsePromise := worker.HandleAsync(accountId, strconv.FormatInt(int64(i), 10), "transfer1pc", []byte("1"))
		responsePromises = append(responsePromises, responsePromise)
	}
	success := 0
	for _, responsePromise := range responsePromises {
		resp := <-responsePromise
		_, ok := resp.([]byte)
		if ok {
			success++
		}
	}
	after := time.Now()
	fmt.Println(1000000 / after.Sub(before).Seconds())
}
