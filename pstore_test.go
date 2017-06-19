package pstore

import (
	"testing"
	"github.com/go-sql-driver/mysql"
	"github.com/v2pro/psql"
	"github.com/json-iterator/go/require"
	"github.com/json-iterator/go"
	"fmt"
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
	DecodeState(
	func(jsonApi jsoniter.Api, stateJson []byte) (interface{}, error) {
		var account Account
		err := jsonApi.Unmarshal(stateJson, &account)
		return account, err
	}).
	Command("create",
	func(jsonApi jsoniter.Api, request []byte, state interface{}) (response []byte, newState interface{}, err error) {
		response, err = jsonApi.Marshal(ResponseMessage{
			Errno: 0,
		})
		return response, Account{}, nil
	}).
	Command("transfer1pc",
	func(jsonApi jsoniter.Api, request []byte, state interface{}) (response []byte, newState interface{}, err error) {
		amount := jsonApi.Get(request).ToInt64()
		account := state.(Account)
		oldBalance := account.UsableBalance
		account.UsableBalance += amount
		if account.UsableBalance < 0 {
			response, err = jsonApi.Marshal(ResponseMessage{
				Errno:  1,
				Errmsg: fmt.Sprintf("account balance can not be negative: %v => %v", oldBalance, account.UsableBalance),
			})
			return response, nil, err
		} else {
			response, err = jsonApi.Marshal(ResponseMessage{
				Errno: 0,
			})
			return response, account, err
		}
	})

func Test_create(t *testing.T) {
	should := require.New(t)
	drv := mysql.MySQLDriver{}
	conn, err := psql.Open(drv, "root:123456@tcp(127.0.0.1:3306)/v2pro")
	should.Nil(err)
	defer conn.Close()
	accountId := NewID().String()
	_, err = accounts.Create(conn, accountId, nil)
	should.Nil(err)
	account, err := accounts.Get(conn, accountId)
	should.Nil(err)
	should.Equal(accountId, account.EntityId)
}

func Test_create_should_be_idempotent(t *testing.T) {
	should := require.New(t)
	drv := mysql.MySQLDriver{}
	conn, err := psql.Open(drv, "root:123456@tcp(127.0.0.1:3306)/v2pro")
	should.Nil(err)
	defer conn.Close()
	accountId := NewID().String()
	_, err = accounts.Create(conn, accountId, nil)
	should.Nil(err)
	_, err = accounts.Create(conn, accountId, nil)
	should.Nil(err)
}

func Test_update(t *testing.T) {
	should := require.New(t)
	drv := mysql.MySQLDriver{}
	conn, err := psql.Open(drv, "root:123456@tcp(127.0.0.1:3306)/v2pro")
	should.Nil(err)
	defer conn.Close()
	accountId := NewID().String()
	_, err = accounts.Create(conn, accountId, nil)
	should.Nil(err)
	response, err := accounts.Update(conn, accountId, "xxx-001", "transfer1pc", []byte("100"))
	should.Nil(err)
	should.Equal(0, jsoniter.Get(response, "errno").ToInt())
	account, err := accounts.Get(conn, accountId)
	should.Nil(err)
	should.Equal(int64(100), account.State.(Account).UsableBalance)
}

func Test_update_should_be_idempotent(t *testing.T) {
	should := require.New(t)
	drv := mysql.MySQLDriver{}
	conn, err := psql.Open(drv, "root:123456@tcp(127.0.0.1:3306)/v2pro")
	should.Nil(err)
	defer conn.Close()
	accountId := NewID().String()
	_, err = accounts.Create(conn, accountId, nil)
	should.Nil(err)
	response, err := accounts.Update(conn, accountId, "xxx-001", "transfer1pc", []byte("100"))
	should.Nil(err)
	should.Equal(0, jsoniter.Get(response, "Errno").MustBeValid().ToInt())
	response, err = accounts.Update(conn, accountId, "xxx-001", "transfer1pc", []byte("100"))
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
	_, err = accounts.Create(conn, accountId, nil)
	should.Nil(err)
	response, err := accounts.Update(conn, accountId, "xxx-001", "transfer1pc", []byte("-100"))
	should.Nil(err)
	should.Equal(1, jsoniter.Get(response, "Errno").MustBeValid().ToInt())
}

//func Benchmark_best_case_performance(b *testing.B) {
//	// when there is no contention
//	drv := mysql.MySQLDriver{}
//	conn, err := psql.Open(drv, "root:123456@tcp(127.0.0.1:3306)/v2pro")
//	if err != nil {
//		b.Error(err)
//	}
//	defer conn.Close()
//	accountId := NewID().String()
//	accounts.Create(conn, accountId, &Account{})
//	b.ReportAllocs()
//	for i := 0; i < b.N; i++ {
//		err = accounts.Update(conn, accountId, strconv.FormatInt(int64(i), 10), "transfer1pc", int64(1))
//		if err != nil {
//			b.Error(err)
//		}
//	}
//}

