package pstore

import (
	"testing"
	"github.com/go-sql-driver/mysql"
	"github.com/v2pro/psql"
	"github.com/json-iterator/go/require"
	"github.com/json-iterator/go"
	"fmt"
	"strconv"
	"sync/atomic"
)

type Account struct {
	UsableBalance int64
	FrozenBalance int64
}

var accounts = StoreOf("account").
	JsonApi(jsoniter.ConfigDefault).
	Command("transfer1pc",
	func(commandName string, commandId string, commandBody interface{}, state jsoniter.Any) (interface{}, error) {
		amount := commandBody.(int64)
		account := Account{}
		state.ToVal(&account)
		if state.LastError() != nil {
			return nil, state.LastError()
		}
		oldBalance := account.UsableBalance
		account.UsableBalance += amount
		if account.UsableBalance < 0 {
			return nil, fmt.Errorf("account balance can not be negative: %v => %v", oldBalance, account.UsableBalance)
		}
		return account, nil
	})

func Test_create(t *testing.T) {
	should := require.New(t)
	drv := mysql.MySQLDriver{}
	conn, err := psql.Open(drv, "root:123456@tcp(127.0.0.1:3306)/v2pro")
	should.Nil(err)
	defer conn.Close()
	accountId := NewID().String()
	should.Nil(accounts.Create(conn, accountId, nil))
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
	should.Nil(accounts.Create(conn, accountId, nil))
	should.Nil(accounts.Create(conn, accountId, nil))
}

func Test_update(t *testing.T) {
	should := require.New(t)
	drv := mysql.MySQLDriver{}
	conn, err := psql.Open(drv, "root:123456@tcp(127.0.0.1:3306)/v2pro")
	should.Nil(err)
	defer conn.Close()
	accountId := NewID().String()
	should.Nil(accounts.Create(conn, accountId, &Account{}))
	should.Nil(accounts.Update(conn, accountId, "xxx-001", "transfer1pc", int64(100)))
	account, err := accounts.Get(conn, accountId)
	should.Nil(err)
	should.Equal(100, account.State.Get("UsableBalance").ToInt())
}

func Test_update_should_be_idempotent(t *testing.T) {
	should := require.New(t)
	drv := mysql.MySQLDriver{}
	conn, err := psql.Open(drv, "root:123456@tcp(127.0.0.1:3306)/v2pro")
	should.Nil(err)
	defer conn.Close()
	accountId := NewID().String()
	should.Nil(accounts.Create(conn, accountId, &Account{}))
	should.Nil(accounts.Update(conn, accountId, "xxx-001", "transfer1pc", int64(100)))
	should.Nil(accounts.Update(conn, accountId, "xxx-001", "transfer1pc", int64(100)))
}

func Test_update_should_not_violate_command_constraint(t *testing.T) {
	should := require.New(t)
	drv := mysql.MySQLDriver{}
	conn, err := psql.Open(drv, "root:123456@tcp(127.0.0.1:3306)/v2pro")
	should.Nil(err)
	defer conn.Close()
	accountId := NewID().String()
	should.Nil(accounts.Create(conn, accountId, &Account{}))
	should.NotNil(accounts.Update(conn, accountId, "xxx-001", "transfer1pc", int64(-100)))
}

func Benchmark_best_case_performance(b *testing.B) {
	// when there is no contention
	drv := mysql.MySQLDriver{}
	conn, err := psql.Open(drv, "root:123456@tcp(127.0.0.1:3306)/v2pro")
	if err != nil {
		b.Error(err)
	}
	defer conn.Close()
	accountId := NewID().String()
	accounts.Create(conn, accountId, &Account{})
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		err = accounts.Update(conn, accountId, strconv.FormatInt(int64(i), 10), "transfer1pc", int64(1))
		if err != nil {
			b.Error(err)
		}
	}
}

func Benchmark_parallel_performance(b *testing.B) {
	// when there is no contention
	drv := mysql.MySQLDriver{}
	pool := psql.NewPool(drv, "root:123456@tcp(127.0.0.1:3306)/v2pro", 32)
	conn, err := pool.Borrow()
	if err != nil {
		b.Error(err)
	}
	accountId := NewID().String()
	accounts.Create(conn, accountId, &Account{})
	conn.Close()
	b.ReportAllocs()
	var success uint64
	var failure uint64
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			commandId := NewID().String()
			conn, err := pool.Borrow()
			if err != nil {
				b.Error(err)
			}
			err = accounts.Update(conn, accountId, commandId, "transfer1pc", int64(1))
			if err != nil {
				atomic.AddUint64(&success, 1)
			} else {
				atomic.AddUint64(&failure, 1)
			}
			conn.Close()
		}
	})
	fmt.Println(success, failure)
}

