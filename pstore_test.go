package pstore

import (
	"testing"
	"github.com/go-sql-driver/mysql"
	"github.com/v2pro/psql"
	"github.com/json-iterator/go/require"
)

var accounts = StoreOf("account")

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
