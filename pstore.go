package pstore

import (
	"github.com/v2pro/psql"
	"time"
	"github.com/json-iterator/go"
)

type entityStore struct {
	insertSql         *psql.TranslatedSql
	getLatestStateSql *psql.TranslatedSql
	getEventSql       *psql.TranslatedSql
	jsonApi           jsoniter.Api
}

func StoreOf(entityName string) *entityStore {
	insertSql := psql.Translate(
		"INSERT "+entityName+" :INSERT_COLUMNS",
		"entity_id", "event_id", "event_name", "command", "state")
	getLatestStateSql := psql.Translate(
		"SELECT * FROM " + entityName + " WHERE entity_id=:entity_id ORDER BY event_id LIMIT 1")
	getEventSql := psql.Translate(
		"SELECT * FROM " + entityName + " WHERE entity_id=:entity_id AND event_name=:event_name")
	return &entityStore{
		insertSql:         insertSql,
		getLatestStateSql: getLatestStateSql,
		getEventSql:       getEventSql,
		jsonApi:           jsoniter.ConfigDefault,
	}
}

type Entity struct {
	EntityId  string
	Version   int64
	State     jsoniter.Any
	UpdatedAt time.Time
}

func (store *entityStore) Create(conn *psql.Conn, entityId string, dataObj interface{}) error {
	data, err := store.jsonApi.MarshalToString(dataObj)
	if err != nil {
		return err
	}
	_, insertErr := conn.Exec(store.insertSql,
		"entity_id", entityId,
		"event_id", int64(1),
		"event_name", "created",
		"command", data,
		"state", data)
	if insertErr == nil {
		return nil
	}
	stmt := conn.Statement(store.getEventSql)
	defer stmt.Close()
	rows, err := stmt.Query("entity_id", entityId, "event_name", "created")
	if err != nil {
		return err
	}
	defer rows.Close()
	if rows.Next() == nil {
		return nil
	}
	return insertErr
}

func (store *entityStore) Get(conn *psql.Conn, entityId string) (*Entity, error) {
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
	entity := &Entity{
		EntityId:  entityId,
		Version:   rows.GetInt64(rows.C("event_id")),
		State:     store.jsonApi.Get([]byte(rows.GetString(rows.C("state")))),
		UpdatedAt: rows.GetTime(rows.C("created_at")),
	}
	return entity, nil
}
