package pstore

import (
	"github.com/v2pro/psql"
	"time"
	"github.com/json-iterator/go"
	"fmt"
)

type entityStore struct {
	insertSql         *psql.TranslatedSql
	getLatestStateSql *psql.TranslatedSql
	getEventSql       *psql.TranslatedSql
	jsonApi           jsoniter.Api
	commandHandlers   map[string]HandleCommand
}

func StoreOf(entityName string) *entityStore {
	insertSql := psql.Translate(
		"INSERT "+entityName+" :INSERT_COLUMNS",
		"entity_id", "event_id", "command_id", "command_name", "command_body", "state")
	getLatestStateSql := psql.Translate(
		"SELECT * FROM " + entityName + " WHERE entity_id=:entity_id ORDER BY event_id DESC LIMIT 1")
	getEventSql := psql.Translate(
		"SELECT * FROM " + entityName + " WHERE entity_id=:entity_id AND command_id=:command_id")
	return &entityStore{
		insertSql:         insertSql,
		getLatestStateSql: getLatestStateSql,
		getEventSql:       getEventSql,
		jsonApi:           jsoniter.ConfigDefault,
		commandHandlers:   map[string]HandleCommand{},
	}
}

func (store *entityStore) JsonApi(jsonApi jsoniter.Api) *entityStore {
	store.jsonApi = jsonApi
	return store
}

type Command struct {
	CommandName string
	CommandId   string
}

type HandleCommand func(commandName string, commandId string, commandBody interface{}, state jsoniter.Any) (interface{}, error)

func (store *entityStore) Command(commandName string, handleCommand HandleCommand) *entityStore {
	store.commandHandlers[commandName] = handleCommand
	return store
}

type Entity struct {
	EntityId  string
	Version   int64
	State     jsoniter.Any
	UpdatedAt time.Time
}

func (store *entityStore) Create(conn *psql.Conn, entityId string, state interface{}) error {
	stateJson, err := store.jsonApi.MarshalToString(state)
	if err != nil {
		return err
	}
	_, insertErr := conn.Exec(store.insertSql,
		"entity_id", entityId,
		"event_id", int64(1),
		"command_id", "create",
		"command_name", "create",
		"command_body", stateJson,
		"state", stateJson)
	if insertErr == nil {
		return nil
	}
	stmt := conn.Statement(store.getEventSql)
	defer stmt.Close()
	rows, err := stmt.Query("entity_id", entityId, "command_id", "create")
	if err != nil {
		return err
	}
	defer rows.Close()
	err = rows.Next()
	if err == nil {
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
		UpdatedAt: rows.GetTime(rows.C("handled_at")),
	}
	return entity, nil
}

func (store *entityStore) Update(conn *psql.Conn, entityId string, commandId string, commandName string, commandBody interface{}) error {
	handleCommand := store.commandHandlers[commandName]
	if handleCommand == nil {
		return fmt.Errorf("no handler defined for command: %v", commandName)
	}
	entity, err := store.Get(conn, entityId)
	if err != nil {
		return err
	}
	newState, err := handleCommand(commandName, commandId, commandBody, entity.State)
	if err != nil {
		return err
	}
	commandBodyJson, err := store.jsonApi.MarshalToString(commandBody)
	if err != nil {
		return err
	}
	stateJson, err := store.jsonApi.MarshalToString(newState)
	if err != nil {
		return err
	}
	_, insertErr := conn.Exec(store.insertSql,
		"entity_id", entityId,
		"event_id", entity.Version+1,
		"command_id", commandId,
		"command_name", commandName,
		"command_body", commandBodyJson,
		"state", stateJson)
	if insertErr != nil {
		stmt := conn.Statement(store.getEventSql)
		defer stmt.Close()
		rows, err := stmt.Query("entity_id", entityId, "command_id", commandId)
		if err != nil {
			return err
		}
		defer rows.Close()
		err = rows.Next()
		if err == nil {
			return nil
		}
	}
	return insertErr
}
