package pstore

import (
	"github.com/v2pro/psql"
	"time"
	"fmt"
	"github.com/json-iterator/go"
)

type HandleCommand func(jsonApi jsoniter.Api, request []byte, state interface{}) (response []byte, newState interface{}, err error)

type DecodeState func(jsonApi jsoniter.Api, state []byte) (interface{}, error)

type entityStore struct {
	cfg               *frozenConfig
	insertSql         *psql.TranslatedSql
	getLatestStateSql *psql.TranslatedSql
	getEventSql       *psql.TranslatedSql
	commandHandlers   map[string]HandleCommand
	decodeState       DecodeState
}

func StoreOf(entityName string) *entityStore {
	return ConfigDefault.StoreOf(entityName)
}

func (cfg *frozenConfig) StoreOf(entityName string) *entityStore {
	insertSql := psql.Translate(
		"INSERT "+entityName+" :INSERT_COLUMNS",
		"entity_id", "event_id", "command_id", "command_name", "request", "response", "state")
	getLatestStateSql := psql.Translate(
		"SELECT * FROM " + entityName + " WHERE entity_id=:entity_id ORDER BY event_id DESC LIMIT 1")
	getEventSql := psql.Translate(
		"SELECT * FROM " + entityName + " WHERE entity_id=:entity_id AND command_id=:command_id")
	return &entityStore{
		cfg:               cfg,
		insertSql:         insertSql,
		getLatestStateSql: getLatestStateSql,
		getEventSql:       getEventSql,
		commandHandlers:   map[string]HandleCommand{},
	}
}

func (store *entityStore) DecodeState(decodeState DecodeState) *entityStore {
	store.decodeState = decodeState
	return store
}

func (store *entityStore) Command(commandName string, handleCommand HandleCommand) *entityStore {
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
	stateJson := []byte(rows.GetString(rows.C("state")))
	state, err := store.decodeState(store.cfg.jsonApi, stateJson)
	if err != nil {
		return nil, err
	}
	entity := &Entity{
		EntityId:  entityId,
		Version:   rows.GetInt64(rows.C("event_id")),
		StateJson: stateJson,
		State:     state,
		UpdatedAt: rows.GetTime(rows.C("committed_at")),
	}
	return entity, nil
}

func (store *entityStore) Create(conn *psql.Conn, entityId string, request []byte) ([]byte, error) {
	return store.Update(conn, entityId, "create", "create", request)
}

func (store *entityStore) Update(conn *psql.Conn, entityId string, commandId string, commandName string, request []byte) (response []byte, err error) {
	handleCommand := store.commandHandlers[commandName]
	if handleCommand == nil {
		return nil, fmt.Errorf("no handler defined for command: %v", commandName)
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
		entity, err = store.Get(conn, entityId)
		if err != nil {
			return nil, err
		}
	}
	response, newState, err := handleCommand(store.cfg.jsonApi, request, entity.State)
	if err != nil {
		return nil, err
	}
	var newStateJson []byte
	if newState == nil {
		newStateJson = entity.StateJson
	} else {
		newStateJson, err = store.cfg.jsonApi.Marshal(newState)
		if err != nil {
			return nil, err
		}
	}
	_, insertErr := conn.Exec(store.insertSql,
		"entity_id", entityId,
		"event_id", entity.Version+1,
		"command_id", commandId,
		"command_name", commandName,
		"request", request,
		"response", response,
		"state", newStateJson)
	if insertErr != nil {
		stmt := conn.Statement(store.getEventSql)
		defer stmt.Close()
		rows, err := stmt.Query("entity_id", entityId, "command_id", commandId)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		err = rows.Next()
		if err == nil {
			response := rows.GetByteArray(rows.C("response"))
			return response, nil
		}
	}
	return response, insertErr
}
