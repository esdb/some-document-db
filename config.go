package quokka

import "github.com/json-iterator/go"

type Config struct {
	JsonApi  jsoniter.Api
	HttpAddr string
}

type frozenConfig struct {
	configBeforeFrozen Config
	jsonApi            jsoniter.Api
	httpAddr           string
}

func (cfg Config) Froze() *frozenConfig {
	if cfg.HttpAddr == "" {
		cfg.HttpAddr = ":9000"
	}
	if cfg.JsonApi == nil {
		cfg.JsonApi = jsoniter.ConfigDefault
	}
	return &frozenConfig{cfg, cfg.JsonApi, cfg.HttpAddr}
}

var ConfigDefault = Config{}.Froze()
