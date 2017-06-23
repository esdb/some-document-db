package quokka

import (
	"github.com/v2pro/plz"
	_ "github.com/v2pro/lego/jsoniter_adapter"
	"github.com/v2pro/plz/codec"
)

type Config struct {
	JsonApi  codec.Codec
	HttpAddr string
}

type frozenConfig struct {
	configBeforeFrozen Config
	jsonApi            codec.Codec
	httpAddr           string
}

func (cfg Config) Froze() *frozenConfig {
	if cfg.HttpAddr == "" {
		cfg.HttpAddr = ":9000"
	}
	if cfg.JsonApi == nil {
		cfg.JsonApi = plz.Codec("json")
	}
	return &frozenConfig{cfg, cfg.JsonApi, cfg.HttpAddr}
}

var ConfigDefault = Config{}.Froze()
