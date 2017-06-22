package quokka

import (
	"github.com/v2pro/plz"
	_ "github.com/v2pro/lego/jsoniter_adapter"
)

type Config struct {
	JsonApi  plz.Codec
	HttpAddr string
}

type frozenConfig struct {
	configBeforeFrozen Config
	jsonApi            plz.Codec
	httpAddr           string
}

func (cfg Config) Froze() *frozenConfig {
	if cfg.HttpAddr == "" {
		cfg.HttpAddr = ":9000"
	}
	if cfg.JsonApi == nil {
		cfg.JsonApi = plz.CodecOf["json"]
	}
	return &frozenConfig{cfg, cfg.JsonApi, cfg.HttpAddr}
}

var ConfigDefault = Config{}.Froze()
