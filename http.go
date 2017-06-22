package quokka

import "net/http"

func StartHttpServer() {
	ConfigDefault.StartHttpServer()
}

func (cfg *frozenConfig) StartHttpServer() {
	mux := http.NewServeMux()
	http.ListenAndServe(cfg.httpAddr, mux)
}

type ClientConfig struct {
	HttpAddr string
}


