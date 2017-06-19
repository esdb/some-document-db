package pstore

import (
	"testing"
	"net/http"
)

func Test_http(t *testing.T) {
	go StartHttpServer()
	http.Get("http://127.0.0.1:9000")
}