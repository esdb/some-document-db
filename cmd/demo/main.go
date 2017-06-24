package main

import "github.com/v2pro/plz"
import (
	_ "github.com/v2pro/quokka/bootstrap"
	"context"
	"time"
	"net/http"
)

var json = plz.Codec("json")

func main() {
	plz.RunApp(func() int {
		server := plz.Server("http_address", "192.168.3.33:8080")
		server.SubServer("status").
			Method("",
			"http_decode", func(request *http.Request) interface{} {
				return ""
			},
			"http_encode", func(responseWriter http.ResponseWriter, resp interface{}, err error) error {
				respBytes, err := json.Marshal(map[string]interface{}{
					"errno": 0,
					"data": resp,
				})
				if err != nil {
					return err
				}
				responseWriter.Write(respBytes)
				return nil
			},
			"handle", func(ctx context.Context, request interface{}) (response interface{}, err error) {
				return "OK", nil
			})
		_, err := server.Start()
		if err != nil {
			panic(err.Error())
		}
		for {
			time.Sleep(time.Minute)
		}
		return 0
	})
}
