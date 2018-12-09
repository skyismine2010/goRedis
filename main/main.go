package main

import _ "net/http/pprof"
import (
	"github.com/skyismine2010/goRedis"
	"log"
	"net/http"
)

func main() {
	go func() {
		log.Println(http.ListenAndServe("localhost:50500", nil))
	}()
	goRedis.StartServer()
}
