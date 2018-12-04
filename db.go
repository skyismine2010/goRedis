package goRedis

import "fmt"

func initRedisDb(server *redisServer) error {
	server.dbNum = 1 //todo

	server.db = make([]redisDB, server.dbNum)

	for i := 0; i < server.dbNum; i++ {
		server.db[i].dbDict = make(map[string]interface{})
		server.db[i].expiresDict = make(map[string]interface{})
	}
	fmt.Printf("Init Go Redis DB success.\n")
	return nil
}

type redisDB struct {
	dbDict      map[string]interface{}
	expiresDict map[string]interface{}
	//todo 这里需要做的东西还有很多，先暂时这样
}
