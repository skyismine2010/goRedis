package goRedis

import "fmt"

type redisDB struct {
	dbDict      map[string]*redisObj
	expiresDict map[string]*redisObj
	//todo 这里需要做的东西还有很多，先暂时这样
}

func initRedisDb(server *redisServer) error {
	server.dbNum = 1 //todo

	server.db = make([]redisDB, server.dbNum)

	for i := 0; i < server.dbNum; i++ {
		server.db[i].dbDict = make(map[string]*redisObj)
		server.db[i].expiresDict = make(map[string]*redisObj)
	}
	fmt.Printf("Init Go Redis DB success.\n")
	return nil
}
