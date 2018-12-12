package goRedis

import "fmt"

type redisDB struct {
	dbDict       map[string]*redisObj
	expiresDict  map[string]*redisObj
	blockingKeys map[string]*redisReq
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

func lookupByKeyOrReply(req *redisReq, key *string, reply *string) *redisObj {
	v, exist := req.db.dbDict[*key]
	if !exist {
		replyRedisAck(req, reply)
		return nil
	}
	return v
}

func lookupByKey(req *redisReq, key *string) *redisObj {
	v, exist := req.db.dbDict[*key]
	if !exist {
		return nil
	}
	return v

}
