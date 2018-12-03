package goRedis

func cmdSetHandler(req *redisReq) {
	req.client.db.dbDict[req.client.argv[1]] = req.client.db.dbDict[req.client.argv[2]]

}

func cmdGetHandler(req *redisReq) {

}
