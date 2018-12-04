package goRedis

import "fmt"

func cmdSetHandler(req *redisReq) {
	k := req.client.argv[1]
	v := req.client.argv[2]
	fmt.Printf("Set k=%s, v=%s\n", k, v)
	req.client.db.dbDict[k] = v
	replyRedisAck(req.client, &ReplyOK)
}

func cmdGetHandler(req *redisReq) {
	k := req.client.argv[1]
	dbMap := req.client.db.dbDict
	v, ok := dbMap[k]
	if !ok {
		replyRedisAck(req.client, &ReplyNoBulk)
	} else {
		s, ok := v.(string)
		if !ok {
			replyRedisAck(req.client, &ReplyNoBulk) //todo
		}
		fmt.Printf("Get k=%s, v=%s\n", k, s)
		replyRedisAck(req.client, addReplyBulkCString(&s))
	}
}
