package goRedis

import (
	"log"
	"time"
)

func cmdDictTypeCheckOrReply(req *redisReq, obj *redisObj) map[*string]*string {
	if obj.rType != ObjHash || obj.encoding != ObjEncodingHT {
		replyRedisAck(req, &ReplyWrongTypeErr)
		return nil
	}
	vMap, ok := obj.value.(map[*string]*string)
	if !ok {
		panic("can't convert to list type")
	}
	return vMap
}

func cmdHmSetHandler(req *redisReq) {
	log.Printf("goRedis server begin handler hmset msg\n")
	if req.argc%2 == 1 {
		replyErrorFormat(req, "wrong number of arguments for HMSET")
		return
	}
	var dict map[*string]*string

	obj := lookupByKey(req, req.argv[1])
	if obj == nil {
		dict = make(map[*string]*string)
		obj = &redisObj{ObjHash, ObjEncodingHT, dict, time.Now()}
		req.db.dbDict[*(req.argv[1])] = obj

	} else {
		dict = cmdDictTypeCheckOrReply(req, obj)
		if dict == nil {
			return
		}
	}
	for i := 2; i < req.argc; i += 2 {
		dict[req.argv[i]] = req.argv[i+1]
	}

	replyRedisAck(req, &ReplyOK)
	log.Printf("goRedis server end handler hmset msg\n")
}

func cmdHmGetAllHandler(req *redisReq) {

}
