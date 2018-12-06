package goRedis

import (
	"fmt"
	"strconv"
	"time"
)

const ObjSetNoFlags = int8(8)
const ObjSetNX = int8(1 << 0)
const ObjSetXX = int8(1 << 1)
const ObjSetEX = int8(1 << 2)
const ObjSetPX = int8(1 << 3)

func cmdSetHandler(req *redisReq) {
	var expire *string
	flags := ObjSetNoFlags

	for i := 3; i < req.client.argc; i++ {
		var a, next *string
		a = &req.client.argv[i]
		if i == req.client.argc-1 {
			next = nil
		} else {
			next = &req.client.argv[i+1]
		}
		if ((*a)[0] == 'n' || (*a)[0] == 'N') &&
			((*a)[1] == 'x' || (*a)[1] == 'X') &&
			len(*a) == 2 && (flags&ObjSetXX) != 0 {
			flags |= ObjSetNX

		} else if ((*a)[0] == 'x' || (*a)[0] == 'X') &&
			((*a)[1] == 'x' || (*a)[1] == 'X') &&
			len(*a) == 2 && (flags&ObjSetNX) != 0 {
			flags |= ObjSetXX

		} else if ((*a)[0] == 'e' || (*a)[0] == 'E') &&
			((*a)[1] == 'x' || (*a)[1] == 'X') &&
			len(*a) == 2 && (flags&ObjSetPX) != 0 && next != nil {
			flags |= ObjSetEX
			expire = next
			i++
		} else if ((*a)[0] == 'p' || (*a)[0] == 'P') &&
			((*a)[1] == 'x' || (*a)[1] == 'X') &&
			len(*a) == 2 && (flags&ObjSetEX) != 0 && next != nil {
			flags |= ObjSetPX
			expire = next
			i++
		} else {
			replyRedisAck(req.client, &ReplySyntaxErr)
			return
		}
	}
	cmdSetCommonHandler(req, expire, flags)

}

func cmdSetCommonHandler(req *redisReq, expireStr *string, flags int8) {
	var expire int
	var err error
	k := req.client.argv[1]
	v := req.client.argv[2]
	fmt.Printf("Set k=%s, v=%s\n", k, v)

	if expireStr != nil {
		expire, err = strconv.Atoi(*expireStr)
		if err != nil {
			replyErrorFormat(req, "invalid expire time in %s", req.client.argv[0])
			return
		}

		if flags&ObjSetEX != 0 {
			expire *= 1000
		}
	}
	now := time.Now()

	ttl := now.Add(time.Duration(expire) * time.Millisecond)
	fmt.Printf("now = %s, ttl=%s\n", time.Now().String(), ttl.String())

	obj := &redisObj{ObjString, ObjEncodingStr, v, ttl}

	_, exist := req.client.db.dbDict[k]
	if (exist && (flags&ObjSetNX) != 0) ||
		(!exist && (flags&ObjSetXX) != 0) {
		replyRedisAck(req.client, &ReplyNullBulk)
		return
	}

	if expireStr != nil {
		req.client.db.expiresDict[k] = obj
	}
	req.client.db.dbDict[k] = obj
	replyRedisAck(req.client, &ReplyOK)
}

func cmdGetHandler(req *redisReq) {
	k := req.client.argv[1]
	dbMap := req.client.db.dbDict
	obj, ok := dbMap[k]
	if !ok {
		replyRedisAck(req.client, &ReplyNullBulk)
	} else {
		s, ok := obj.value.(string) //todo
		if !ok {
			replyRedisAck(req.client, &ReplyNullBulk) //todo
		}
		fmt.Printf("Get k=%s, v=%s\n", k, s)
		replyRedisAck(req.client, addReplyBulkCString(&s))
	}
}
