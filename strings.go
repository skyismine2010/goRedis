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

	for i := 3; i < req.argc; i++ {
		var a, next *string
		a = req.argv[i]
		if i == req.argc-1 {
			next = nil
		} else {
			next = req.argv[i+1]
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
			replyRedisAck(req, &ReplySyntaxErr)
			return
		}
	}
	cmdSetCommonHandler(req, expire, flags)

}

func cmdSetCommonHandler(req *redisReq, expireStr *string, flags int8) {
	var expire int
	var err error
	k := req.argv[1]
	v := req.argv[2]
	//fmt.Printf("Set k=%s, v=%s\n", k, v)

	if expireStr != nil {
		expire, err = strconv.Atoi(*expireStr)
		if err != nil {
			replyErrorFormat(req, "invalid expire time in %s", req.argv[0])
			return
		}

		if flags&ObjSetEX != 0 {
			expire *= 1000
		}
	}
	now := time.Now()

	ttl := now.Add(time.Duration(expire) * time.Millisecond)
	fmt.Printf("set key = %s, now = %s, ttl=%s\n", k, time.Now().String(), ttl.String())

	obj := str2Obj(v, &ttl)

	_, exist := req.db.dbDict[*k]
	if (exist && (flags&ObjSetNX) != 0) ||
		(!exist && (flags&ObjSetXX) != 0) {
		replyRedisAck(req, &ReplyNullBulk)
		return
	}

	if expireStr != nil {
		req.db.expiresDict[*k] = obj
	}
	req.db.dbDict[*k] = obj
	replyRedisAck(req, &ReplyOK)
}

func cmdGetHandler(req *redisReq) {
	k := req.argv[1]
	dbMap := req.db.dbDict
	obj, ok := dbMap[*k]
	if !ok {
		replyRedisAck(req, &ReplyNullBulk)
	} else {
		s := Obj2Str(obj)
		fmt.Printf("Get k=%s, v=%s\n", *k, *s)
		replyRedisAck(req, addReplyBulkCString(s))
	}
}

func str2Obj(v *string, ttl *time.Time) *redisObj {
	var obj *redisObj
	numV, err := strconv.Atoi(*v)
	if err != nil {
		obj = &redisObj{ObjString, ObjEncodingStr, v, *ttl}
	} else {
		obj = &redisObj{ObjString, ObjEncodingINT, numV, *ttl}
	}

	return obj
}

func Obj2Str(obj *redisObj) *string {
	if obj.encoding == ObjEncodingStr {
		v, _ := obj.value.(*string)
		return v
	} else if obj.encoding == ObjEncodingINT {
		vNum, _ := obj.value.(int)
		v := strconv.Itoa(vNum)
		return &v
	} else {
		return nil
	}
}
