package goRedis

import (
	"container/list"
	"strconv"
	"time"
)

const LIST_HEAD = 0
const LIST_TAIL = 1

func cmdLPushHandler(req *redisReq) {
	cmdPushGenericHandler(req, LIST_HEAD)
}

func cmdRPushHandler(req *redisReq) {
	cmdPushGenericHandler(req, LIST_TAIL)
}

func cmdListTypeCheckOrReply(req *redisReq, v *redisObj) *list.List {
	if v.rType != ObjList || v.encoding != ObjEncodingList {
		replyRedisAck(req, &ReplyWrongTypeErr)
		return nil
	}
	vList, ok := v.value.(*list.List)
	if !ok {
		panic("can't convert to list type")
	}
	return vList
}

func cmdPushGenericHandler(req *redisReq, where int) {
	var vList *list.List
	k := req.client.argv[1]
	obj := lookupByKey(req, k)
	if obj == nil {
		vList = list.New()
		obj = &redisObj{ObjList, ObjEncodingList, vList, time.Now()}
	} else {
		vList = cmdListTypeCheckOrReply(req, obj)
		if vList == nil {
			return
		}
	}

	var pushed int64
	for i := 2; i < req.client.argc; i++ {
		if where == LIST_HEAD {
			vList.PushFront(req.client.argv[i])
		} else {
			vList.PushBack(req.client.argv[i])
		}
		pushed++
	}
	req.client.db.dbDict[*k] = obj
	replyNumerFormat(req, int64(vList.Len()))
}

func cmdLLenHandler(req *redisReq) {
	obj := lookupByKey(req, req.client.argv[1])
	if obj == nil {
		replyNumerFormat(req, 0)
		return
	}
	vList := cmdListTypeCheckOrReply(req, obj)
	if vList == nil {
		return
	}
	replyNumerFormat(req, int64(vList.Len()))
}

func cmdLRangeHandler(req *redisReq) {
	k := req.client.argv[1]
	obj := lookupByKeyOrReply(req, k, &ReplyEmptyMultiBulk)
	if obj == nil {
		return
	}

	vList := cmdListTypeCheckOrReply(req, obj)
	{
		if vList == nil {
			return
		}
	}

	start, err := strconv.Atoi(*(req.client.argv[2]))
	if err != nil {
		replyErrorFormat(req, "value is not an integer or out of range")
		return
	}
	stop, err := strconv.Atoi(*(req.client.argv[3]))
	if err != nil {
		replyErrorFormat(req, "value is not an integer or out of range")
		return
	}

	if start < 0 {
		start += vList.Len()
	}
	if stop < 0 {
		stop += vList.Len()
	}
	if start < 0 {
		start = 0
	}

}
