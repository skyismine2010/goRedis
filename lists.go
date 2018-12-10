package goRedis

import (
	"container/list"
	"strconv"
	"time"
)

const ListHead = 0
const ListTail = 1

func cmdLPushHandler(req *redisReq) {
	cmdPushGenericHandler(req, ListHead)
}

func cmdRPushHandler(req *redisReq) {
	cmdPushGenericHandler(req, ListTail)
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
		if where == ListHead {
			vList.PushFront(req.client.argv[i]) //todo 还可以对数字类型进行压缩
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

func cmdListIteratorByIdx(vList *list.List, idx int) *list.Element {
	el := vList.Front()
	for idx > 0 {
		el = el.Next()
		idx--
	}
	return el
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

	lLen := vList.Len()
	start, err := strconv.Atoi(*(req.client.argv[2]))
	if err != nil {
		replyErrorFormat(req, "value is not an integer or out of range")
		return
	}
	end, err := strconv.Atoi(*(req.client.argv[3]))
	if err != nil {
		replyErrorFormat(req, "value is not an integer or out of range")
		return
	}

	if start < 0 {
		start += vList.Len()
	}
	if end < 0 {
		end += vList.Len()
	}
	if start < 0 {
		start = 0
	}

	if start > end || start >= lLen {
		replyRedisAck(req, &ReplyEmptyMultiBulk)
		return
	}

	if end >= lLen {
		end = lLen - 1
	}

	rangeLen := (end - start) + 1

	var tmpReplyList []*string
	el := cmdListIteratorByIdx(vList, start)

	for rangeLen > 0 {
		vStr, _ := el.Value.(*string)
		tmpReplyList = append(tmpReplyList, vStr)
		el = el.Next()
		rangeLen--
	}
	replyStr := addReplyMultiBulk(tmpReplyList)
	replyRedisAck(req, replyStr)
}

func cmdBLPopHandler(req *redisReq) {
	cmdBlockingPopGenericHandler(req, ListHead)
}

func cmdBRPopHandler(req *redisReq) {
	cmdBlockingPopGenericHandler(req, ListTail)
}

func cmdLPopHandler(req *redisReq) {
	cmdPopGenericHandler(req, ListHead)
}

func cmdRPopHandler(req *redisReq) {
	cmdPopGenericHandler(req, ListTail)
}

func cmdPopGenericHandler(req *redisReq, where int) {
	obj := lookupByKey(req, req.client.argv[1])
	if obj == nil {
		replyNumerFormat(req, 0)
		return
	}
	vList := cmdListTypeCheckOrReply(req, obj)
	if vList == nil {
		return
	}

}

func cmdBlockingPopGenericHandler(req *redisReq, where int) {

}
