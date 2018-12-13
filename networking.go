package goRedis

import (
	"fmt"
)

func addReplyMultiBulkLen(length int64) *string {
	ret := fmt.Sprintf("*%d\r\n", length)
	return &ret
}

func addReplyBulkCString(subStr *string) *string {
	ret := fmt.Sprintf("$%d\r\n%s\r\n", len(*subStr), *subStr)
	return &ret
}

func addReplyMultiBulk(l []*string) *string {
	ret := fmt.Sprintf("*%d\r\n", len(l))
	for _, v := range l {
		ret += fmt.Sprintf("$%d\r\n%s\r\n", len(*v), *v)
	}
	return &ret
}

func addReplyInt64(length int64) *string {
	ret := fmt.Sprintf(":%d\r\n", length)
	return &ret
}

func addReplyCommandFlag(str *string, cmd *redisCommand, cmdFlag int32, cmdFlagStr string) int {
	if (cmd.flags & cmdFlag) != 0 {
		*str += fmt.Sprintf("+%s\r\n", cmdFlagStr)
		return 1
	}
	return 0
}

func replyErrorFormat(req *redisReq, str string, fmtList ...interface{}) {
	ackStr := fmt.Sprintf("-ERR "+str+"\r\n", fmtList...)
	replyRedisAck(req, &ackStr)
}

func replyRedisAck(req *redisReq, ackMsg *string) {
	//	log.Printf("send ack=%s to client", *ackMsg)
	req.clientChan <- *ackMsg
	//	log.Printf("send ack=%s to client finish", *ackMsg)
}

func replyNumerFormat(req *redisReq, num int64) {
	ackStr := fmt.Sprintf(":%d\r\n", num)
	replyRedisAck(req, &ackStr)
}
