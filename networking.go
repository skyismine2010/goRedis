package goRedis

import "fmt"

func addReplyMultiBulkLen(str *string, length int64) {
	*str += fmt.Sprintf("*%d\r\n", length)
}

func addReplyBulkCString(str *string, subStr *string) {
	*str += fmt.Sprintf("$%d\r\n%s\r\n", len(*subStr), *subStr)
}

func addReplyint64(str *string, length int64) {
	*str += fmt.Sprintf(":%d\r\n", length)
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
	replyRedisAck(req.client, &ackStr)
}

func replyRedisAck(client *redisClient, ackMsg *string) {
	client.ackChan <- *ackMsg
}
