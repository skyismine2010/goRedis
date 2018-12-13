package goRedis

import (
	"bufio"
	"errors"
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"
	"time"
)

type redisClient struct {
	createTime      time.Time //todo
	addrInfo        string
	db              *redisDB // 当前使用的db
	lastInterAction time.Duration
	conn            net.Conn
	scanner         *bufio.Scanner
	writer          *bufio.Writer
	clientChan      chan string
	clientErrChan   chan int
}

type redisReq struct {
	clientChan chan string
	db         *redisDB
	cmd        *redisCommand
	argc       int
	argv       []string
}

type cmdProcess func(req *redisReq)

type redisServer struct {
	reqChan   chan *redisReq
	dbNum     int
	db        []redisDB
	loopTimer *time.Ticker
}

const ObjString = int8(0)
const ObjList = int8(1)
const ObjSet = int8(2)
const ObjZSet = int8(3)
const ObjHash = int8(4)

const ObjEncodingStr = int8(0)
const ObjEncodingINT = int8(1)
const ObjEncodingHT = int8(2)
const ObjEncodingList = int8(3)

type redisObj struct {
	rType    int8 //redis类型
	encoding int8 //内部存储类型
	value    interface{}
	ttl      time.Time
}

type redisCommand struct {
	process     cmdProcess
	arity       int
	sFlags      string
	flags       int32
	firstKey    int
	lastKey     int
	keyStep     int
	microsecond int64
	calls       int64
}

var server redisServer

const ActiveExpireCycleLookupsPerLoop = 20

const CMD_WRITE = int32(1 << 0)              /* "w" flag */
const CMD_READONLY = int32(1 << 1)           /* "r" flag */
const CMD_DENYOOM = int32(1 << 2)            /* "m" flag */
const CMD_MODULE = int32(1 << 3)             /* Command exported by module. */
const CMD_ADMIN = int32(1 << 4)              /* "a" flag */
const CMD_PUBSUB = int32(1 << 5)             /* "p" flag */
const CMD_NOSCRIPT = int32(1 << 6)           /* "s" flag */
const CMD_RANDOM = int32(1 << 7)             /* "R" flag */
const CMD_SORT_FOR_SCRIPT = int32(1 << 8)    /* "S" flag */
const CMD_LOADING = int32(1 << 9)            /* "l" flag */
const CMD_STALE = int32(1 << 10)             /* "t" flag */
const CMD_SKIP_MONITOR = int32(1 << 11)      /* "M" flag */
const CMD_ASKING = int32(1 << 12)            /* "k" flag */
const CMD_FAST = int32(1 << 13)              /* "F" flag */
const CMD_MODULE_GETKEYS = int32(1 << 14)    /* Use the modules getkeys interface. */
const CMD_MODULE_NO_CLUSTER = int32(1 << 15) /* Deny on Redis Cluster. */

var ReplyOK = "+OK\r\n"
var ReplyErr = "-ERR\r\n"
var ReplyEmptyBulk = "$0\r\n\r\n"
var ReplyNullBulk = "$-1\r\n"
var ReplyEmptyMultiBulk = "*0\r\n"
var ReplySyntaxErr = "-ERR syntax error\r\n"
var ReplyWrongTypeErr = "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"

var redisCommandTable map[string]*redisCommand

var ReadTimeout = errors.New("scan time out")
var FormatWrong = errors.New("format wrong")
var ContinueRead = errors.New("continue read")

//arity 是负数的意思是必须大于等于 abs(arity)
func init() {
	redisCommandTable = map[string]*redisCommand{
		"command": &redisCommand{cmdCommandHandler, 0, "ltR", 1, 1, 1, 0, 0, 0},
		"set":     &redisCommand{cmdSetHandler, 3, "wm", 1, 1, 1, 0, 0, 0},
		"get":     &redisCommand{cmdGetHandler, 2, "rF", 1, 1, 1, 0, 0, 0},
		"lpush":   &redisCommand{cmdLPushHandler, 3, "wmF", 1, 1, 1, 0, 0, 0},
		"rpush":   &redisCommand{cmdRPushHandler, 3, "wmF", 1, 1, 1, 0, 0, 0},
		"llen":    &redisCommand{cmdLLenHandler, 2, "rF", 0, 1, 1, 1, 0, 0},
		"lrange":  &redisCommand{cmdLRangeHandler, 2, "rF", 0, 1, 1, 1, 0, 0},
		"blpop":   &redisCommand{cmdBLPopHandler, 2, "ws", 0, 1, 1, 1, 0, 0},
		"brpop":   &redisCommand{cmdBRPopHandler, 2, "ws", 0, 1, 1, 1, 0, 0},
		"lpop":    &redisCommand{cmdLPopHandler, 2, "wF", 0, 1, 1, 1, 0, 0},
		"rpop":    &redisCommand{cmdRPopHandler, 2, "wF", 0, 1, 1, 1, 0, 0},
		"hmset":   &redisCommand{cmdHmSetHandler, -4, "wmF", 0, 1, 1, 1, 0, 0},
	}
}

func receiveClientReq(client *redisClient) (*redisReq, error) {
	var err error
	var req redisReq

	if client.scanner.Scan() {
		text := client.scanner.Text()
		if text[0] != '*' {
			return nil, FormatWrong
		}
		req.argc, err = strconv.Atoi(text[1:])
		if err != nil {
			return nil, err
		}
	} else {
		return nil, nil //return asap
	}

	frameLen := 0

	for i := 0; i < req.argc*2; i++ {
		if client.scanner.Scan() {
			text := client.scanner.Text()
			if i%2 == 0 {
				if text[0] != '$' {
					return nil, FormatWrong
				}
				frameLen, err = strconv.Atoi(text[1:])
				if err != nil {
					return nil, fmt.Errorf("client format error, text=%s", text)
				}
			} else {
				if len(text) != frameLen {
					return nil, fmt.Errorf("client format error, text=%s", text)
				} else {
					req.argv = append(req.argv, text)
				}
				frameLen = 0
			}
		}
	}
	req.db = client.db
	req.clientChan = client.clientChan
	return &req, nil
}

func sendClientAck(client *redisClient, s string) error {
	if _, err := client.writer.Write([]byte(s)); err != nil {
		log.Printf("send redis ack to client failed, err=%v\n", err)
		return err
	}
	//log.Printf("send redis ack to client success ,ack = %s", s)
	client.writer.Flush()
	return nil
}

func nonEmptySplit(data []byte, atEOF bool) (advance int, token []byte, err error) {
	advance, token, err = bufio.ScanLines(data, atEOF)
	if len(token) == 0 {
		return 0, nil, nil
	}
	return advance, token, err

}

func initClient(conn net.Conn, client *redisClient) {
	client.clientChan = make(chan string, 1024) // 阻塞式chan不合理，对于pipe的场景处理有问题，所以改成非阻塞
	client.clientErrChan = make(chan int, 2)
	client.addrInfo = conn.RemoteAddr().String()
	client.createTime = time.Now()
	client.scanner = bufio.NewScanner(conn)
	client.scanner.Split(nonEmptySplit)
	client.writer = bufio.NewWriter(conn)
	client.db = &server.db[0] //和redis的实现暂时保持一致
}

func redisReqHandler(client *redisClient) {
	for {
		req, err := receiveClientReq(client)
		if err != nil {
			log.Printf("error receive message, err=%v", err)
			client.clientErrChan <- 1
			return
		}

		if req != nil {
			sendReqToRedisServer(req)
		}
	}

}

func clientConnHandler(conn net.Conn) {
	defer conn.Close()

	var client redisClient
	initClient(conn, &client)
	go redisReqHandler(&client)
	go redisAckHandler(&client)

	for {
		select {
		case rwFlag := <-client.clientErrChan:
			if rwFlag == 1 {
				log.Printf("Read failed. conn close")
			} else {
				log.Printf("Write failed. conn.close")
			}
			return
		default:
			continue
		}
	}
}

func redisAckHandler(client *redisClient) error {
	log.Printf("Prepared to ack ack ack ")
	for {
		select {
		case ack := <-client.clientChan:
			err := sendClientAck(client, ack)
			if err != nil {
				log.Printf("error send mesaage err=%v", err)
				return err
			}
			//	log.Printf("Send msg to client finish")
		}
	}
}

func sendReqToRedisServer(req *redisReq) {
	server.reqChan <- req
}

func listenAndServe(ip string, port int) error {
	addr := ip + ":" + strconv.Itoa(port)

	listener, err := net.Listen("tcp4", addr)
	if err != nil {
		log.Println(err)
		return err
	}

	for true {
		conn, err := listener.Accept()
		if err != nil {
			log.Println("can't accept client socket, err=%v", err)
			continue
		}
		log.Printf("accept conn, remote info = %v\n", conn.RemoteAddr())
		go clientConnHandler(conn)
	}

	return nil
}

//todo:搞清楚整体逻辑，这个只是最简实现
func cmdCommandHandler(req *redisReq) {
	ackStr := fmt.Sprintf("*%d\r\n", len(redisCommandTable))
	for cmdName, cmd := range redisCommandTable {
		flagCount := 0

		ackStr += *(addReplyMultiBulkLen(6)) // 这个是固定的后面有6个
		ackStr += *(addReplyBulkCString(&cmdName))
		ackStr += *(addReplyInt64(int64(cmd.arity)))
		ackStr += *(addReplyCommand(flagCount, cmd))
		ackStr += *(addReplyInt64(int64(cmd.firstKey)))
		ackStr += *(addReplyInt64(int64(cmd.lastKey)))
		ackStr += *(addReplyInt64(int64(cmd.keyStep)))
	}
	replyRedisAck(req, &ackStr)
}

func addReplyCommand(flagCount int, cmd *redisCommand) *string {
	var flagBuff string
	flagCount += addReplyCommandFlag(&flagBuff, cmd, CMD_WRITE, "write")
	flagCount += addReplyCommandFlag(&flagBuff, cmd, CMD_READONLY, "readonly")
	flagCount += addReplyCommandFlag(&flagBuff, cmd, CMD_DENYOOM, "denyoom")
	flagCount += addReplyCommandFlag(&flagBuff, cmd, CMD_ADMIN, "admin")
	flagCount += addReplyCommandFlag(&flagBuff, cmd, CMD_PUBSUB, "pubsub")
	flagCount += addReplyCommandFlag(&flagBuff, cmd, CMD_NOSCRIPT, "noscript")
	flagCount += addReplyCommandFlag(&flagBuff, cmd, CMD_RANDOM, "random")
	flagCount += addReplyCommandFlag(&flagBuff, cmd, CMD_SORT_FOR_SCRIPT, "sort_for_script")
	flagCount += addReplyCommandFlag(&flagBuff, cmd, CMD_STALE, "stale")
	flagCount += addReplyCommandFlag(&flagBuff, cmd, CMD_LOADING, "loading")
	flagCount += addReplyCommandFlag(&flagBuff, cmd, CMD_SKIP_MONITOR, "skip_monitor")
	flagCount += addReplyCommandFlag(&flagBuff, cmd, CMD_ASKING, "asking")
	flagCount += addReplyCommandFlag(&flagBuff, cmd, CMD_FAST, "fast")
	ret := *(addReplyMultiBulkLen(int64(flagCount)))
	ret += flagBuff
	return &ret
}

func initRedisCmdTable() error {
	for k, _ := range redisCommandTable {
		for _, ch := range redisCommandTable[k].sFlags {
			switch ch {
			case 'w':
				redisCommandTable[k].flags |= CMD_WRITE
			case 'r':
				redisCommandTable[k].flags |= CMD_READONLY
			case 'm':
				redisCommandTable[k].flags |= CMD_DENYOOM
			case 'a':
				redisCommandTable[k].flags |= CMD_ADMIN
			case 'p':
				redisCommandTable[k].flags |= CMD_PUBSUB
			case 's':
				redisCommandTable[k].flags |= CMD_NOSCRIPT
			case 'R':
				redisCommandTable[k].flags |= CMD_RANDOM
			case 'S':
				redisCommandTable[k].flags |= CMD_SORT_FOR_SCRIPT
			case 'l':
				redisCommandTable[k].flags |= CMD_LOADING
			case 't':
				redisCommandTable[k].flags |= CMD_STALE
			case 'M':
				redisCommandTable[k].flags |= CMD_SKIP_MONITOR
			case 'k':
				redisCommandTable[k].flags |= CMD_ASKING
			case 'F':
				redisCommandTable[k].flags |= CMD_FAST
			default:
				return fmt.Errorf("unsupported command flag")
			}
		}
	}
	return nil
}

func initLog() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds | log.Llongfile)
	log.Printf("init log ok ...")
}

func initServer() error {
	initLog()
	server.reqChan = make(chan *redisReq, 2048)
	if err := initRedisCmdTable(); err != nil {
		return err
	}

	initRedisDb(&server)
	server.loopTimer = time.NewTicker(time.Millisecond)

	return nil
}

func serverMainLoop() {
	for {
		select {
		case redisReq := <-server.reqChan:
			serverMsgHandler(redisReq)
		case <-server.loopTimer.C:
			timerHandler()
		}
	}
}

func serverMsgHandler(req *redisReq) {
	cmdName := strings.ToLower(req.argv[0])
	//	log.Printf("goRedis server receive msg = %s\n", cmdName)
	cmd, ok := redisCommandTable[cmdName]
	if !ok {
		replyErrorFormat(req, "unknown command `%s`", cmdName)
		return
	}
	req.cmd = cmd

	if (req.cmd.arity > 0 && req.cmd.arity != req.argc) ||
		(req.argc < -1*(req.cmd.arity)) {
		replyErrorFormat(req, "-wrong number of arguments for '%s' command",
			cmdName)
		return
	}

	cmd.process(req)
}

func StartServer() {
	err := initServer()
	if err != nil {
		log.Printf("init Server failed. err = %v\n", err)
		return
	}
	go serverMainLoop()

	ip := "0.0.0.0"
	port := 6379
	listenAndServe(ip, port)
}
