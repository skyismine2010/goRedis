package goRedis

import (
	"bufio"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"
)

type redisClient struct {
	createTime      time.Time //todo
	addrInfo        string
	db              *redisDB // 当前使用的db
	cmd             *redisCommand
	lastInterAction time.Duration
	ackChan         chan string
	argc            int
	argv            []string
	conn            net.Conn
	scanner         *bufio.Scanner
	writer          *bufio.Writer
}

type redisReq struct {
	client *redisClient
}

type cmdProcess func(req *redisReq)

type redisServer struct {
	reqChan chan *redisReq
	dbNum   int
	db      []redisDB
}

type redisObj struct {
	rType     int8
	rEncoding int8
	value     interface{}
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
var ReplyNoBulk = "$-1\r\n"

var redisCommandTable map[string]*redisCommand

func init() {
	redisCommandTable = map[string]*redisCommand{
		"command": &redisCommand{cmdCommandHandler, 0, "ltR", 1, 1, 1, 0, 0, 0},
		"set":     &redisCommand{cmdSetHandler, 3, "wm", 1, 1, 1, 0, 0, 0},
		"get":     &redisCommand{cmdGetHandler, 2, "rF", 1, 1, 1, 0, 0, 0},
	}
}

func recvClientReq(client *redisClient) error {
	var err error

	if client.scanner.Scan() {
		text := client.scanner.Text()
		if text[0] != '*' {
			return fmt.Errorf("Client Format Error.")
		}
		client.argc, err = strconv.Atoi(text[1:])
		if err != nil {
			return err
		}
	}

	frameLen := 0

	for i := 0; i < client.argc*2; i++ {
		if client.scanner.Scan() {
			text := client.scanner.Text()
			if i%2 == 0 {
				if text[0] != '$' {
					return fmt.Errorf("client format error. text=%s", text)
				}
				frameLen, err = strconv.Atoi(text[1:])
				if err != nil {
					return fmt.Errorf("client format error, text=%s", text)
				}
			} else {
				if frameLen == 0 || len(text) != frameLen {
					return fmt.Errorf("client format error, text=%s", text)
				} else {
					client.argv = append(client.argv, text)
				}
				frameLen = 0
			}
		}
	}
	return nil
}

func sendClientAck(client *redisClient, s string) error {
	if _, err := client.writer.Write([]byte(s)); err != nil {
		fmt.Printf("send redis ack to client failed, err=%v\n", err)
		return err
	}
	client.writer.Flush()
	return nil
}

func initClient(conn net.Conn, client *redisClient) {
	client.ackChan = make(chan string) // 阻塞式chan
	client.addrInfo = conn.RemoteAddr().String()
	client.createTime = time.Now()
	client.scanner = bufio.NewScanner(conn)
	client.writer = bufio.NewWriter(conn)
	client.db = &server.db[0] //和redis的实现暂时保持一致
}

func clientConnHandler(conn net.Conn) {
	defer conn.Close()

	var client redisClient
	initClient(conn, &client)
	for {
		err := recvClientReq(&client)
		if err != nil || client.argc == 0 || client.argc != len(client.argv) {
			fmt.Printf("error receivce message, err=%v, client.argc=%d", err, client.argc)
			return
		}
		fmt.Printf("redis client receive argc  = %d, argv=%v client = %v\n",
			client.argc, client.argv, conn.RemoteAddr())

		sendReqToRedisServer(&client)

		ack := recvAckFromRedisServer(&client)

		err = sendClientAck(&client, ack)
		if err != nil {
			fmt.Printf("error send mesaage err=%v", err)
			return
		}

		client.argc = 0
		client.argv = nil
	}
}

func recvAckFromRedisServer(client *redisClient) string {
	ack := <-client.ackChan
	return ack
}

func sendReqToRedisServer(client *redisClient) {
	req := redisReq{client}
	server.reqChan <- &req //因为有buffer，所以是一个非阻塞的操作
}

func listenAndServe(ip string, port int) error {
	addr := ip + ":" + strconv.Itoa(port)

	listener, err := net.Listen("tcp4", addr)
	if err != nil {
		fmt.Println(err)
		return err
	}

	for true {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("can't accept client socket, err=%v", err)
			continue
		}
		fmt.Printf("accept conn, remote info = %v\n", conn.RemoteAddr())
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
	fmt.Printf("Send [Command] Ack = %s", ackStr)
	replyRedisAck(req.client, &ackStr)
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
				return fmt.Errorf("Unsupported command flag")
			}
		}
	}
	return nil
}

func initServer() error {
	server.reqChan = make(chan *redisReq, 2048)
	if err := initRedisCmdTable(); err != nil {
		return err
	}

	initRedisDb(&server)

	return nil
}

func serverMainLoop() {
	for {
		select {
		case redisReq := <-server.reqChan:
			serverMsgHandler(redisReq)
		}
	}
}

func serverMsgHandler(req *redisReq) {
	cmdName := strings.ToLower(req.client.argv[0])
	cmd, ok := redisCommandTable[cmdName]
	if !ok {
		replyErrorFormat(req, "unknown command `%s`", cmdName)
		return
	}
	req.client.cmd = cmd
	if req.client.cmd.arity > req.client.argc {
		replyErrorFormat(req, "-wrong number of arguments for '%s' command",
			cmdName)
		return
	}

	cmd.process(req)
}

func StartServer() {
	err := initServer()
	if err != nil {
		fmt.Printf("init Server failed. err = %v\n", err)
		return
	}
	go serverMainLoop()

	ip := "0.0.0.0"
	port := 6379
	listenAndServe(ip, port)
}
