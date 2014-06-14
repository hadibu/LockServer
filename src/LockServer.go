package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"
)

func PrintPanicStack() {
	if reco := recover(); reco != nil {
		log.Printf("%v", reco)
		for i := 0; i < 10; i++ {
			funcName, fileName, line, ok := runtime.Caller(i)
			if ok {
				log.Printf("frame %v:[func:%v,file:%v,line:%v]\n", i, runtime.FuncForPC(funcName).Name(), fileName, line)
			}
		}
	}
}

///////////////////////////////////packet start///////////////////////////////////////
// 消息类型常量
const (
	PACKET_TYPE_TEST = 0
	PACKET_TYPE_LOCK = 1
)

type Packet struct {
	Type int
	Data string
}

// 返回该Packet的json字符串，用于与客户端通信
func (this *Packet) String() string {
	b, err := json.Marshal(this)
	if err != nil {
		log.Println("error:", err)
	}
	return string(b)
}

func (this *Packet) Size() int {
	b, err := json.Marshal(this)
	if checkError(err) {
		return 0
	}
	return len(b)
}

const (
	// 存储Packet长度的字符串长度
	PACKET_SIZE_BUF_LEN = 6
)

///////////////////////////////////packet end///////////////////////////////////////

// per session per lock
type Session struct {
	SID           int
	Conn          *net.TCPConn
	LockType      string
	ChannelParent chan *Session // session所属channel
}

func (this *Session) Close() bool {
	if this.Conn != nil {
		(*this.Conn).Close()
		this.Conn = nil
	}

	return true
}

var g_CurrentConnentions int
var g_MaxConnections int

func checkError(err error) bool {
	if err != nil {
		log.Println("Error = ", err)
		return true
	}
	return false
}

func StartTCP(host string, port int) {
	defer PrintPanicStack()

	log.Println("listen port = ", port)

	s := fmt.Sprintf(":%d", port)
	addr, err := net.ResolveTCPAddr("tcp", host + s)
	listener, err := net.ListenTCP("tcp", addr)
	if checkError(err) {
		return
	}

	var sid int
	sid = 0

	for {
		conn, err := listener.AcceptTCP()
		if checkError(err) {
			continue
		}

		var session Session
		session.SID = sid
		session.Conn = conn
		session.LockType = ""
		session.ChannelParent = nil

		conn.SetNoDelay(true)
		conn.SetKeepAlive(true)
		conn.SetLinger(0)

		sid++
		g_CurrentConnentions++
		if g_CurrentConnentions > g_MaxConnections {
			g_MaxConnections = g_CurrentConnentions
			log.Println("MaxConnections = ", g_MaxConnections)
		}
		log.Println("CurrentConnentions = ", g_CurrentConnentions, " TotalConnections = ", sid)

		go handleClient(&session)
	}
}

func handleClient(session *Session) {
	log.Println("accept client = ", (*(session.Conn)).RemoteAddr())

	defer PrintPanicStack()
	defer closeClient(session)

	for {
		if session == nil || session.Conn == nil {
			break
		}

		sizeChunk := make([]byte, PACKET_SIZE_BUF_LEN)
		headersize, err := io.ReadFull(session.Conn, sizeChunk)
		if headersize == 0 || headersize != PACKET_SIZE_BUF_LEN || err == io.EOF {
			break
		}
		declareSize, _ := strconv.Atoi(string(sizeChunk))
		dataChunk := make([]byte, declareSize)
		readSize, err := io.ReadFull((session.Conn), dataChunk)
		if readSize == 0 || readSize != declareSize || err == io.EOF {
			break
		}
		if err != nil {
			log.Println("recv data err = ", err, " client = ", (*(session.Conn)).RemoteAddr())
			break
		}
		dataBuf := string(dataChunk)
		dec := json.NewDecoder(strings.NewReader(dataBuf))
		var packet Packet
		err = dec.Decode(&packet)
		if err != nil {
			log.Println("json decode data = ", dataBuf, " err =", err, " client = ", (*(session.Conn)).RemoteAddr())
			break
		}

		//println("sizeChunk = ", string(sizeChunk), "\ndataBuf = ", dataBuf)

		if PACKET_TYPE_LOCK == packet.Type {
			// 开始等待packet.Data锁
			if !WaitForLock(packet.Data, session) {
				log.Println("WaitForLock err", " client = ", (*(session.Conn)).RemoteAddr())
				break
			}

			// reuse packet
			packet.Data = "true"

			// 先写入packet长度，再写入packet数据
			writeSizeStr := fmt.Sprintf("%06d", packet.Size())
			_, err := io.WriteString(session.Conn, writeSizeStr + packet.String())
			if err != nil {
				log.Println("WriteString err", err, " client = ", (*(session.Conn)).RemoteAddr())
				break
			}
		}
	}

	//log.Println("handleClient routine exit", (*(session.Conn)).RemoteAddr())
	g_CurrentConnentions--
}

func closeClient(session *Session) {
	if session != nil {
		if session.Conn != nil {
			log.Println("break client = ", (*(session.Conn)).RemoteAddr())
		}
		ReleaseLock(session)
		session.Close()
	}
}

// 并发访问，每个channel内最多允许LockLimit个元素
// 如果channel内当前元素个数超出LockLimit的话说明
// 当前该锁的占有者已经达到上限，该函数被阻塞住
func WaitForLock(lockType string, session *Session) bool {
	if session == nil {
		return false
	}

	channel := getChannel(lockType)
	if channel == nil {
		log.Println("lock ", lockType, "'s channel is nil", " client = ", (*(session.Conn)).RemoteAddr())
		return false
	}

	log.Println("等待锁", lockType, (*(session.Conn)).RemoteAddr())

	session.ChannelParent = channel;
	session.LockType = lockType

	if len(session.ChannelParent) > 0 {
		log.Println("锁占有者数量", len(session.ChannelParent))
	}

	// 尝试写入进通道，无法写入的话进入阻塞状态
	session.ChannelParent <- session

	log.Println("拿到锁", lockType, (*(session.Conn)).RemoteAddr())

	return true
}

// 只要当前关闭的session已占有该锁的话就可以释放任意一个占有该锁的session
// 锁队列保证的是当前占有锁的数量不超过其上限，所以关闭任意一个占有该锁的
// session的话就可以释放锁队列的任意一个元素
func ReleaseLock(session *Session) {
	if session == nil || session.ChannelParent == nil {
		return
	}

	<-session.ChannelParent
	log.Println("释放锁", session.LockType, (*(session.Conn)).RemoteAddr())
}

func initLog() bool {
	file, err := os.Create(CFG.LogPath + "lock_" + time.Now().Format("2006-01-02_15：04：05") + ".log")
	if err != nil {
		log.Println("initLog err = ", err)
		return false
	}
	log.SetOutput(file)
	fmt.Printf("LogFile = %s", file.Name())
	return true
}

const (
	// 默认锁占有者数量限制
	DEFAULT_LOCK_LIMIT = 1
)

// LockType作为key
var g_ChannelMap map[string] chan *Session

func initChannel() {
	g_ChannelMap = make(map[string] chan *Session)
	for i := range CFG.Locks {
		g_ChannelMap[CFG.Locks[i].LockType] = make(chan *Session, CFG.Locks[i].LockLimit)
	}
}

func getChannel(lockType string) chan *Session {
	return g_ChannelMap[lockType]
}

func startServer() {

	initLog()

	runtime.GOMAXPROCS(CFG.CPU)

	initChannel()

	StartTCP(CFG.BindIp, CFG.ListenPort)
}

type LockConfig struct {
	LockType  string
	LockLimit int
}

type Config struct {
	CPU             int          // 进程最多占用cpu个数
	ListenPort      int          // 绑定IP
	BindIp          string       // 监听端口
	LogPath         string       // log路径
	Locks		   []LockConfig // 锁配置
}

var CFG Config

func parseConfig(cfgPath string) bool {
	file, err := os.Open(cfgPath)
	if err != nil {
		log.Println("parseConfig err = ", err)
		return false
	}

	defer file.Close()

	bytes := make([]byte, 4096)
	size, err := file.Read(bytes)
	if err != nil || size >= 4096 {
		println("parseConfig err = ", err)
		return false
	}
	data := string(bytes)
	dec := json.NewDecoder(strings.NewReader(data))
	err = dec.Decode(&CFG)
	if err != nil {
		log.Fatal("parse", data, "\nerr =", err)
		return false
	}
	log.Println("CFG = ", CFG)
	return true
}

func main() {
	parseConfig(os.Args[1])
	startServer()
}
