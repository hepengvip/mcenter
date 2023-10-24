package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"sync"

	"golang.org/x/net/websocket"

	proto "github.com/hepengvip/mcenter-proto"
)

func Out() {
	fmt.Println("msg", proto.MSG_MESSAGE)
}

type Client struct {
	conn    net.Conn
	wsConn  *websocket.Conn
	userId  string
	ch      chan proto.Message
	replyCh chan proto.Response
}

func NewClient(conn net.Conn, wsConn *websocket.Conn) *Client {
	return &Client{
		conn:    conn,
		wsConn:  wsConn,
		ch:      make(chan proto.Message),
		replyCh: make(chan proto.Response),
	}
}

func (c *Client) IsWebsocketClient() bool {
	return c.wsConn != nil && c.conn == nil
}

func (c *Client) IsSocketClient() bool {
	return c.wsConn == nil && c.conn != nil
}

type UserClient struct {
	sync.RWMutex
	clients map[string]*Client
}

func NewUserClient() *UserClient {
	return &UserClient{
		clients: make(map[string]*Client),
	}
}

/*
通过userId获取Client对象

@Params
  - client 指向Client的指针
  - userId

@Returns
  - error 如果userId非法 或 userId已存在， 否则返回nil
*/
func (uc *UserClient) SetUserId(client *Client, userId string) error {
	if userId == "" {
		return errors.New("userId非法")
	}

	// 加写锁
	uc.Lock()
	defer uc.Unlock()

	if _, ok := uc.clients[userId]; ok {
		return errors.New("userId已存在")
	}

	uc.clients[userId] = client
	client.userId = userId

	return nil
}

func (uc *UserClient) UserOut(userId string) {

	// 加写锁
	uc.Lock()
	defer uc.Unlock()

	cli, ok := uc.clients[userId]
	if !ok {
		return
	}

	cli.userId = ""
	delete(uc.clients, userId)
}

func (uc *UserClient) GetUserIdList() *[]string {
	uc.RLock()
	defer uc.RUnlock()

	uids := make([]string, 0, len(uc.clients))
	for uid := range uc.clients {
		uids = append(uids, uid)
	}
	return &uids
}

func (uc *UserClient) GetClientByUserId(userId string) *Client {
	// 加读锁
	uc.RLock()
	defer uc.RUnlock()

	return uc.clients[userId]
}

type SubRegistry struct {
	sync.RWMutex
	registry    map[string][]string // chan -> []userId
	subRegistry map[string][]string // userId -> []chan
}

func NewSubRegistry() *SubRegistry {
	return &SubRegistry{
		registry:    make(map[string][]string),
		subRegistry: make(map[string][]string),
	}
}

/*
创建新频道

@Params
  - channel 频道名称

@Returns
  - true 创建了新频道， false 频道已经存在
*/
func (r *SubRegistry) NewChannel(channel string) bool {

	// 加写锁
	r.Lock()
	defer r.Unlock()

	if _, ok := r.registry[channel]; ok {
		return false
	}

	r.registry[channel] = make([]string, 0)

	return true
}

/*
订阅一个频道

@Params
  - channel 频道名称
  - userId

@Returns
  - true 当且仅当增加了一个新的userId时
  - error 发生的错误
*/
func (r *SubRegistry) SubChannel(channel string, userId string) (bool, error) {

	// 加写锁
	r.Lock()
	defer r.Unlock()

	// 检查channel是否存在
	userIdList, ok := r.registry[channel]
	if !ok {
		return false, errors.New("channel不存在")
	}

	for _, _userId := range userIdList {
		if _userId == userId {
			return false, nil
		}
	}

	r.registry[channel] = append(r.registry[channel], userId)
	r.subRegistry[userId] = append(r.subRegistry[userId], channel)

	return true, nil
}

/*
取消频道订阅

@Params
  - channel 频道名称
  - userId

@Returns
  - true 当且仅当减少了一个userId时
  - error 发生的错误
*/
func (r *SubRegistry) UnsubChannel(channel string, userId string) (bool, error) {

	// 加写锁
	r.Lock()
	defer r.Unlock()

	// 检查channel是否存在
	userIdList, ok := r.registry[channel]
	if !ok {
		return false, errors.New("channel不存在")
	}

	// 从registry中删除userId
	idx := -1
	for _idx, _userId := range userIdList {
		if _userId == userId {
			idx = _idx
		}
	}

	if idx != -1 { // remove
		r.registry[channel][idx] = r.registry[channel][len(r.registry[channel])-1]
		r.registry[channel] = r.registry[channel][:len(r.registry[channel])-1]
	}

	// 从subRegistry中删除channel
	idx = -1
	for _idx, _channel := range r.subRegistry[userId] {
		if _channel == channel {
			idx = _idx
		}
	}

	if idx != -1 {
		r.subRegistry[userId][idx] = r.subRegistry[userId][len(r.subRegistry[userId])-1]
		r.subRegistry[userId] = r.subRegistry[userId][:len(r.subRegistry[userId])-1]
	}

	return true, nil
}

/*
获取所有频道

@Params
  - userId
*/
func (r *SubRegistry) GetUserChannels(userId string) *[]string {
	r.RLock()
	defer r.RUnlock()

	userIdList, ok := r.subRegistry[userId]
	if !ok {
		return nil
	}

	return &userIdList
}

/*
用户离开

@Params
  - userId
*/
func (r *SubRegistry) UserOut(userId string) {

	channels := r.GetUserChannels(userId)
	if channels == nil {
		return
	}

	for _, channel := range *channels {
		r.UnsubChannel(channel, userId)
	}

	// TODO remove user
	delete(r.subRegistry, userId)
}

type Server struct {
	registery *SubRegistry
	uc        *UserClient
}

func (s *Server) Publish(
	channel string, userId string, message []byte,
) error {

	// 加读锁
	s.registery.RLock()
	defer s.registery.RUnlock()

	// 检查消息
	if len(message) <= 0 {
		return errors.New("消息非法")
	}

	// 检查channel是否存在
	userIdList, ok := s.registery.registry[channel]
	if !ok {
		return errors.New("channel不存在")
	}

	// 加读锁
	s.uc.RLock()
	defer s.uc.RUnlock()

	for _, _userId := range userIdList {
		if _userId == userId {
			continue
		}

		cli, ok := s.uc.clients[_userId]
		if !ok {
			// TODO 移除客户端
			continue
		}

		cli.ch <- proto.Message{
			ReqType:     proto.MSG_MESSAGE,
			UserId:      userId,
			Channel:     channel,
			PayloadSize: len(message),
			Payload:     &message,
		}
	}

	return nil
}

func (s *Server) StartHttpServer(addr string) {
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, "mcenter version 0.1.0")
	})
	http.HandleFunc("/stats", func(w http.ResponseWriter, r *http.Request) {

		type Stat struct {
			UserIds          *[]string           `json:"UserIds"`
			UserSubscrube    map[string][]string `json:"UserSubscrube"`
			ChannelSubscrube map[string][]string `json:"ChannelSubscrube"`
		}
		stats, err := json.Marshal(Stat{
			UserIds:          s.uc.GetUserIdList(),
			UserSubscrube:    s.registery.subRegistry,
			ChannelSubscrube: s.registery.registry,
		})
		if err != nil {
			io.WriteString(w, "{}")
		} else {
			io.WriteString(w, string(stats))
		}
	})
	http.Handle("/ws", websocket.Handler(s.handleWsConn))

	http.ListenAndServe(addr, nil)
}

func (s *Server) handleWsConn(ws *websocket.Conn) {
	defer ws.Close()

	client := NewClient(nil, ws)
	go beginWriter(client)

	reader := bufio.NewReader(ws)
	s.handler(client, reader)
}

func StartServer(addr, httpAddr string) error {
	l, err := net.Listen("tcp", addr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "listen failed - %v\n", err)
		return err
	}
	defer l.Close()

	s := &Server{
		uc:        NewUserClient(),
		registery: NewSubRegistry(),
	}

	go s.StartHttpServer(httpAddr)

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Fprintf(os.Stderr, "accept failed - %v\n", err)
			return err
		}

		go handleConnection(conn, s)
	}
}

func handleConnection(conn net.Conn, s *Server) {
	defer conn.Close()

	client := NewClient(conn, nil)
	go beginWriter(client)
	reader := bufio.NewReader(conn)

	s.handler(client, reader)
}

func (s *Server) handler(client *Client, reader *bufio.Reader) {
	for {
		data, err := proto.ReadHeader(reader, '\n')
		if err == io.EOF {
			if client.userId != "" {
				fmt.Fprintf(os.Stdout, "user out [%s]\n", client.userId)
				s.registery.UserOut(client.userId)
				s.uc.UserOut(client.userId)
			}
			break
		} else if err != nil {
			if client.userId != "" {
				fmt.Fprintf(os.Stdout, "user out [%s]\n", client.userId)
				s.registery.UserOut(client.userId)
				s.uc.UserOut(client.userId)
			}
			break
		}

		fmt.Fprintf(os.Stdout, "###################### debug ######################\n")
		fmt.Fprintf(os.Stdout, "  Received message [%s]\n", string(*data))
		fmt.Fprintf(os.Stdout, "###################################################\n")

		msg, err := proto.Parse(*data)
		if err != nil {
			client.replyCh <- proto.Response{ReqCode: 100002, ReqMsg: fmt.Sprintf("Unknown command - %s", err.Error())}
			continue
		}

		switch msg.ReqType {
		case proto.MSG_SET_USER:
			if client.userId != "" {
				client.replyCh <- proto.Response{ReqId: msg.ReqId, ReqCode: 100002, ReqMsg: "Unknown command"}
				continue
			}

			err = s.uc.SetUserId(client, msg.UserId)
			if err != nil {
				client.replyCh <- proto.Response{ReqId: msg.ReqId, ReqCode: 100003, ReqMsg: err.Error()}
			} else {
				client.replyCh <- proto.Response{ReqId: msg.ReqId, ReqMsg: "ok"}
			}
		case proto.MSG_SUBSCRIBE:
			if client.userId == "" {
				client.replyCh <- proto.Response{ReqId: msg.ReqId, ReqCode: 100110, ReqMsg: "userId not set"}
				continue
			}

			_, err := s.registery.SubChannel(msg.Channel, client.userId)
			if err != nil {
				client.replyCh <- proto.Response{ReqId: msg.ReqId, ReqCode: 100003, ReqMsg: err.Error()}
			} else {
				client.replyCh <- proto.Response{ReqId: msg.ReqId, ReqMsg: "ok"}
			}
		case proto.MSG_UNSUBSCRIBE:
			if client.userId == "" {
				client.replyCh <- proto.Response{ReqId: msg.ReqId, ReqCode: 100110, ReqMsg: "userId not set"}
				continue
			}

			// unsubscribe
			_, err := s.registery.UnsubChannel(msg.Channel, client.userId)
			if err != nil {
				client.replyCh <- proto.Response{ReqId: msg.ReqId, ReqCode: 100003, ReqMsg: err.Error()}
			} else {
				client.replyCh <- proto.Response{ReqId: msg.ReqId, ReqMsg: "ok"}
			}
		case proto.MSG_NEW_CHANNEL:
			if client.userId == "" {
				client.replyCh <- proto.Response{ReqId: msg.ReqId, ReqCode: 100110, ReqMsg: "userId not set"}
				continue
			}

			ok := s.registery.NewChannel(msg.Channel)
			cnt := 0
			if ok {
				cnt = 1
			}
			client.replyCh <- proto.Response{ReqId: msg.ReqId, ReqMsg: fmt.Sprintf("%d new channel created.", cnt)}
		case proto.MSG_PUBLISH:
			if client.userId == "" {
				client.replyCh <- proto.Response{ReqId: msg.ReqId, ReqCode: 100110, ReqMsg: "userId not set"}
				continue
			}

			err = msg.ReadPayload(reader)
			if err != nil {
				client.replyCh <- proto.Response{ReqId: msg.ReqId, ReqCode: 100003, ReqMsg: err.Error()}
				return
			}

			printMsg(msg)

			err = s.Publish(msg.Channel, msg.UserId, *msg.Payload)
			if err != nil {
				client.replyCh <- proto.Response{ReqId: msg.ReqId, ReqCode: 100003, ReqMsg: err.Error()}
			} else {
				client.replyCh <- proto.Response{ReqId: msg.ReqId, ReqMsg: "ok"}
			}
		case proto.MSG_MESSAGE:
			panic("unimplemented")
		}
	}
}

func beginWriter(client *Client) {
	var data []byte
	for {
		select {
		case msg := <-client.ch:
			data = msg.ToBytes()
		case rMsg := <-client.replyCh:
			data = rMsg.ToBytes()
		}

		fmt.Fprintf(os.Stdout, "###################### debug ######################\n")
		fmt.Fprintf(os.Stdout, "  Send message [%s]\n", string(data))
		fmt.Fprintf(os.Stdout, "###################################################\n")

		var n int
		var err error
		if client.IsWebsocketClient() {
			n, err = client.wsConn.Write(data)
		} else if client.IsSocketClient() {
			n, err = client.conn.Write(data)
		} else {
			fmt.Fprintf(os.Stderr, "Fatal: Server start error\n")
			os.Exit(999)
		}

		if err != nil {
			fmt.Fprintf(os.Stderr, "error writing message - %v\n", err)

			// TODO
			continue
		}
		fmt.Fprintf(os.Stdout, "write %d bytes to client\n", n)
	}
}

func printMsg(msg *proto.Message) {
	fmt.Fprintf(os.Stdout, "##################### message #####################\n")
	fmt.Fprintf(os.Stdout, "  ReqType %s\n", string(msg.ReqType))
	fmt.Fprintf(os.Stdout, "  UserId %s\n", string(msg.UserId))
	fmt.Fprintf(os.Stdout, "  Channel %s\n", string(msg.Channel))
	fmt.Fprintf(os.Stdout, "  ReqId %s\n", string(msg.ReqId))
	fmt.Fprintf(os.Stdout, "  PayloadSize %d\n", msg.PayloadSize)
	fmt.Fprintf(os.Stdout, "  Payload %s\n", string(*msg.Payload))
	fmt.Fprintf(os.Stdout, "###################################################\n")
}
