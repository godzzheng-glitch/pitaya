package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"time"

	"strings"

	pitaya "github.com/topfreegames/pitaya/v3/pkg"
	"github.com/topfreegames/pitaya/v3/pkg/acceptor"
	"github.com/topfreegames/pitaya/v3/pkg/component"
	"github.com/topfreegames/pitaya/v3/pkg/config"
	"github.com/topfreegames/pitaya/v3/pkg/groups"
	"github.com/topfreegames/pitaya/v3/pkg/logger"
	"github.com/topfreegames/pitaya/v3/pkg/timer"
)

type (
	// Room 是 Pitaya 的组件实例。嵌入 component.Base 后获得四个生命周期方法的
	// 空实现，因此这里只覆写需要的 AfterInit 即可满足 component.Component 接口。
	//
	// 这个组件通过 app.Register 注册为客户端 Handler 服务；Join 和 Message 会被
	// 反射提取为可由客户端调用的方法。timer 保存定时器句柄，app 用于访问会话、
	// 群组和推送等 Pitaya 运行时能力。

	// Room represents a component that contains a bundle of room related handler
	// like Join/Message
	Room struct {
		component.Base
		timer *timer.Timer
		app   pitaya.Pitaya
	}

	// UserMessage 是客户端 notify "room.message" 的 JSON 请求体。
	// 指针参数会由 Pitaya 按当前序列化器反序列化后传给 Message。
	// UserMessage represents a message that user sent
	UserMessage struct {
		Name    string `json:"name"`
		Content string `json:"content"`
	}

	// NewUser 是服务端推送 "onNewUser" 的负载。
	// NewUser message will be received when new user join room
	NewUser struct {
		Content string `json:"content"`
	}

	// AllMembers 是服务端推送 "onMembers" 的负载；Members 保存加入前已有成员的 UID。
	// AllMembers contains all members uid
	AllMembers struct {
		Members []string `json:"members"`
	}

	// JoinResponse 是客户端 request "room.join" 的响应体。
	// JoinResponse represents the result of joining room
	JoinResponse struct {
		Code   int    `json:"code"`
		Result string `json:"result"`
	}
)

// NewRoom 构造一个 Room，并注入应用实例。此时尚未注册路由，也尚未触发生命周期。
// NewRoom returns a Handler Base implementation
func NewRoom(app pitaya.Pitaya) *Room {
	return &Room{
		app: app,
	}
}

// AfterInit 在 app.Start 的组件初始化阶段调用。
// 这里注册全局定时器：每分钟查询名为 room 的群组成员数并记录日志。
// 定时器本身由 Pitaya 的 timer manager 调度，回调使用 Background context，
// 因此不依赖任何一次客户端请求的生命周期。
// AfterInit component lifetime callback
func (r *Room) AfterInit() {
	r.timer = pitaya.NewTimer(time.Minute, func() {
		count, err := r.app.GroupCountMembers(context.Background(), "room")
		logger.Log.Debugf("UserCount: Time=> %s, Count=> %d, Error=> %v", time.Now().String(), count, err)
	})
}

// Join 是客户端 Request Handler，对应路由 room.join。
// 入参 []byte 表示原始请求体，当前没有读取它；返回 (*JoinResponse, error)
// 使它成为 Request 类型 Handler，Pitaya 会向调用方返回序列化后的 JoinResponse。
func (r *Room) Join(ctx context.Context, msg []byte) (*JoinResponse, error) {
	// ctx 在 HandlerService 收到客户端消息后创建，里面携带当前 session。
	s := r.app.GetSessionFromCtx(ctx)
	// demo 用连接级 Session ID 作为 UID。生产环境通常使用登录鉴权得到的业务 UID。
	fakeUID := s.ID()
	// Bind 将 UID 绑定到当前 session，使后续群组、Push 等操作可以通过 UID 寻址。
	err := s.Bind(ctx, strconv.Itoa(int(fakeUID)))

	if err != nil {
		return nil, pitaya.Error(err, "RH-000", map[string]string{"failed": "bind"})
	}

	// 在加入前读取成员列表，随后只把已有成员通知给当前用户。
	uids, err := r.app.GroupMembers(ctx, "room")
	if err != nil {
		return nil, err
	}
	// Push 是点对点服务端推送，事件名由前端 starx.on("onMembers", ...) 订阅。
	s.Push("onMembers", &AllMembers{Members: uids})
	// 向 group=room 内的用户广播。第一个 "chat" 是目标 server type，
	// "room" 是群组名，"onNewUser" 是推送事件；此时新用户尚未入组，
	// 所以广播对象只有已有成员。
	r.app.GroupBroadcast(ctx, "chat", "room", "onNewUser", &NewUser{Content: fmt.Sprintf("New user: %s", s.UID())})
	// 以 UID 加入群组。MemoryGroupService 时数据驻留在当前进程内存。
	r.app.GroupAddMember(ctx, "room", s.UID())

	// 连接关闭时执行清理，避免离线用户仍留在群组成员列表中。
	s.OnClose(func() {
		r.app.GroupRemoveMember(ctx, "room", s.UID())
	})

	return &JoinResponse{Result: "success"}, nil
}

// Message 是客户端 Notify Handler，对应路由 room.message。
// 它没有返回值，Pitaya 将其标记为 Notify；客户端通过 starx.notify 发送后
// 不等待业务响应。收到的 UserMessage 被广播给 room 中的所有 UID。
func (r *Room) Message(ctx context.Context, msg *UserMessage) {
	err := r.app.GroupBroadcast(ctx, "chat", "room", "onMessage", msg)
	if err != nil {
		fmt.Println("error broadcasting message", err)
	}
}

var app pitaya.Pitaya

func main() {

	// 读取并组装应用配置。
	// configApp 通常负责加载配置文件、环境变量及默认配置。
	conf := configApp()

	builder := pitaya.NewDefaultBuilder(true, "chat", pitaya.Cluster, map[string]string{}, *conf)

	// 为客户端连接添加 WebSocket 接入器
	// 服务监听在 3250 端口，客户端可通过 ws://<host>:3250 建立连接
	builder.AddAcceptor(acceptor.NewWSAcceptor(":3250"))

	// 配置群组服务
	// MemoryGroupService 将群组数据保存在当前进程内存中
	// 常用于开发环境、单节点服务或临时房间管理
	builder.Groups = groups.NewMemoryGroupService(builder.Config.Groups.Memory)

	// 根据构建器配置创建最终的 Pitaya 应用实例
	app = builder.Build()

	// main 函数退出时关闭 Pitaya 服务
	// 释放网络连接、定时任务和内部资源
	defer app.Shutdown()

	// 创建名为 "room" 的群组。
	// 这个群组可用于管理房间内玩家、广播消息或维护成员关系
	err := app.GroupCreate(context.Background(), "room")
	if err != nil {
		// 群组创建失败时终止程序，避免服务在核心初始化不完整时继续运行
		panic(err)
	}

	// rewrite component and handler name
	// 创建房间组件实例
	// NewRoom(app) 通常会把 Pitaya 应用实例注入 Room，
	// 供组件调用 RPC、群组管理、会话管理等能力。
	room := NewRoom(app)

	// 注册 Room 组件，使其能够接收客户端请求和其他服务的 RPC 调用
	// 将组件对外名称设为 "room"。
	// 客户端路由通常会使用类似 room.xxx 的形式
	// 将处理方法名转换为小写。
	// 例如 Join、Leave、SendMessage 可映射为 join、leave、sendmessage
	app.Register(room,
		component.WithName("room"),
		component.WithNameFunc(strings.ToLower),
	)

	// 设置标准日志格式：
	// 日期、时间、源文件路径和行号。
	// Llongfile 有助于快速定位日志对应的代码位置。
	log.SetFlags(log.LstdFlags | log.Llongfile)

	// 注册静态文件服务。
	//
	// 请求路径：
	//   /web/index.html
	//
	// 实际文件路径：
	//   web/index.html
	//
	// StripPrefix 会去掉 URL 中的 /web/ 前缀，
	// FileServer 再从本地 web 目录读取并返回文件。
	http.Handle("/web/", http.StripPrefix("/web/", http.FileServer(http.Dir("web"))))

	// 异步启动 HTTP 静态资源服务，监听 3251 端口。
	// 浏览器可通过 http://<host>:3251/web/ 访问前端页面。
	// goroutine 让 HTTP 服务与 Pitaya 主服务并行运行。
	go http.ListenAndServe(":3251", nil)

	// 启动 Pitaya 应用。
	// 此调用进入服务主循环，开始接受 WebSocket 客户端连接、
	// 处理组件路由、执行 RPC 和维护集群通信。
	app.Start()
}

func configApp() *config.PitayaConfig {
	// 创建并加载 Pitaya 的默认配置。
	conf := config.NewDefaultPitayaConfig()
	// 设置本地进程可同时处理的 Handler 请求数量。
	conf.Buffer.Handler.LocalProcess = 15
	// 设置心跳检测间隔为 15 秒。
	conf.Heartbeat.Interval = time.Duration(15 * time.Second)

	// 设置每个 Agent 可缓存的消息数量为 32。
	conf.Buffer.Agent.Messages = 32
	// 关闭 Handler 消息压缩，减少压缩与解压的 CPU 开销。
	conf.Handler.Messages.Compression = false
	return conf
}
