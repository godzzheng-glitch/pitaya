// Copyright (c) TFG Co. All Rights Reserved.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package cluster

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/topfreegames/pitaya/v3/pkg/config"
	"github.com/topfreegames/pitaya/v3/pkg/conn/message"
	"github.com/topfreegames/pitaya/v3/pkg/constants"
	pcontext "github.com/topfreegames/pitaya/v3/pkg/context"
	pitErrors "github.com/topfreegames/pitaya/v3/pkg/errors"
	"github.com/topfreegames/pitaya/v3/pkg/interfaces"
	"github.com/topfreegames/pitaya/v3/pkg/logger"
	"github.com/topfreegames/pitaya/v3/pkg/metrics"
	"github.com/topfreegames/pitaya/v3/pkg/protos"
	"github.com/topfreegames/pitaya/v3/pkg/route"
	"github.com/topfreegames/pitaya/v3/pkg/session"
	"github.com/topfreegames/pitaya/v3/pkg/tracing"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
)

// GRPCClient rpc client struct
type GRPCClient struct {
	bindingStorage   interfaces.BindingStorage
	clientMap        sync.Map
	dialTimeout      time.Duration
	infoRetriever    InfoRetriever
	lazy             bool
	metricsReporters []metrics.Reporter
	reqTimeout       time.Duration
	server           *Server
}

// NewGRPCClient returns a new instance of GRPCClient
func NewGRPCClient(
	config config.GRPCClientConfig,
	server *Server,
	metricsReporters []metrics.Reporter,
	bindingStorage interfaces.BindingStorage,
	infoRetriever InfoRetriever,
) (*GRPCClient, error) {
	gs := &GRPCClient{
		bindingStorage:   bindingStorage,
		infoRetriever:    infoRetriever,
		metricsReporters: metricsReporters,
		server:           server,
	}

	gs.dialTimeout = config.DialTimeout
	gs.lazy = config.LazyConnection
	gs.reqTimeout = config.RequestTimeout

	return gs, nil
}

type grpcClient struct {
	address   string
	cli       protos.PitayaClient
	conn      *grpc.ClientConn
	connected bool
	lock      sync.Mutex
}

// Init inits grpc rpc client
func (gs *GRPCClient) Init() error {
	return nil
}

// Call makes a RPC Call
func (gs *GRPCClient) Call(
	ctx context.Context,
	rpcType protos.RPCType,
	route *route.Route,
	session session.Session,
	msg *message.Message,
	server *Server,
) (*protos.Response, error) {
	c, ok := gs.clientMap.Load(server.ID)
	if !ok {
		return nil, constants.ErrNoConnectionToServer
	}

	parent, err := tracing.ExtractSpan(ctx)
	if err != nil {
		logger.Log.Warnf("[grpc client] failed to retrieve parent span: %s", err.Error())
	}
	ctx = trace.ContextWithRemoteSpanContext(ctx, parent)

	attributes := []attribute.KeyValue{
		attribute.String("span.kind", "client"),
		attribute.String("local.id", gs.server.ID),
		attribute.String("peer.serverType", server.Type),
		attribute.String("peer.id", server.ID),
	}
	ctx, _ = tracing.StartSpan(ctx, "GRPC RPC Call", attributes...)
	defer tracing.FinishSpan(ctx, err)

	if session != nil {
		requestID := uuid.New().String()
		requestInfo := ""
		if route != nil {
			requestInfo = route.Method
		}
		session.SetRequestInFlight(requestID, requestInfo, true)
		defer session.SetRequestInFlight(requestID, "", false)
	}

	ctx = pcontext.AddToPropagateCtx(ctx, constants.RequestTimeout, gs.reqTimeout.String())
	req, err := buildRequest(ctx, rpcType, route, session, msg, gs.server)
	if err != nil {
		return nil, err
	}

	ctxT, done := context.WithTimeout(ctx, gs.reqTimeout)
	defer done()

	if gs.metricsReporters != nil {
		startTime := time.Now()
		ctxT = pcontext.AddToPropagateCtx(ctxT, constants.StartTimeKey, startTime.UnixNano())
		ctxT = pcontext.AddToPropagateCtx(ctxT, constants.RouteKey, route.String())
		defer metrics.ReportTimingFromCtx(ctxT, gs.metricsReporters, "rpc", err)
	}

	res, err := c.(*grpcClient).call(ctxT, &req)
	if err != nil {
		return nil, err
	}
	if res.Error != nil {
		if res.Error.Code == "" {
			res.Error.Code = pitErrors.ErrUnknownCode
		}
		err = &pitErrors.Error{
			Code:     res.Error.Code,
			Message:  res.Error.Msg,
			Metadata: res.Error.Metadata,
		}
		return nil, err
	}
	return res, nil
}

// Send not implemented in grpc client
func (gs *GRPCClient) Send(uid string, d []byte) error {
	return constants.ErrNotImplemented
}

// BroadcastSessionBind sends the binding information to other servers that may be interested in this info
func (gs *GRPCClient) BroadcastSessionBind(uid string) error {
	if gs.bindingStorage == nil {
		return constants.ErrNoBindingStorageModule
	}
	fid, _ := gs.bindingStorage.GetUserFrontendID(uid, gs.server.Type)
	if fid != "" {
		if c, ok := gs.clientMap.Load(fid); ok {
			msg := &protos.BindMsg{
				Uid: uid,
				Fid: gs.server.ID,
			}
			ctxT, done := context.WithTimeout(context.Background(), gs.reqTimeout)
			defer done()
			err := c.(*grpcClient).sessionBindRemote(ctxT, msg)
			return err
		}
	}
	return nil
}

// SendKick sends a kick to an user
func (gs *GRPCClient) SendKick(userID string, serverType string, kick *protos.KickMsg) error {
	var svID string
	var err error

	if gs.bindingStorage == nil {
		return constants.ErrNoBindingStorageModule
	}

	svID, err = gs.bindingStorage.GetUserFrontendID(userID, serverType)
	if err != nil {
		return err
	}

	if c, ok := gs.clientMap.Load(svID); ok {
		ctxT, done := context.WithTimeout(context.Background(), gs.reqTimeout)
		defer done()
		err := c.(*grpcClient).sendKick(ctxT, kick)
		return err
	}
	return constants.ErrNoConnectionToServer
}

// SendPush sends a message to an user, if you dont know the serverID that the user is connected to, you need to set a BindingStorage when creating the client
// TODO: Jaeger?
func (gs *GRPCClient) SendPush(userID string, frontendSv *Server, push *protos.Push) error {
	var svID string
	var err error
	if frontendSv.ID != "" {
		svID = frontendSv.ID
	} else {
		if gs.bindingStorage == nil {
			return constants.ErrNoBindingStorageModule
		}
		svID, err = gs.bindingStorage.GetUserFrontendID(userID, frontendSv.Type)
		if err != nil {
			return err
		}
	}
	if c, ok := gs.clientMap.Load(svID); ok {
		ctxT, done := context.WithTimeout(context.Background(), gs.reqTimeout)
		defer done()
		err := c.(*grpcClient).pushToUser(ctxT, push)
		return err
	}
	return constants.ErrNoConnectionToServer
}

// AddServer is called when a new server is discovered
// 服务发现 etcd 发现一个新的 Server 后，GRPCClient 根据它的 Metadata 算出 host:port，
// 创建一个 gRPC 客户端，并把这个客户端保存到 clientMap。
func (gs *GRPCClient) AddServer(sv *Server) {

	// host：服务器的主机地址，例如：
	//   127.0.0.1
	//   192.168.1.100
	//
	// port：服务器的端口，例如：
	//   3000
	//   3250
	//
	// portKey：告诉后面应该从 Metadata 的哪个 key 读取端口。
	// 因为不同情况下可能使用不同的端口字段。
	var host, port, portKey string

	// ok 用来判断从 Metadata 中读取 port 时，key 是否存在。
	var ok bool

	// 根据 Server 的 Metadata 决定使用哪个 host，以及对应的 portKey。
	//
	// 例如可能得到：
	//   host    = "192.168.1.100"
	//   portKey = "grpcPort"
	//
	// getServerHost() 内部会根据 region 、 externalHost、
	// internalHost 等信息决定最终应该连接哪个地址。
	host, portKey = gs.getServerHost(sv)
	if host == "" {
		logger.Log.Errorf("[grpc client] server %s has no grpcHost specified in metadata", sv.ID)
		return
	}

	// 根据 getServerHost() 返回的 portKey，
	// 从 Server.Metadata 中取出对应的端口。
	//
	// 例如：
	// portKey = "grpcPort"
	//
	// 那么这里实际执行的就是：
	// port = sv.Metadata["grpcPort"]
	if port, ok = sv.Metadata[portKey]; !ok {
		logger.Log.Errorf("[grpc client] server %s has no %s specified in metadata", sv.ID, portKey)
		return
	}

	// 把 host 和 port 拼成 gRPC 连接地址。
	//
	// 例如：
	//   host = "192.168.1.100"
	//   port = "3000"
	//
	// 最终：
	//   address = "192.168.1.100:3000"
	address := fmt.Sprintf("%s:%s", host, port)
	// 创建一个 grpcClient。
	//
	// 此时只是把服务器地址保存下来：
	//   client.address = "192.168.1.100:3000"
	//
	// 真正建立 gRPC connection 是否立即发生，
	// 取决于下面的 lazy 配置。
	client := &grpcClient{address: address}

	// lazy == false：
	//   AddServer 时立即建立 gRPC 连接。
	//
	// lazy == true：
	//   AddServer 时只创建 grpcClient，
	//   等真正使用这个 client 时再建立连接。
	if !gs.lazy {
		if err := client.connect(); err != nil {
			logger.Log.Errorf("[grpc client] unable to connect to server %s at %s: %v", sv.ID, address, err)
		}
	}

	// 把这个 grpcClient 保存到 clientMap。
	//
	// key：
	//   sv.ID
	//
	// value：
	//   client
	//
	// 于是以后可以通过 Server ID 找到对应的 gRPC Client。
	//
	// 例如：
	//   clientMap["server-123"] -> grpcClient{
	//       address: "192.168.1.100:3000"
	//   }
	//
	// Store 是 sync.Map 的方法，因此这里也是并发安全的。
	gs.clientMap.Store(sv.ID, client)
	logger.Log.Debugf("[grpc client] added server %s at %s", sv.ID, address)
}

// RemoveServer is called when a server is removed
func (gs *GRPCClient) RemoveServer(sv *Server) {
	if c, ok := gs.clientMap.Load(sv.ID); ok {
		c.(*grpcClient).disconnect()
		gs.clientMap.Delete(sv.ID)
		logger.Log.Debugf("[grpc client] removed server %s", sv.ID)
	}
}

// AfterInit runs after initialization
func (gs *GRPCClient) AfterInit() {}

// BeforeShutdown runs before shutdown
func (gs *GRPCClient) BeforeShutdown() {}

// Shutdown stops grpc rpc server
func (gs *GRPCClient) Shutdown() error {
	return nil
}

//                 Server 有 Region？
//                      │
//           ┌──────────┴──────────┐
//          否                     是
//           │                      │
//    有 externalHost？       Client 和 Server
//      │        │             是同 Region？
//     是        否              │       │
//      │        │             是       否
//   external   internal        │       │
//                           internal   有 externalHost？
//                                       │       │
//                                      是       否
//                                       │       │
//                                    external  internal

func (gs *GRPCClient) getServerHost(sv *Server) (host, portKey string) {
	var (
		// 从服务元数据中读取区域及内外网地址；hasRegion/hasExternal 用于标记值是否有效。
		// 服务端所在的 Region
		serverRegion, hasRegion = sv.Metadata[constants.RegionKey]
		// 服务端提供给“跨 Region 访问”的外部 Host
		externalHost, hasExternal = sv.Metadata[constants.GRPCExternalHostKey]
		// 服务端提供给“同 Region / 内网访问”的 Host
		internalHost, _ = sv.Metadata[constants.GRPCHostKey]
	)

	// 空字符串视为未配置，避免仅存在键但没有可用地址时误走对应分支。
	hasRegion = hasRegion && serverRegion != ""
	hasExternal = hasExternal && externalHost != ""

	if !hasRegion {
		// 缺少区域时无法判断网络范围，优先使用显式配置的外网地址。
		if hasExternal {
			logger.Log.Warnf("[grpc client] server %s has no region specified in metadata, using external host", sv.ID)
			return externalHost, constants.GRPCExternalPortKey
		}

		// 区域和外网地址都缺失时，回退到内网地址。
		logger.Log.Warnf("[grpc client] server %s has no region nor external host specified in metadata, using internal host", sv.ID)
		return internalHost, constants.GRPCPortKey
	}

	// 同区域服务走内网；跨区域且未提供外网地址时也只能使用内网地址。
	if gs.infoRetriever.Region() == serverRegion || !hasExternal {
		logger.Log.Infof("[grpc client] server %s is in same region or external host not provided, using internal host", sv.ID)
		return internalHost, constants.GRPCPortKey
	}

	// 跨区域服务优先通过外网地址访问。
	logger.Log.Infof("[grpc client] server %s is in other region, using external host", sv.ID)
	return externalHost, constants.GRPCExternalPortKey
}

// connect 建立与远端 gRPC Server 的连接。
func (gc *grpcClient) connect() error {
	// 加锁，防止多个 goroutine 同时调用 connect，
	// 导致重复建立 gRPC 连接。
	gc.lock.Lock()
	defer gc.lock.Unlock()

	// 已经连接成功，直接返回。
	// 这样 connect() 具备幂等性：调用多次只建立一次连接。
	if gc.connected {
		return nil
	}

	// 创建一个 gRPC ClientConn。
	//
	// gc.address 一般类似：
	//   "127.0.0.1:3000"
	//
	// WithInsecure() 表示这个 gRPC 连接不使用 TLS 加密。
	conn, err := grpc.Dial(
		gc.address,
		grpc.WithInsecure(),
	)
	if err != nil {
		return err
	}

	// 基于 ClientConn 创建 Pitaya RPC Client。
	//
	// conn：
	//   底层 gRPC 连接
	//
	// c：
	//   上层真正调用 RPC 方法的客户端对象
	c := protos.NewPitayaClient(conn)

	// 保存 RPC Client。
	// 后面发送 RPC 请求时，通过 gc.cli 调用。
	gc.cli = c
	gc.conn = conn
	gc.connected = true
	return nil
}

func (gc *grpcClient) disconnect() {
	gc.lock.Lock()
	if gc.connected {
		gc.conn.Close()
		gc.connected = false
	}
	gc.lock.Unlock()
}

func (gc *grpcClient) pushToUser(ctx context.Context, push *protos.Push) error {
	if !gc.connected {
		if err := gc.connect(); err != nil {
			return err
		}
	}
	_, err := gc.cli.PushToUser(ctx, push)
	return err
}

func (gc *grpcClient) call(ctx context.Context, req *protos.Request) (*protos.Response, error) {
	if !gc.connected {
		if err := gc.connect(); err != nil {
			return nil, err
		}
	}
	return gc.cli.Call(ctx, req)
}

func (gc *grpcClient) sessionBindRemote(ctx context.Context, req *protos.BindMsg) error {
	if !gc.connected {
		if err := gc.connect(); err != nil {
			return err
		}
	}
	_, err := gc.cli.SessionBindRemote(ctx, req)
	return err
}

func (gc *grpcClient) sendKick(ctx context.Context, req *protos.KickMsg) error {
	if !gc.connected {
		if err := gc.connect(); err != nil {
			return err
		}
	}
	_, err := gc.cli.KickUser(ctx, req)
	return err
}
