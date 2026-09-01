// Copyright (c) nano Author and TFG Co. All Rights Reserved.
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

package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync/atomic"
	"time"

	"github.com/nats-io/nuid"
	"go.opentelemetry.io/otel/attribute"

	"github.com/topfreegames/pitaya/v3/pkg/acceptor"
	"github.com/topfreegames/pitaya/v3/pkg/pipeline"

	"github.com/topfreegames/pitaya/v3/pkg/agent"
	"github.com/topfreegames/pitaya/v3/pkg/cluster"
	"github.com/topfreegames/pitaya/v3/pkg/component"
	"github.com/topfreegames/pitaya/v3/pkg/conn/codec"
	"github.com/topfreegames/pitaya/v3/pkg/conn/message"
	"github.com/topfreegames/pitaya/v3/pkg/conn/packet"
	"github.com/topfreegames/pitaya/v3/pkg/constants"
	pcontext "github.com/topfreegames/pitaya/v3/pkg/context"
	"github.com/topfreegames/pitaya/v3/pkg/docgenerator"
	e "github.com/topfreegames/pitaya/v3/pkg/errors"
	"github.com/topfreegames/pitaya/v3/pkg/logger"
	"github.com/topfreegames/pitaya/v3/pkg/metrics"
	"github.com/topfreegames/pitaya/v3/pkg/route"
	"github.com/topfreegames/pitaya/v3/pkg/serialize"
	"github.com/topfreegames/pitaya/v3/pkg/session"
	"github.com/topfreegames/pitaya/v3/pkg/timer"
	"github.com/topfreegames/pitaya/v3/pkg/tracing"
)

var (
	handlerType = "handler"
)

// server *cluster.Server
// 集群节点的“身份证/节点描述对象”。
// 持有的“当前 Pitaya Server 身份/上下文对象”。
// 每一个实际运行的 Pitaya 进程，都可以对应一个 cluster.Server，
// 里面会描述这个节点的一些信息，例如：
// Server
// ├── ID
// ├── Type
// ├── Host
// ├── Port
// ├── Metadata
// └── ...
// 表明我自己这个服务器是谁

type (
	// HandlerService service
	HandlerService struct {
		baseService
		chLocalProcess   chan unhandledMessage // channel of messages that will be processed locally
		chRemoteProcess  chan unhandledMessage // channel of messages that will be processed remotely
		decoder          codec.PacketDecoder   // binary decoder
		remoteService    *RemoteService
		serializer       serialize.Serializer          // message serializer
		server           *cluster.Server               // server obj
		services         map[string]*component.Service // all registered service
		metricsReporters []metrics.Reporter
		agentFactory     agent.AgentFactory
		handlerPool      *HandlerPool
		handlers         map[string]*component.Handler // all handler method
		// dispatch worker pool counters (atomic): numDispatch is the number of running
		// Dispatch goroutines, busyDispatch how many are currently processing a message
		numDispatch  int32
		busyDispatch int32
	}

	unhandledMessage struct {
		ctx   context.Context
		agent agent.Agent
		route *route.Route
		msg   *message.Message
	}
)

// NewHandlerService creates and returns a new handler service
func NewHandlerService(
	packetDecoder codec.PacketDecoder,
	serializer serialize.Serializer,
	localProcessBufferSize int,
	remoteProcessBufferSize int,
	server *cluster.Server,
	remoteService *RemoteService,
	agentFactory agent.AgentFactory,
	metricsReporters []metrics.Reporter,
	handlerHooks *pipeline.HandlerHooks,
	handlerPool *HandlerPool,
) *HandlerService {
	h := &HandlerService{
		services:         make(map[string]*component.Service),
		chLocalProcess:   make(chan unhandledMessage, localProcessBufferSize),
		chRemoteProcess:  make(chan unhandledMessage, remoteProcessBufferSize),
		decoder:          packetDecoder,
		serializer:       serializer,
		server:           server,
		remoteService:    remoteService,
		agentFactory:     agentFactory,
		metricsReporters: metricsReporters,
		handlerPool:      handlerPool,
		handlers:         make(map[string]*component.Handler),
	}

	h.handlerHooks = handlerHooks

	return h
}

// Dispatch message to corresponding logic handler
// 把消息分法到对应的 逻辑 handler
// Dispatch 负责从两个消息队列中取消息，
// 然后把消息分发给对应的业务逻辑。
// thread 表示当前是第几个 Dispatch worker。
// 这里主要用于 worker 数量/监控统计。
func (h *HandlerService) Dispatch(thread int) {
	// TODO: This timer is being stopped multiple times,
	// it probably doesn't need to be stopped here
	// Dispatch goroutine 退出时停止全局定时器。
	//
	// TODO：作者自己也觉得这里有问题：
	// GlobalTicker 可能被多个 Dispatch goroutine 重复 Stop。
	defer timer.GlobalTicker.Stop()

	// register this goroutine as part of the dispatch worker pool so its utilization
	// can be reported (see ReportMetrics)
	// 记录当前 Dispatch worker 已经启动。
	// numDispatch 表示：
	// HandlerService 当前一共有多少个 Dispatch worker。
	atomic.AddInt32(&h.numDispatch, 1)

	for {
		// Calls to remote servers block calls to local server
		select {
		case lm := <-h.chLocalProcess:
			metrics.ReportMessageProcessDelayFromCtx(lm.ctx, h.metricsReporters, "local")
			atomic.AddInt32(&h.busyDispatch, 1)
			h.localProcess(lm.ctx, lm.agent, lm.route, lm.msg)
			atomic.AddInt32(&h.busyDispatch, -1)

		case rm := <-h.chRemoteProcess:
			metrics.ReportMessageProcessDelayFromCtx(rm.ctx, h.metricsReporters, "remote")
			atomic.AddInt32(&h.busyDispatch, 1)
			h.remoteService.remoteProcess(rm.ctx, nil, rm.agent, rm.route, rm.msg)
			atomic.AddInt32(&h.busyDispatch, -1)

		case <-timer.GlobalTicker.C: // execute cron task
			timer.Cron()

		case t := <-timer.Manager.ChCreatedTimer: // new Timers
			timer.AddTimer(t)

		case id := <-timer.Manager.ChClosingTimer: // closing Timers
			timer.RemoveTimer(id)
		}
	}
}

// ReportMetrics reports the fill level of the handler dispatch channels and the
// utilization of the dispatch worker pool. It does a single reporting pass and is
// meant to be called periodically (see App.periodicMetrics).
func (h *HandlerService) ReportMetrics() {
	if len(h.metricsReporters) == 0 {
		return
	}
	metrics.ReportChannelCapacity(h.metricsReporters, "handler_chlocalprocess", cap(h.chLocalProcess)-len(h.chLocalProcess))
	metrics.ReportChannelCapacity(h.metricsReporters, "handler_chremoteprocess", cap(h.chRemoteProcess)-len(h.chRemoteProcess))
	metrics.ReportWorkerPoolUsage(
		h.metricsReporters,
		"handler_dispatch",
		int(atomic.LoadInt32(&h.busyDispatch)),
		int(atomic.LoadInt32(&h.numDispatch)),
	)
}

// Register registers components
func (h *HandlerService) Register(comp component.Component, opts []component.Option) error {

	s := component.NewService(comp, opts)

	if _, ok := h.services[s.Name]; ok {
		return fmt.Errorf("handler: service already defined: %s", s.Name)
	}

	if err := s.ExtractHandler(); err != nil {
		return err
	}

	// register all handlers
	h.services[s.Name] = s
	for name, handler := range s.Handlers {
		h.handlerPool.Register(s.Name, name, handler)
	}
	return nil
}

// Handle handles messages from a conn
// HandlerService 为一个连接创建 Agent，并启动 Agent 的后台协程
// 当前协程负责持续从连接中读取消息、解码数据包，并将数据包交给对应的处理流程
// 当连接关闭或读取、解码、处理过程中发生错误时，结束当前 Session 的生命周期
func (h *HandlerService) Handle(conn acceptor.PlayerConn) {
	// create a client agent and startup write goroutine
	// 为当前连接创建 Agent。
	// Agent 是连接在 Pitaya 中对应的运行实体，负责维护 Session 以及连接相关的读写、心跳等状态
	a := h.agentFactory.CreateAgent(conn)

	// startup agent goroutine
	// 启动 Agent 的后台协程。
	// Agent.Handle() 会负责 Agent 自身的运行逻辑，例如写消息、心跳以及等待 Agent 退出
	go a.Handle()

	logger.Log.Debugf("New session established: %s", a.String())

	// guarantee agent related resource is destroyed
	// Handle() 退出时关闭 Session，释放当前连接对应的资源
	defer func() {
		a.GetSession().Close()
		logger.Log.Debugf("Session read goroutine exit, SessionID=%d, UID=%s", a.GetSession().ID(), a.GetSession().UID())
	}()

	for {

		// 从当前连接中读取下一条完整消息。
		// msg为字节流
		msg, err := conn.GetNextMessage()

		if err != nil {
			// Check if this is an expected error due to connection being closed
			if errors.Is(err, net.ErrClosed) {
				logger.Log.Debugf("Connection no longer available while reading next available message: %s", err.Error())
			} else if err == constants.ErrConnectionClosed {
				logger.Log.Debugf("Connection no longer available while reading next available message: %s", err.Error())
			} else {
				// Differentiate errors for valid sessions, to avoid noise from load balancer healthchecks and other internet noise
				if a.GetStatus() != constants.StatusStart {
					logger.Log.Errorf("Error reading next available message for UID: %s, Build: %s, error: %s", a.GetSession().UID(), "a.GetSession().GetHandshakeData().Sys.BuildNumber", err.Error())
				} else {
					logger.Log.Debugf("Error reading next available message on initial connection: %s", err.Error())
				}
			}

			return
		}

		packets, err := h.decoder.Decode(msg)
		if err != nil {
			logger.Log.Errorf("Failed to decode message: %s", err.Error())
			return
		}

		if len(packets) < 1 {
			logger.Log.Warnf("Read no packets, data: %v", msg)
			continue
		}

		// process all packet
		for i := range packets {
			if err := h.processPacket(a, packets[i]); err != nil {
				logger.Log.Errorf("Failed to process packet: %s", err.Error())
				return
			}
		}
	}
}

// processPacket processes a packet received from a client
// It handles handshake, handshake ack, data and heartbeat packets
// 我想 是不是应该把他们分开啊  我特别讨厌放在一起
func (h *HandlerService) processPacket(a agent.Agent, p *packet.Packet) error {
	switch p.Type {
	case packet.Handshake:
		logger.Log.Debug("Received handshake packet")

		// Parse the json sent with the handshake by the client
		handshakeData := &session.HandshakeData{}
		if err := json.Unmarshal(p.Data, handshakeData); err != nil {
			defer a.Close()
			logger.Log.Errorf("Failed to unmarshal handshake data: %s", err.Error())
			if serr := a.SendHandshakeErrorResponse(); serr != nil {
				logger.Log.Errorf("Error sending handshake error response: %s", err.Error())
				return err
			}

			return fmt.Errorf("invalid handshake data. Id=%d", a.GetSession().ID())
		}

		if err := a.GetSession().ValidateHandshake(handshakeData); err != nil {
			defer a.Close()
			logger.Log.Errorf("Handshake validation failed: %s", err.Error())
			if serr := a.SendHandshakeErrorResponse(); serr != nil {
				logger.Log.Errorf("Error sending handshake error response: %s", err.Error())
				return err
			}

			return fmt.Errorf("handshake validation failed: %w. SessionId=%d", err, a.GetSession().ID())
		}

		if err := a.SendHandshakeResponse(); err != nil {
			logger.Log.Errorf("Error sending handshake response: %s", err.Error())
			return err
		}
		logger.Log.Debugf("Session handshake Id=%d, Remote=%s", a.GetSession().ID(), a.RemoteAddr())

		a.GetSession().SetHandshakeData(handshakeData)
		a.SetStatus(constants.StatusHandshake)
		err := a.GetSession().Set(constants.IPVersionKey, a.IPVersion())
		if err != nil {
			logger.Log.Warnf("failed to save ip version on session: %q\n", err)
		}

		logger.Log.Debug("Successfully saved handshake data")

	case packet.HandshakeAck:
		a.SetStatus(constants.StatusWorking)
		logger.Log.Debugf("Receive handshake ACK Id=%d, Remote=%s", a.GetSession().ID(), a.RemoteAddr())

	case packet.Data:
		if a.GetStatus() < constants.StatusWorking {
			return fmt.Errorf("receive data on socket which is not yet ACK, session will be closed immediately, remote=%s",
				a.RemoteAddr().String())
		}

		msg, err := message.Decode(p.Data)
		if err != nil {
			return err
		}
		h.processMessage(a, msg)

	case packet.Heartbeat:
		// expected
	}

	a.SetLastAt()
	return nil
}

func (h *HandlerService) processMessage(a agent.Agent, msg *message.Message) {
	requestID := nuid.New().Next()
	ctx := pcontext.AddToPropagateCtx(context.Background(), constants.StartTimeKey, time.Now().UnixNano())
	ctx = pcontext.AddToPropagateCtx(ctx, constants.RouteKey, msg.Route)
	ctx = pcontext.AddToPropagateCtx(ctx, constants.RequestIDKey, requestID)

	attributes := []attribute.KeyValue{
		attribute.String("local.id", h.server.ID),
		attribute.String("span.kind", "server"),
		attribute.String("msg.type", strings.ToLower(msg.Type.String())),
		attribute.String("user.id", a.GetSession().UID()),
		attribute.String("request.id", requestID),
	}
	ctx, _ = tracing.StartSpan(ctx, msg.Route, attributes...)
	ctx = context.WithValue(ctx, constants.SessionCtxKey, a.GetSession())

	r, err := route.Decode(msg.Route)
	if err != nil {
		logger.Log.Errorf("Failed to decode route: %s", err.Error())
		a.AnswerWithError(ctx, msg.ID, e.NewError(err, e.ErrBadRequestCode))
		return
	}

	if r.SvType == "" {
		r.SvType = h.server.Type
	}

	message := unhandledMessage{
		ctx:   ctx,
		agent: a,
		route: r,
		msg:   msg,
	}
	if r.SvType == h.server.Type {
		h.chLocalProcess <- message
	} else {
		if h.remoteService != nil {
			h.chRemoteProcess <- message
		} else {
			logger.Log.Warnf("request made to another server type but no remoteService running")
		}
	}
}

func (h *HandlerService) localProcess(ctx context.Context, a agent.Agent, route *route.Route, msg *message.Message) {
	var mid uint
	switch msg.Type {
	case message.Request:
		mid = msg.ID
	case message.Notify:
		mid = 0
	}

	ret, err := h.handlerPool.ProcessHandlerMessage(ctx, route, h.serializer, h.handlerHooks, a.GetSession(), msg.Data, msg.Type, false)
	if msg.Type != message.Notify {
		if err != nil {
			logger.Log.Errorf("Failed to process handler message: %s", err.Error())
			a.AnswerWithError(ctx, mid, err)
		} else {
			err := a.GetSession().ResponseMID(ctx, mid, ret)
			if err != nil {
				logger.Log.Errorf("Failed to process handler message: %s", err.Error())
				tracing.FinishSpan(ctx, err)
				metrics.ReportTimingFromCtx(ctx, h.metricsReporters, handlerType, err)
			}
		}
	} else {
		metrics.ReportTimingFromCtx(ctx, h.metricsReporters, handlerType, err)
		tracing.FinishSpan(ctx, err)
		if err != nil {
			logger.Log.Errorf("Failed to process notify message: %s", err.Error())
		}
	}
}

// DumpServices outputs all registered services
func (h *HandlerService) DumpServices() {
	handlers := h.handlerPool.GetHandlers()
	for name := range handlers {
		logger.Log.Infof("registered handler %s, isRawArg: %v", name, handlers[name].IsRawArg)
	}
}

// Docs returns documentation for handlers
func (h *HandlerService) Docs(getPtrNames bool) (map[string]interface{}, error) {
	if h == nil {
		return map[string]interface{}{}, nil
	}
	return docgenerator.HandlersDocs(h.server.Type, h.services, getPtrNames)
}
