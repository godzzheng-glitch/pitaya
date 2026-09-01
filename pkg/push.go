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

package pitaya

import (
	"github.com/topfreegames/pitaya/v3/pkg/cluster"
	"github.com/topfreegames/pitaya/v3/pkg/constants"
	"github.com/topfreegames/pitaya/v3/pkg/logger"
	"github.com/topfreegames/pitaya/v3/pkg/protos"
	"github.com/topfreegames/pitaya/v3/pkg/util"
)

// SendPushToUsers sends a message to the given list of users.
//
// 调用方只需要提供路由、消息内容和用户 ID 列表；函数会根据用户所在
// 的前端服务器，自动选择本机 Session 推送或通过 RPC 转发到其他前端。
// 返回值中的用户 ID 表示推送失败的用户；全部成功时返回 (nil, nil)。

// SendPushToUsers 给一批用户发送推送消息。
// 核心逻辑：
// 1. 先把消息转换成 []byte。
// 2. 如果用户就在当前前端服务器上，直接通过 Session.Push() 发送。
// 3. 如果用户在其他前端服务器上，通过 RPC 转发。
// 4. 最后返回所有发送失败的 UID。

func (app *App) SendPushToUsers(route string, v interface{}, uids []string, frontendType string) ([]string, error) {
	// Session.Push 需要字节数据。SerializeOrRaw 会将普通对象交给序列化器，
	// 对已经是 []byte 等原始数据的输入则直接复用，避免重复编码。
	data, err := util.SerializeOrRaw(app.serializer, v)
	if err != nil {
		// 序列化失败时还没有向任何用户发送消息，因此整份 uids 都视为未推送。
		return uids, err
	}

	// 后端服务器本身没有用户 Session，必须知道目标前端类型才能通过 RPC
	// 找到正确的前端；前端服务器调用时可以省略该参数。
	if !app.server.Frontend && frontendType == "" {
		return uids, constants.ErrFrontendTypeNotSpecified
	}

	// 只记录实际推送失败的用户。循环结束后统一返回，调用方可以针对这些
	// 用户重试或记录业务错误。
	var notPushedUids []string

	logger.Log.Debugf("Type=PushToUsers Route=%s, Data=%+v, SvType=%s, #Users=%d", route, v, frontendType, len(uids))

	for _, uid := range uids {
		// 目标 Session 已经连接在当前服务器，并且当前服务器类型与目标
		// frontendType 一致时，直接本机推送，链路最短。
		if s := app.sessionPool.GetSessionByUID(uid); s != nil && app.server.Type == frontendType {
			if err := s.Push(route, data); err != nil {
				// Session 存在但发送失败，例如连接已断开；只标记当前用户，
				// 继续处理列表中的其他用户。
				notPushedUids = append(notPushedUids, uid)
				logger.Log.Errorf("Session push message error, ID=%d, UID=%s, Error=%s",
					s.ID(), s.UID(), err.Error())
			}
		} else if app.rpcClient != nil {
			// 当前服务器没有可用的本机 Session，或 Session 属于其他前端
			// 类型。通过 RPC 将完整消息转发给指定 frontendType 的服务器。
			push := &protos.Push{
				Route: route,
				Uid:   uid,
				Data:  data,
			}
			if err = app.rpcClient.SendPush(uid, &cluster.Server{Type: frontendType}, push); err != nil {
				// RPC 转发失败同样只影响当前用户，保留失败用户并继续循环。
				notPushedUids = append(notPushedUids, uid)
				logger.Log.Errorf("RPCClient send message error, UID=%s, SvType=%s, Error=%s", uid, frontendType, err.Error())
			}
		} else {
			// 没有本机可用 Session，也没有 RPC 客户端，当前用户没有任何
			// 可达的推送通道。
			notPushedUids = append(notPushedUids, uid)
		}
	}

	// 部分或全部用户失败时返回失败列表和统一错误；成功用户不会出现在
	// 列表中，调用方可以据此区分并处理失败对象。
	if len(notPushedUids) != 0 {
		return notPushedUids, constants.ErrPushingToUsers
	}

	// 所有用户都已成功推送。按照 Go 的常见约定，用 nil 表示没有失败项。
	return nil, nil
}
