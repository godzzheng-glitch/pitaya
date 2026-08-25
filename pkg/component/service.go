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

// package component 把业务组件转换为框架可注册、可路由、可反射调用的 Service。
// service 可以理解为 Component 转化而来的 真实运行态
// 里面会绑定对应的 handler 和remote

package component

import (
	"errors"
	"reflect"

	"github.com/topfreegames/pitaya/v3/pkg/conn/message"
	"github.com/topfreegames/pitaya/v3/pkg/constants"
)

type (
	//Handler represents a message.Message's handler's meta information.
	// Handler 保存一个可接收客户端消息的业务方法的运行时元数据。
	//
	// Service 在注册阶段通过反射扫描组件的导出方法。满足 Handler 签名约束的
	// 方法会转换为 Handler，并按路由方法名保存到 Service.Handlers 中。
	// 消息到达后，框架利用这些字段定位并调用具体业务方法。
	Handler struct {

		// Receiver 是业务组件实例的反射值，例如 reflect.ValueOf(&UserService{})。
		// 调用 Method 时，框架以它作为方法接收者。
		Receiver reflect.Value // receiver of method
		// Method 是反射层的方法描述，包含方法名、签名和调用入口。
		Method reflect.Method // method stub
		// Type 是 Method.Type 的缓存，即该方法完整的反射类型信息。
		Type reflect.Type // low-level type of method
		// IsRawArg 表示参数采用原始字节形式。为 true 时，框架直接传递 []byte；
		// 为 false 时，框架将请求体反序列化为业务方法声明的指针参数。
		IsRawArg bool // whether the data need to serialize
		// MessageType 限定该方法接收的消息类别，例如 Request 或 Notify。
		// 路由成功后，框架仍会利用该字段校验消息类别。
		MessageType message.Type // handler allowed message type (either request or notify)
	}

	//Remote represents remote's meta information.
	// Remote 保存一个可供其他服务器进程调用的业务方法的运行时元数据。
	// 它与 Handler 的核心区别在于调用来源：Handler 服务于入站客户端消息，
	// Remote 服务于服务器之间的 RPC 调用。
	Remote struct {
		// Receiver 是承载 Remote 方法的业务组件实例。
		Receiver reflect.Value // receiver of method
		// Method 是可远程调用方法的反射描述。
		Method reflect.Method // method stub
		// HasArgs 表示远程方法是否声明业务参数。
		// 框架据此决定是否将收到的数据反序列化后传入该方法。
		HasArgs bool // if remote has no args we won't try to serialize received data into arguments
		// Type 是远程方法的完整反射类型信息。
		Type reflect.Type // low-level type of method
	}

	// Service implements a specific service, some of it's methods will be
	// called when the correspond events is occurred.
	// service会实现一个特定的service ... 以及他的一些方法
	// 当一些event(我理解应该是消息吧) 发生的 时候被调用

	// Component 主要承载业务代码和生命周期回调；
	// Service 承载框架侧的元数据：
	// 服务名、业务实例、反射类型、可路由的 Handler、可 RPC 调用的 Remote
	// 以及命名配置。
	// 一个组件实例对应一个 Service 实例。
	Service struct {
		// Name 是服务名，参与路由。例如 Name 为 "User"、Handler 名为
		// "GetProfile" 时，框架可将请求路由到 User.GetProfile
		Name string // name of service

		// Type 是 comp 的动态类型，例如 *UserService。
		// ExtractHandler 和 ExtractRemote 通过它扫描组件的方法集合。
		Type reflect.Type // type of the receiver

		// Receiver 是 comp 的动态值，例如 &UserService{}。
		// 每个提取出来的 Handler 和 Remote 都会引用这个接收者。
		Receiver reflect.Value // receiver of methods for the service

		// Handlers 以路由方法名为键，保存当前服务接受客户端消息的方法。
		// 它由 ExtractHandler 填充。
		Handlers map[string]*Handler // registered methods
		// Remotes 以远程方法名为键，保存当前服务暴露给其他服务器的方法。
		// 它由 ExtractRemote 填充。
		Remotes map[string]*Remote // registered remote methods
		// Options 保存创建服务时应用的配置，例如显式服务名和方法名转换函数。
		Options options // options
	}
)

// NewService creates a new service
//
// NewService 将一个业务组件实例包装为框架运行时 Service。
//
// 此函数只完成实例和配置的初始化。Handler 与 Remote 的反射扫描分别由
// ExtractHandler 和 ExtractRemote 完成，注册流程通常会在后续调用它们。
func NewService(comp Component, opts []Option) *Service {

	// 同时保存类型和值：Type 用于扫描方法签名，Receiver 用于后续实际调用方法。
	s := &Service{
		Type:     reflect.TypeOf(comp),
		Receiver: reflect.ValueOf(comp),
	}

	// apply options
	// Option 是 func(*options) 形式的配置函数。
	// 按传入顺序执行，因此同一配置项重复出现时，后面的配置拥有最终值。
	for i := range opts {
		opt := opts[i]
		opt(&s.Options)
	}

	// 取option里的name 作为service的 name
	// 可见 option是个配置项 而且还需要先用 opt取apply一下这些配置项...
	// 道理我是懂的
	// WithName 提供显式名称时优先使用该名称。
	if name := s.Options.name; name != "" {
		s.Name = name
	} else {
		s.Name = reflect.Indirect(s.Receiver).Type().Name()
	}

	return s
}

// ExtractHandler extract the set of methods from the
// receiver value which satisfy the following conditions:
// - exported method of exported type
// - one or two arguments
// - the first argument is context.Context
// - the second argument (if it exists) is []byte or a pointer
// - zero or two outputs
// - the first output is [] or a pointer
// - the second output is an error

// ExtractHandler 从接收者值中提取满足 Handler 签名要求的方法：
// - 导出类型上的导出方法
// - 1 个或 2 个入参
// - 第一个入参是 context.Context
// - 第二个入参（如果存在）是 []byte 或指针
// - 0 个或 2 个返回值
// - 第一个返回值是 slice 或指针
// - 第二个返回值是 error
func (s *Service) ExtractHandler() error {
	typeName := reflect.Indirect(s.Receiver).Type().Name()
	if typeName == "" {
		return errors.New("no service name for type " + s.Type.String())
	}
	if !isExported(typeName) {
		return errors.New("type " + typeName + " is not exported")
	}

	// 扫描并安装符合条件的 handler 方法
	s.Handlers = suitableHandlerMethods(s.Type, s.Options.nameFunc)

	if len(s.Handlers) == 0 {
		str := ""
		// 顺手检查指针接收者，给出更明确的提示
		method := suitableHandlerMethods(reflect.PtrTo(s.Type), s.Options.nameFunc)
		if len(method) != 0 {
			str = "type " + s.Name + " has no exported methods of handler type (hint: pass a pointer to value of that type)"
		} else {
			str = "type " + s.Name + " has no exported methods of handler type"
		}
		return errors.New(str)
	}

	for i := range s.Handlers {
		s.Handlers[i].Receiver = s.Receiver
	}

	return nil
}

// ExtractRemote extract the set of methods from the
// receiver value which satisfy the following conditions:
// - exported method of exported type
// - two return values
// - the first return implements protobuf interface
// - the second return is an error
func (s *Service) ExtractRemote() error {
	typeName := reflect.Indirect(s.Receiver).Type().Name()
	if typeName == "" {
		return errors.New("no service name for type " + s.Type.String())
	}
	if !isExported(typeName) {
		return errors.New("type " + typeName + " is not exported")
	}

	// Install the methods
	s.Remotes = suitableRemoteMethods(s.Type, s.Options.nameFunc)

	if len(s.Remotes) == 0 {
		str := ""
		// To help the user, see if a pointer receiver would work.
		method := suitableRemoteMethods(reflect.PtrTo(s.Type), s.Options.nameFunc)
		if len(method) != 0 {
			str = "type " + s.Name + " has no exported methods of remote type (hint: pass a pointer to value of that type)"
		} else {
			str = "type " + s.Name + " has no exported methods of remote type"
		}
		return errors.New(str)
	}

	for i := range s.Remotes {
		s.Remotes[i].Receiver = s.Receiver
	}
	return nil
}

// ValidateMessageType validates a given message type against the handler's one
// and returns an error if it is a mismatch and a boolean indicating if the caller should
// exit in the presence of this error or not.
func (h *Handler) ValidateMessageType(msgType message.Type) (exitOnError bool, err error) {
	if h.MessageType != msgType {
		switch msgType {
		case message.Request:
			err = constants.ErrRequestOnNotify
			exitOnError = true

		case message.Notify:
			err = constants.ErrNotifyOnRequest
		}
	}
	return
}
