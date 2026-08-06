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

package message

import (
	"encoding/binary"

	"github.com/topfreegames/pitaya/v3/pkg/util/compression"
)

// 本文件实现 Pitaya/Pomelo 协议中的 Message 层。
//
// 完整的网络数据分为两层：
//
//   Package = [1 字节包类型][3 字节长度][Message 字节]
//   Message = [flag][可选 id][可选 route][业务数据]
//
// 本文件只负责第二层 Message。编码完成的 []byte 会交给
// pkg/conn/codec/PomeloPacketEncoder，后者再补上 Package 头并写入连接。
//
// Go 中 []byte 是“切片”：它保存底层字节数组的位置、当前长度和容量。
// make([]byte, 0) 创建一个长度为 0 的切片；append 依次写入字段，容量不足时
// Go 运行时会分配更大的底层数组并复制已有内容。

// Encoder 定义 Message 层对外提供的能力。
type Encoder interface {
	// IsCompressionEnabled 返回业务数据压缩开关。
	IsCompressionEnabled() bool

	// Encode 将 Message 编码为 Message 层二进制数据。
	Encode(message *Message) ([]byte, error)
}

// MessagesEncoder 保存 Message 编码配置。
type MessagesEncoder struct {
	// DataCompression 为 true 时，Encode 会尝试压缩 message.Data。
	// 编码器只在压缩结果更小时使用压缩结果。
	DataCompression bool
}

// NewMessagesEncoder 创建一个 Message 编码器。
func NewMessagesEncoder(dataCompression bool) *MessagesEncoder {
	me := &MessagesEncoder{dataCompression}
	return me
}

// IsCompressionEnabled 返回当前编码器是否启用了业务数据压缩。
func (me *MessagesEncoder) IsCompressionEnabled() bool {
	return me.DataCompression
}

// Encode 将一个 Message 按 Pomelo Message 协议编码为 []byte。
//
// flag 的第 2~4 位保存消息类型，其他位保存路由压缩、业务数据压缩和错误标记。
// 各类型字段结构如下：
//
//   request  = [flag][变长 id][route][data]
//   notify   = [flag][route][data]
//   response = [flag][变长 id][data]
//   push     = [flag][route][data]
//
// route 有两种编码：
//
//   压缩路由 = [2 字节 route code]
//   字符串路由 = [1 字节 route length][route UTF-8 bytes]
//
// 返回的 []byte 是独立的 Message 数据。调用方随后会使用 PacketEncoder
// 创建完整 Package 数据；当前 Pitaya 实现因此会为 Message 和 Package
// 各生成一段输出字节切片。
//
// 协议说明：
// https://github.com/topfreegames/pitaya/v3/blob/master/docs/communication_protocol.md
func (me *MessagesEncoder) Encode(message *Message) ([]byte, error) {
	// 先验证消息类型，避免生成接收端无法识别的 flag。
	if invalidType(message.Type) {
		return nil, ErrWrongMessageType
	}

	// buf 是正在构建的 Message 字节数据。
	// 初始长度和容量均为 0；每次 append 将新字段追加到末尾。
	buf := make([]byte, 0)

	// flag 的消息类型位从第 1 位开始，因此左移 1 位。
	flag := byte(message.Type) << 1

	// routes 保存“路由字符串 -> 两字节路由码”的共享字典。
	// 编码期间使用读锁，保证并发更新路由字典时的数据一致性。
	routesCodesMutex.RLock()
	code, compressed := routes[message.Route]
	routesCodesMutex.RUnlock()

	// 路由字典命中时，在 flag 中记录“压缩路由”标志。
	if compressed {
		flag |= msgRouteCompressMask
	}

	// 业务处理结果表示错误时，在 flag 中写入错误标志。
	if message.Err {
		flag |= errorMask
	}

	// flag 始终位于 Message 的第一个字节。
	buf = append(buf, flag)

	// request 和 response 通过 id 建立一一对应关系。
	if message.Type == Request || message.Type == Response {
		n := message.ID
		// id 使用变长整数编码：每个字节存储 7 位数据；最高位为 1
		// 表示后面还有字节，最高位为 0 表示当前字节是最后一个字节。
		// 例如 0~127 占 1 字节，128~16383 占 2 字节。
		for {
			b := byte(n % 128)
			n >>= 7
			if n != 0 {
				// 加 128 等价于设置最高位为 1。
				buf = append(buf, b+128)
			} else {
				// 最后一个字节保持最高位为 0。
				buf = append(buf, b)
				break
			}
		}
	}

	// request、notify、push 都携带 route；response 通过 id 找回原请求路由。
	if routable(message.Type) {
		if compressed {
			// 压缩路由固定占用 2 字节，采用大端序写入。
			buf = append(buf, byte((code>>8)&0xFF))
			buf = append(buf, byte(code&0xFF))
		} else {
			// 原始路由先写 1 字节长度，再写完整路由字符串字节。
			// 协议长度字段为 1 字节，路由长度应保持在 255 字节以内。
			buf = append(buf, byte(len(message.Route)))
			buf = append(buf, []byte(message.Route)...)
		}
	}

	// 数据压缩只处理业务 data，flag、id 和 route 始终保持原始协议格式。
	if me.DataCompression {
		d, err := compression.DeflateData(message.Data)
		if err != nil {
			return nil, err
		}

		// 较小的压缩结果才具有网络传输价值。
		if len(d) < len(message.Data) {
			// 此处更新 message.Data，使后续追加步骤直接使用压缩后的数据。
			message.Data = d
			// buf[0] 是 flag；设置 gzipMask 告诉接收端执行解压。
			buf[0] |= gzipMask
		}
	}

	// 业务数据始终位于 Message 尾部。
	buf = append(buf, message.Data...)
	return buf, nil
}

// Decode 调用通用解码函数，将 Message 二进制数据还原为 Message 对象。
func (me *MessagesEncoder) Decode(data []byte) (*Message, error) {
	return Decode(data)
}

// Decode 将 Message 二进制数据按 [flag][id][route][data] 顺序拆解。
// data 通常来自 Package 的 body，即 Package 头已经由上层解码器处理完毕。
func Decode(data []byte) (*Message, error) {
	// 每个 Message 至少包含 1 字节 flag。
	if len(data) < msgHeadLength {
		return nil, ErrInvalidMessage
	}

	// New 创建协议 Message；offset 指向下一个待解析字段。
	m := New()
	flag := data[0]
	offset := 1

	// 取出 flag 中第 2~4 位保存的消息类型。
	m.Type = Type((flag >> 1) & msgTypeMask)

	if invalidType(m.Type) {
		return nil, ErrWrongMessageType
	}

	// request 和 response 含有变长 id，解码过程与 Encode 中的编码方向相反。
	if m.Type == Request || m.Type == Response {
		id := uint(0)
		// 每个字节低 7 位为有效数据；第一个字节保存最低 7 位。
		// uint 的位数决定可表示的最大 id。
		for i := offset; i < len(data); i++ {
			b := data[i]
			id += uint(b&0x7F) << uint(7*(i-offset))
			if b < 128 {
				// 最高位为 0 表示变长 id 到此结束。
				offset = i + 1
				break
			}
		}
		m.ID = id
	}

	// 从 flag 还原错误标志。
	m.Err = flag&errorMask == errorMask

	size := len(data)
	// 带路由的消息继续解析 route。
	if routable(m.Type) {
		if flag&msgRouteCompressMask == 1 {
			// 压缩路由至少需要 2 个字节路由码。
			if offset > size || (offset+2) > size {
				return nil, ErrInvalidMessage
			}

			m.compressed = true
			// 编码侧按大端序写入路由码，解码侧使用相同字节序恢复。
			code := binary.BigEndian.Uint16(data[offset:(offset + 2)])
			routesCodesMutex.RLock()
			route, ok := codes[code]
			routesCodesMutex.RUnlock()
			if !ok {
				return nil, ErrRouteInfoNotFound
			}
			m.Route = route
			offset += 2
		} else {
			m.compressed = false
			// 原始路由首字节保存字符串字节长度。
			rl := data[offset]
			offset++

			// 校验 route 数据没有超出当前 Message 边界。
			if offset > size || (offset+int(rl)) > size {
				return nil, ErrInvalidMessage
			}

			// 将路由字节转换为 Go string。
			m.Route = string(data[offset:(offset + int(rl))])
			offset += int(rl)
		}
	}

	// route 解析结束后，offset 应位于业务数据起始位置或刚好到达末尾。
	if offset > size {
		return nil, ErrInvalidMessage
	}

	// 业务数据直接引用输入 data 的剩余区间；当前分支不创建额外副本。
	m.Data = data[offset:]
	var err error

	// gzip 标志存在时，InflateData 创建解压后的业务数据切片。
	if flag&gzipMask == gzipMask {
		m.Data, err = compression.InflateData(m.Data)
		if err != nil {
			return nil, err
		}
	}
	return m, nil
}
