// Copyright (c) nano Authors and TFG Co. All Rights Reserved.
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

package component

///////////////////////////////////////////////////////////////////////////////
// type ( ... ) 是类型声明分组，效果等同于
// type options struct { ... }
// type Option func(options *options)
// options 是一个结构体，接近 C++ 的 struct：
// struct options {
//     std::string name;
//     std::function<std::string(std::string)> nameFunc;
// };
// 字段含义：
// name     string              // 一个字符串
// nameFunc func(string) string // 一个函数：输入 string，输出 string
// Option 是一个“具名函数类型”：
///////////////////////////////////////////////////////////////////////////////

///////////////////////////什么叫类型声明分组////////////////////////////////////////////////////
// “类型声明分组”就是：用一对括号，把多条 type 声明写在一起。
// 单独写：
// type options struct {
// 	name string
// }

// type Option func(*options)
// 分组写：
// type (
// 	options struct {
// 		name string
// 	}

//	Option func(*options)
//
// )
// 两种写法生成完全相同的 options 和 Option 类型。
// /////////////////////////////////////////////////////////////////////////////////////

/////////////////////////////////////具名函数类型 /////////////////////////////////////////////////////
// 什么是具名函数类型
// 更准确的说法是：Option 是一个“定义出来的函数类型”。
// 英语可写作：
// defined function type：最准确，贴合 Go 规范
// named function type：常见口语说法，强调类型有名字 Option
//
// 代码：
// type Option func(*options)
// 含义分两层：
// func(*options) // 函数类型：接收 *options，无返回值
// Option         // 给这个函数类型定义的类型名
//
// Go 规范使用 defined type 表示通过 type 名字 底层类型 创建的类型：
// type UserID int
// type Handler func(string) error
// type Option func(*options)
//
// 这里的 Option 是类型名；它的底层类型（underlying type）是 func(*options)。
// “具名函数”对应英语 named function，通常指有函数名的函数：
//
// func WithName(name string) Option { ... } // named function
// “具名函数类型”对应 named function type，通常指像 Option 这样有类型名的函数类型。
// ////推荐记作：Option 是一个 defined function type。
//////////////////////////////////////////////////////////////////////////////////////////////////

type (
	// options 保存所有可配置项
	options struct {
		name     string              // component name
		nameFunc func(string) string // rename handler name
	}

	// Option used to customize handler
	// Option 是配置函数类型。
	// 每个 Option 接收 *options，并修改其中的配置字段。
	Option func(options *options)
)

// WithName used to rename component name
// 用于rename 一个 Component
// WithName 创建一个 Option，用于设置组件名称。
// 传入参数 name 传出一个函数 Option
// 这个函数 需要传入参数 options
func WithName(name string) Option {
	return func(opt *options) {
		opt.name = name
	}
}

// WithNameFunc override handler name by specific function
// such as: strings.ToUpper/strings.ToLower
// WithNameFunc 创建一个 Option，用于设置名称转换函数。
// 例如：WithNameFunc(strings.ToUpper)。
func WithNameFunc(fn func(string) string) Option {
	return func(opt *options) {
		opt.nameFunc = fn
	}
}
