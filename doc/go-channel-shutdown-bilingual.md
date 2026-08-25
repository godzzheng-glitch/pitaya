# Go Channel Shutdown Signals / Go Channel 退出信号

This guide explains the following Go code for beginners:

本文面向 Go 初学者，解释下面这段代码：

```go
dieChan := make(chan bool)

select {
case <-dieChan:
	return
}
```

## 1. The goal / 作用

A Go program can run several goroutines at the same time. A channel can carry a shutdown signal from one goroutine to the others. Each receiver exits after it receives that signal.

一个 Go 程序可以同时运行多个 goroutine（轻量级并发任务）。Channel 可以把“该退出了”的信号从一个 goroutine 传给其他 goroutine。接收方收到信号后执行退出逻辑。

Think of a channel as a pipe:

可以把 channel 想象成一根管道：

```text
Sender / 发送方  ── signal / 信号 ──>  Receiver / 接收方
```

## 2. `:=` creates a variable / `:=` 创建变量

```go
dieChan := make(chan bool)
```

`:=` declares a new variable and assigns its first value at the same time. The variable name here is `dieChan`.

`:=` 会声明一个新变量，并在同一行给它赋初始值。这里的变量名是 `dieChan`。

The long form is:

完整写法是：

```go
var dieChan chan bool
dieChan = make(chan bool)
```

## 3. `chan bool` means a bool channel / `chan bool` 表示 bool 类型的 channel

```go
chan bool
```

`chan` is the Go keyword for a channel type. `bool` defines the value type carried through the channel. Valid values are `true` and `false`.

`chan` 是 Go 中表示 channel 类型的关键字。`bool` 定义管道中传递的数据类型，合法值为 `true` 和 `false`。

Examples of other channel types:

其他常见 channel 类型：

```go
chan int       // carries integers / 传递整数
chan string    // carries strings / 传递字符串
chan User      // carries User values / 传递 User 值
chan struct{}  // carries a signal with no data / 传递无数据的信号
```

## 4. `make` builds a usable channel / `make` 创建可用的 channel

```go
make(chan bool)
```

`make` is a built-in Go function. It creates and initializes three runtime-managed data structures: slices, maps, and channels. The resulting channel is ready for sending and receiving.

`make` 是 Go 的内置函数。它会创建并初始化三类由运行时管理的数据结构：slice、map 和 channel。创建后的 channel 可以直接用于发送和接收。

```go
users := make([]string, 0)       // slice / 切片
scores := make(map[string]int)   // map / 字典
dieChan := make(chan bool)       // channel / 通道
```

## 5. `<-` sends and receives / `<-` 用于发送和接收

The arrow points toward the channel when sending:

发送时，箭头指向 channel：

```go
dieChan <- true
// Send true into dieChan.
// 将 true 发送到 dieChan。
```

The arrow points away from the channel when receiving:

接收时，箭头从 channel 指向外部：

```go
value := <-dieChan
// Receive one value from dieChan and store it in value.
// 从 dieChan 接收一个值，并存入 value。
```

For a shutdown signal, code often only cares that a signal arrived:

退出信号场景通常只关心“信号已到达”：

```go
<-dieChan
// Wait for one signal and ignore its value.
// 等待一个信号，并忽略它携带的值。
```

## 6. `select` waits for channel events / `select` 等待多个 channel 事件

```go
select {
case <-dieChan:
	return
case job := <-jobs:
	handle(job)
}
```

`select` waits until one of its `case` channel operations can proceed. The ready case runs first. In this example, a shutdown signal runs `return`, while an incoming job runs `handle(job)`.

`select` 会等待其中某个 `case` 的 channel 操作能够执行。哪个 case 先就绪，就执行哪个。在这个例子中，收到退出信号会执行 `return`，收到任务会执行 `handle(job)`。

## 7. Sending versus closing / 发送信号与关闭 channel

Sending one value wakes one receiver:

发送一个值会唤醒一个接收方：

```go
dieChan <- true
```

Closing a channel wakes every goroutine currently waiting to receive from it. It also makes future receives return immediately.

关闭 channel 会唤醒当前所有等待接收的 goroutine。之后的接收操作也会立即返回。

```go
close(dieChan)
```

Therefore, a shutdown broadcast usually uses a channel with no payload:

因此，向多个组件广播退出信号时，通常使用不携带数据的 channel：

```go
dieChan := make(chan struct{})

// Many goroutines listen here.
// 多个 goroutine 在这里监听。
case <-dieChan:
	return

// One owner sends the broadcast.
// 由一个负责方发出广播。
close(dieChan)
```

`struct{}` occupies zero bytes and clearly communicates that the channel carries an event rather than application data.

`struct{}` 占用零字节，表达的含义也很明确：这个 channel 传递的是事件，而非业务数据。

## 8. Complete example / 完整示例

```go
package main

import (
	"fmt"
	"sync"
	"time"
)

func worker(id int, dieChan <-chan struct{}, wg *sync.WaitGroup) {
	defer wg.Done()

	for {
		select {
		case <-dieChan:
			fmt.Printf("worker %d exits\\n", id)
			return
		case <-time.After(300 * time.Millisecond):
			fmt.Printf("worker %d is working\\n", id)
		}
	}
}

func main() {
	dieChan := make(chan struct{})
	var wg sync.WaitGroup

	for id := 1; id <= 2; id++ {
		wg.Add(1)
		go worker(id, dieChan, &wg)
	}

	time.Sleep(time.Second)
	close(dieChan)
	wg.Wait()
	fmt.Println("all workers exited")
}
```

`dieChan <-chan struct{}` in the function parameter means the worker has receive-only access. `main` keeps ownership of `close(dieChan)`.

函数参数中的 `dieChan <-chan struct{}` 表示 worker 只拥有接收权限。`main` 保留 `close(dieChan)` 的关闭职责。

## 9. Recommended pattern / 推荐写法

Use `chan struct{}` with `close` for a simple in-process broadcast shutdown signal. Use `context.WithCancel` when the shutdown signal must travel through a call chain, carry deadlines, or integrate with APIs that accept `context.Context`.

简单的进程内广播退出信号使用 `chan struct{}` 配合 `close`。退出信号需要穿过多层函数调用、需要超时控制、或要对接接受 `context.Context` 的 API 时，使用 `context.WithCancel`。

```go
dieChan := make(chan struct{})
// Start shutdown / 开始退出
close(dieChan)
```
