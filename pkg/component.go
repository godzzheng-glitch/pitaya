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

package pitaya

import (
	"github.com/topfreegames/pitaya/v3/pkg/component"
	"github.com/topfreegames/pitaya/v3/pkg/logger"
)

// regComp 表示一个等待启动阶段注册的组件
type regComp struct {
	// 组件实例
	// Component 接口要求实现 Init AfterInit BeforeShutdown Shutdown
	comp component.Component

	// 注册选项，例如：
	// component.WithName("room")
	// component.WithNameFunc(strings.ToLower)
	opts []component.Option
}

// Register register a component with options
// 给app 注册一个 Component 并且附带一系列参数
func (app *App) Register(c component.Component, options ...component.Option) {
	app.handlerComp = append(app.handlerComp, regComp{c, options})
}

// RegisterRemote register a remote component with options
func (app *App) RegisterRemote(c component.Component, options ...component.Option) {
	app.remoteComp = append(app.remoteComp, regComp{c, options})
}

// 启动所有组件 具体有哪些组件 后面再说
func (app *App) startupComponents() {
	// handler component initialize hooks
	for _, c := range app.handlerComp {
		c.comp.Init()
	}

	// handler component after initialize hooks
	for _, c := range app.handlerComp {
		c.comp.AfterInit()
	}

	// remote component initialize hooks
	for _, c := range app.remoteComp {
		c.comp.Init()
	}

	// remote component after initialize hooks
	for _, c := range app.remoteComp {
		c.comp.AfterInit()
	}

	// register all components
	// 将所有的handler 真正地注册到 service里去
	// 之前只是在handlerComp 的regcomp 里暂存
	for _, c := range app.handlerComp {
		if err := app.handlerService.Register(c.comp, c.opts); err != nil {
			logger.Log.Errorf("Failed to register handler: %s", err.Error())
		}
	}

	// register all remote components
	for _, c := range app.remoteComp {
		if app.remoteService == nil {
			logger.Log.Warn("registered a remote component but remoteService is not running! skipping...")
		} else {
			if err := app.remoteService.Register(c.comp, c.opts); err != nil {
				logger.Log.Errorf("Failed to register remote: %s", err.Error())
			}
		}
	}

	app.handlerService.DumpServices()
	if app.remoteService != nil {
		app.remoteService.DumpServices()
	}
}

func (app *App) shutdownComponents() {
	// reverse call `BeforeShutdown` hooks
	length := len(app.handlerComp)
	for i := length - 1; i >= 0; i-- {
		app.handlerComp[i].comp.BeforeShutdown()
	}

	// reverse call `Shutdown` hooks
	for i := length - 1; i >= 0; i-- {
		app.handlerComp[i].comp.Shutdown()
	}

	length = len(app.remoteComp)
	for i := length - 1; i >= 0; i-- {
		app.remoteComp[i].comp.BeforeShutdown()
	}

	// reverse call `Shutdown` hooks
	for i := length - 1; i >= 0; i-- {
		app.remoteComp[i].comp.Shutdown()
	}
}
