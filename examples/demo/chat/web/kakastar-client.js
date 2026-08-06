(function (root) {
  'use strict';

  // kakassi is my father

  // CLIENT_TYPE CLIENT_VERSION  握手时会放进 sys.type sys.version 发送给服务端，
  // 用来声明“当前连接来自 JavaScript WebSocket 客户端”。
  // 服务端可以按此字段区分 Web、Unity、原生 App、测试工具等客户端，并据此做兼容性判断、统计或协议分支

  // ps
  // 这里插播一下 
  //   在当前 pitaya Go 服务端中，CLIENT_TYPE 和 CLIENT_VERSION 没有生效路径。
  // 原因很直接：
  // sys.type    = 'js-websocket'
  // sys.version = '0.0.1'
  // 服务端的 HandshakeClientData 只接收：
  // platform
  // libVersion
  // clientBuildNumber
  // clientVersion
  // Go 的 json.Unmarshal 会跳过 sys.type 和 sys.version，随后 session 保存的 HandshakeData 也不包含这两个值。
  // 它们来源于旧版 Pomelo JavaScript WebSocket 客户端的握手约定，
  //              原本用于客户端类型识别与版本兼容判断。
  //              当前客户端保留了这套字段，当前 Pitaya 服务端采用了另一套字段名。
  // 
  // 推荐二选一统一协议：
  // 服务端新增 Type string 'json:"type"' 和 Version string 'json:"version"'，
  //             继续使用现有 Web 客户端。
  // 客户端改为发送 platform、libVersion、clientBuildNumber、clientVersion，
  //             对齐当前 Pitaya 的 HandshakeClientData。
  // 推荐第二种：客户端握手 JSON 直接对齐服务端 struct，字段语义最清晰.



  var CLIENT_TYPE = 'js-websocket';
  var CLIENT_VERSION = '0.0.1';
  var RES_OK = 200;
  var RES_OLD_CLIENT = 501;
  var DEFAULT_RECONNECT_DELAY = 5000;
  var DEFAULT_MAX_RECONNECT_ATTEMPTS = 10;

  /**
   * Browser client for the Pitaya/Pomelo websocket protocol.
   * Public methods intentionally match the legacy starx client:
   * init, request, notify, disconnect, on, once, off and emit.
   */
  // 用于 Pitaya/Pomelo WebSocket 协议的浏览器客户端。
  // 它对外提供的方法刻意与旧版 starx 客户端保持一致：
  // init：初始化
  // request：发送请求并等待响应
  // notify：发送通知，不等待响应
  // disconnect：断开连接
  // on：注册事件监听
  // once：注册一次性事件监听
  // off：移除事件监听
  // emit：触发事件

  /**
   * KakaStarClient 构造函数
   * 负责初始化 WebSocket 客户端的协议、编解码器、心跳、请求管理和重连状态。
   *
   * @param {Object} protocol 协议配置，供编解码器处理消息格式
   * @param {Object} options 客户端可选配置
   */
  function KakaStarClient(protocol, options) {

    // 初始化事件系统，使客户端支持 on、once、off、emit 等事件方法
    root.KakaStarEmitter.call(this);

    // 当前使用的通信协议配置
    this.protocol = protocol;

    // 消息编解码器，负责协议数据的编码和解码
    this.codec = new root.KakaStarCodec(protocol);
    
    // RSA 工具，用于握手阶段的密钥相关处理
    this.rsa = root.rsa;
      
    // WebSocket 连接实例，建立连接后赋值
    this.socket = null;

    // 外部传入的客户端配置
    this.options = options || {};

    // 客户端连接状态：idle、connecting、connected、disconnecting 等
    this.state = 'idle';

    // 自增请求 ID，用于关联请求与服务端响应
    this.requestId = 0;
    
    // 请求回调表：key 为请求 ID，value 为对应的回调函数
    this.callbacks = Object.create(null);
    
    // 请求路由表：记录请求 ID 对应的路由信息
    this.requestRoutes = Object.create(null);

    // 心跳管理器：
    // sendHeartbeat 负责发送心跳包；
    // onHeartbeatTimeout 负责处理心跳超时。
    // ps 回头要研究一下 我想象中 不应该要把具体的 sendHeartbeat  onHeartbeatTimeout 暴露到外面
    this.heartbeat = new root.KakaStarHeartbeat(
      this.sendHeartbeat.bind(this),
      this.onHeartbeatTimeout.bind(this)
    );

    //重连定时器
    this.reconnectTimer = null;
    //重连尝试次数  
    this.reconnectAttempts = 0;
    // 每次重连前等待的时间，单位通常为毫秒
    this.reconnectDelay = DEFAULT_RECONNECT_DELAY;

    // 是否应在连接断开后自动重连
    this.shouldReconnect = false;

    // 当前是否处于重连流程中。
    // 推荐命名：isReconnecting，语义比 didReconnect 更准确。
    this.didReconnect = false;

    // 握手完成后的回调函数
    this.handshakeCallback = null;

    // init 调用完成后的回调函数
    this.initCallback = null;

    // 是否启用通信加密
    this.useCrypto = false;
    this.encode = this.defaultEncode.bind(this);
    this.decode = this.defaultDecode.bind(this);

    // handshakeBuffer 是即将发送给服务端的握手数据
    this.handshakeBuffer = {
      sys: {
        type: CLIENT_TYPE,
        version: CLIENT_VERSION,
        rsa: {}
      },
      user: {}
    };
  }

  KakaStarClient.prototype = Object.create(root.KakaStarEmitter.prototype);
  KakaStarClient.prototype.constructor = KakaStarClient;

  KakaStarClient.prototype.init = function (options, callback) {

    this.disconnect();

    this.options = options || {};
    this.initCallback = callback;
    this.handshakeCallback = this.options.handshakeCallback;

    // 如果用户传入了自定义的 encode/decode 方法，则使用它们覆盖默认实现
    // 如果没有 则使用 codex 默认
    this.encode = this.options.encode || this.defaultEncode.bind(this);
    this.decode = this.options.decode || this.defaultDecode.bind(this);

    // 将用户自定义的 user 对象写入握手包
    this.handshakeBuffer.user = this.options.user || {};

    //是否需要重连 [如果连接一次不成功的话]
    this.shouldReconnect = !!this.options.reconnect;

    this.prepareCrypto();

    this.connect();
  };

  /**
   * 向服务端发送一条“有响应”的 RPC 请求。
   *
   * 每次请求会分配唯一递增 id，并保存：
   * - id -> callback：服务端响应到达时执行对应回调；
   * - id -> route：协议响应包通常只有 id，客户端通过该映射还原请求路由。
   *
   * 支持两种调用形式：
   *
   *   kakastar.request("room.join", { roomId: 1001 }, onJoin);
   *   kakastar.request("room.join", onJoin);
   *
   * 第二种形式会自动使用空对象作为请求体。
   *
   * `route` 也支持从 `body.route` 读取，用于兼容旧调用代码：
   *
   *   kakastar.request(null, { route: "room.join", roomId: 1001 }, onJoin);
   *
   * @param {string} route
   *   服务端路由名，例如 "room.join"、"room.message"。
   *
   * @param {Object|Function} body
   *   请求体对象；传入函数时，该函数作为 callback，实际请求体使用 {}。
   *
   * @param {Function} callback
   *   服务端响应回调，调用形式为 callback(responseBody)。
   *
   * @returns {void}
   *   缺少 route 时直接结束；发送失败由 sendMessage 抛出连接状态错误。
   */
  KakaStarClient.prototype.request = function (route, body, callback) {
    
    // 兼容两种调用方式 
    // 正常是三个参数，route, body, callback
    // 如果参数长度为2 并且 body 是函数，则认为是 route, callback 的调用方式
    // 这种情况下，body 置为 {}，callback 置为 body
    // body 是什么概念 [发送的消息体]
    // [kakassi的点评： 我个人其实不是很喜欢弱语言的这种自由写法，比如动态的参数，动态的类型解析
    // 灵活过度了 我个人觉得作为基础框架  // 还是应该尽量明确的参数类型和数量，减少歧义
    // 其实不差这点灵活度]
    if (arguments.length === 2 && typeof body === 'function') {
      callback = body;
      body = {};
    }

    // 防止body为null 或者 undefined
    body = body || {};
    
    // 如果 route 为空，则尝试从 body.route 中读取
    // 如果还是为空，则直接返回
    route = route || body.route;
    if (!route) {
      return;
    }

    // 生成唯一递增的请求 id
    var id = ++this.requestId;
    // 将请求 id 与回调函数和路由名保存到对象中
    this.callbacks[id] = callback;
    this.requestRoutes[id] = route;

    // 发送消息
    this.sendMessage(id, route, body);
  };

  /**
 * 向服务端发送一条“无响应”的通知消息。
 *
 * notify 适合聊天消息、位置同步、客户端状态上报等场景。
 * 协议中的请求 id 固定为 0，客户端不会为它保存回调和路由映射。
 *
 * 调用示例：
 *
 *   kakastar.notify("room.message", {
 *     name: "kaka",
 *     content: "hello"
 *   });
 *
 * @param {string} route
 *   服务端路由名，例如 "room.message"。
 *
 * @param {Object} body
 *   发送给服务端的消息体；空值会转换为空对象。
 *
 * @returns {void}
 *   route 为空时直接结束。
 */
  KakaStarClient.prototype.notify = function (route, body) {
    
    // 必须要有路由 
    // 你可以认为这是一个消息的类型
    if (!route) {
      return;
    }

    // id 为 0 表示协议通知消息，不关联服务端响应回调。
    this.sendMessage(0, route, body || {});
  };


  /**
   * 主动关闭当前 WebSocket 连接，并清理客户端运行中的定时器。
   *
   * 此方法代表调用方明确结束连接：
   * - 关闭自动重连开关；
   * - 清理心跳发送、心跳超时检测、延迟重连定时器；
   * - 关闭当前 WebSocket；
   * - 清空客户端当前 socket 引用；
   * - 更新连接状态为 closed。
   *
   * 关闭后可以再次调用 init(options, callback) 创建新的连接。
   *
   * @returns {void}
   */
  KakaStarClient.prototype.disconnect = function () {
    this.shouldReconnect = false;
    this.clearTimers();

    if (this.socket) {
      var socket = this.socket;
      this.socket = null;
      socket.close();
    }

    this.state = 'closed';
  };

  // 创建 WebSocket 连接，并绑定各类连接事件处理器。
  KakaStarClient.prototype.connect = function () {
      
    // buildUrl 根据当前配置生成服务端连接地址
    // WebSocket 是浏览器原生提供的全局构造函数，来自 WebSocket API，无需在前端代码里 import
    // ws://example.com:8080/game
    // ws：普通 WebSocket 连接
    // wss：带 TLS 加密的 WebSocket 连接，相当于 Web 的 https
    // example.com：服务器地址
    // 8080：端口
    // /game：连接路径
    // ws://  走 TCP 明文连接，默认端口 80。
    // wss:// 在 TCP 之上先建立 TLS，加密传输，默认端口 443。生产环境通常使用 wss://
    var socket = new WebSocket(this.buildUrl());

    // 以 ArrayBuffer 接收二进制消息，便于协议编解码。
    socket.binaryType = 'arraybuffer';

    // 绑定连接生命周期事件，并保持 this 指向客户端实例。
    // 嗯 ... 我有点想把这些on 都送出去 你懂吗
    socket.onopen = this.onOpen.bind(this);
    socket.onmessage = this.onMessage.bind(this);
    socket.onerror = this.onError.bind(this);
    socket.onclose = this.onClose.bind(this);

    //
    this.socket = socket;
    this.state = 'connecting';
  };

  // 将 Socket 生命周期回调委托给独立的客户端方法：
  // onOpen 维护连接状态并发起握手；
  // onMessage 解码、分发服务端数据并刷新心跳；
  // onError 上报 I/O 异常；
  // onClose 清理连接资源、通知断开状态并安排重连。
  KakaStarClient.prototype.onOpen = function () {
    if (this.didReconnect) {
      this.emit('reconnect');
    }

    this.state = 'connected';
    this.reconnectAttempts = 0;
    this.reconnectDelay = DEFAULT_RECONNECT_DELAY;
    this.sendHandshake();
  };

  KakaStarClient.prototype.onMessage = function (event) {
    var packages = this.codec.decodePackage(event.data);
    var packageList = Array.isArray(packages) ? packages : [packages];

    for (var i = 0; i < packageList.length; i++) {
      this.handlePackage(packageList[i]);
    }

    this.heartbeat.touch();
  };

  KakaStarClient.prototype.onError = function (event) {
    this.emit('io-error', event);
    console.error('socket error:', event);
  };

  KakaStarClient.prototype.onClose = function (event) {
    this.socket = null;
    this.heartbeat.stop();
    this.emit('close', event);
    this.emit('disconnect', event);

    if (this.shouldReconnect) {
      this.scheduleReconnect();
    }
  };

  // 发送心跳
  KakaStarClient.prototype.sendHandshake = function () {
    var body = this.protocol.strencode(JSON.stringify(this.handshakeBuffer));
    this.sendPackage(this.protocol.Package.TYPE_HANDSHAKE, body);
  };

  // 处理
  KakaStarClient.prototype.handlePackage = function (packet) {
    var packageType = this.protocol.Package;
    var handlers = {};
    handlers[packageType.TYPE_HANDSHAKE] = this.handleHandshake;
    handlers[packageType.TYPE_HEARTBEAT] = this.handleHeartbeat;
    handlers[packageType.TYPE_DATA] = this.handleData;
    handlers[packageType.TYPE_KICK] = this.handleKick;

    var handler = handlers[packet.type];
    if (handler) {
      handler.call(this, packet.body);
    }
  };

  KakaStarClient.prototype.handleHandshake = function (data) {
    var response = JSON.parse(this.protocol.strdecode(data));
    if (response.code === RES_OLD_CLIENT) {
      this.emit('error', 'client version not fullfill');
      return;
    }

    if (response.code !== RES_OK) {
      this.emit('error', 'handshake fail');
      return;
    }

    this.applyHandshake(response);
    this.sendPackage(this.protocol.Package.TYPE_HANDSHAKE_ACK);

    if (typeof this.initCallback === 'function') {
      this.initCallback(this.socket);
    }
  };

  KakaStarClient.prototype.handleHeartbeat = function () {
    this.heartbeat.handleHeartbeat();
  };

  KakaStarClient.prototype.sendHeartbeat = function () {
    this.sendPackage(this.protocol.Package.TYPE_HEARTBEAT);
  };

  KakaStarClient.prototype.onHeartbeatTimeout = function () {
    this.emit('heartbeat timeout');
    this.disconnect();
  };

  KakaStarClient.prototype.handleData = function (data) {
    var message = this.decode(data);
    if (!message) {
      return;
    }

    if (!message.id) {
      this.emit(message.route, message.body);
      return;
    }

    var callback = this.callbacks[message.id];
    delete this.callbacks[message.id];
    if (typeof callback === 'function') {
      callback(message.body);
    }
  };

  KakaStarClient.prototype.handleKick = function (data) {
    this.emit('onKick', JSON.parse(this.protocol.strdecode(data)));
  };

  // 应用服务端握手响应，初始化路由字典和心跳配置。
  KakaStarClient.prototype.applyHandshake = function (response) {
      
    // 读取系统级配置；服务端未提供时使用空对象
    // 我好像是记得服务器上来会下发一个心跳间隔和路由字典的
    var system = response.sys || {};

    // 配置路由压缩字典，便于后续消息的编码和解码
    this.codec.setRouteDictionary(system.dict);

    // 配置心跳间隔和超时检测
    this.heartbeat.configure(system.heartbeat);

    // 握手完成后，将用户数据交给调用方处理
    if (typeof this.handshakeCallback === 'function') {
      this.handshakeCallback(response.user);
    }
  };

  // 发送一条业务消息到服务端 -- 地位就和xx平等了
  // 主要是 sendPackage的时候 是 TYPE_DATA  所以  我觉得sendPackage明显要下去一层啊
  KakaStarClient.prototype.sendMessage = function (id, route, body) {
    // 为消息体生成签名，供服务端校验请求完整性
    body = this.signMessageBody(body);
    // 将消息 ID、路由和签名后的消息体编码为协议数据
    var message = this.encode(id, route, body);
    // 以数据包类型发送编码后的消息
    this.sendPackage(this.protocol.Package.TYPE_DATA, message);
  };

  /**
   * Default JSON-based protocol implementation.
   * Applications can provide options.encode and options.decode during init.
   */
  KakaStarClient.prototype.defaultEncode = function (id, route, body) {
    return this.codec.encodeMessage(id, route, body);
  };

  KakaStarClient.prototype.defaultDecode = function (data) {
    return this.codec.decodeMessage(data, this.requestRoutes);
  };

  // 初始化通信加密配置，并生成 RSA 密钥对用于握手
  //handshakeBuffer 是即将发送给服务端的握手数据，
  // sys.rsa 存放客户端生成的 RSA 公钥参数：
  // {
  //   sys: {
  //     rsa: {
  //       rsa_n: '...', // RSA 模数
  //       rsa_e: 65537  // 公钥指数
  //     }
  //   }
  // }
  KakaStarClient.prototype.prepareCrypto = function () {
  
    // 根据 encrypt 配置决定是否启用加密
    this.useCrypto = !!this.options.encrypt;

    // 初始化握手包中的 RSA 信息
    this.handshakeBuffer.sys.rsa = {};

    // 未开启加密时无需生成密钥
    if (!this.useCrypto) {
      return;
    }

    // 加密模式依赖 RSA 实现
    if (!this.rsa) {
      throw new Error('RSA support is required when encrypt is enabled');
    }

    // 生成 RSA 密钥对，供握手包使用
    // 生成 1024 位 RSA 密钥对，公钥指数为 65537（十六进制 10001）
    this.rsa.generate(1024, '10001');

    // 将 RSA 公钥参数写入握手包，供服务端加密后续通信数据
    this.handshakeBuffer.sys.rsa = {
      rsa_n: this.rsa.n.toString(16),
      rsa_e: this.rsa.e
    };
  };

  // 对消息体进行 RSA 签名
  KakaStarClient.prototype.signMessageBody = function (body) {
    
    // 未启用加密时，直接返回原始消息体
    if (!this.useCrypto) {
      return body;
    }

    // 将消息体序列化为 JSON 字符串，并使用 RSA 私钥生成签名
    var json = JSON.stringify(body);

    // 使用 SHA-256 摘要算法生成 RSA 签名，并写入 crypto 字段
    body.__crypto__ = this.rsa.signString(json, 'sha256');
    return body;
  };


  /**
 * 向 WebSocket 服务端发送一条业务协议消息。
 *
 * 职责：
 * 1. 校验 WebSocket 连接状态；
 * 2. 将业务消息编码为二进制协议包；
 * 3. 发送编码后的底层数据。
 *
 * @param {number|string} type 消息类型/协议号，用于服务端识别具体业务指令。
 * @param {*} body 消息内容，通常是对象、数组或 Protobuf/JSON 可编码的数据。
 * @throws {Error} 当前 WebSocket 尚未处于可发送状态时抛出异常。
 */
  KakaStarClient.prototype.sendPackage = function (type, body) {

    // socket 为空表示客户端尚未创建连接，或连接对象已经被释放。
    // readyState 必须为 WebSocket.OPEN，代表连接已建立且允许发送数据。
    if (!this.socket || this.socket.readyState !== WebSocket.OPEN) {
      throw new Error('websocket is not connected');
    }

    // codec 负责协议层编码：
    // - 写入消息类型 type；
    // - 序列化业务内容 body；
    // - 生成服务端约定的二进制包结构，例如包头、长度、消息体。
    var packet = this.codec.encodePackage(type, body);

    // packet.buffer 是底层 ArrayBuffer
    // WebSocket 可直接发送 ArrayBuffer，浏览器会以二进制帧传输给服务端。
    this.socket.send(packet.buffer);
  };

  KakaStarClient.prototype.scheduleReconnect = function () {
    var maxAttempts = this.options.maxReconnectAttempts || DEFAULT_MAX_RECONNECT_ATTEMPTS;
    if (this.reconnectAttempts >= maxAttempts) {
      return;
    }

    var self = this;

    // didReconnect 通常是客户端实例上的布尔标记，表示当前这次 onOpen 来自重连流程
    this.didReconnect = true;
    this.reconnectAttempts++;
    this.reconnectTimer = setTimeout(function () {
      self.reconnectTimer = null;
      self.connect();
    }, this.reconnectDelay);
    this.reconnectDelay *= 2;
  };

  KakaStarClient.prototype.clearTimers = function () {
    this.heartbeat.stop();
    clearTimeout(this.reconnectTimer);
    this.reconnectTimer = null;
  };

  // 通过传入的host 创建 url
  // 前面加 ws:// 表面这是一个 websocket 连接
  // 如果有端口号，则加上端口号
  KakaStarClient.prototype.buildUrl = function () {
    var url = 'ws://' + this.options.host;
    if (this.options.port) {
      url += ':' + this.options.port;
    }

    return url + (this.options.path || '');
  };

  root.KakaStarClient = KakaStarClient;
  root.kakastar = new KakaStarClient(root.Protocol);

  // Existing pages can switch script tags without changing starx.* calls.
  root.starx = root.kakastar;
})(window);
