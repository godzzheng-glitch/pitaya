(function (root) {
  'use strict';

// 消息的数据流
//
// encode 流程
// 业务对象 body
//   → JSON.stringify
//   → protocol.strencode
//   → Message.encode
//   → Package.encode
//   → 网络传输
//
// decode 流程
// 网络数据
//   → Package.decode
//   → Message.decode
//   → protocol.strdecode
//   → JSON.parse
//   → 业务对象 body


  /**
   * Owns protocol-specific message encoding and route compression.
   * Protocol is supplied by protocol.js and must load before this file.
   */
  /**
   * KakaStarCodec：协议编解码器。  
   *
   * 职责：
   * 1. 调用 protocol.js 提供的 Package / Message 编解码能力。
   * 2. 维护“路由名称 <-> 路由数字编码”的双向字典。
   * 3. 对业务消息进行 JSON 序列化、编码、解码。
   *
   * @param {Object} protocol 协议模块，通常为 window.Protocol。
   */

  // 其实吧 核心都被protocol.js给封装好了  这个只是一个简单的封装器
  //
  function KakaStarCodec(protocol) {

    /**
     * 初始化阶段校验协议依赖。
     * Package 负责外层数据包；
     * Message 负责业务消息；
     * strencode / strdecode 通常负责字符串与字节数组转换。
     */
    if (!protocol || !protocol.Package || !protocol.Message) {
      throw new Error('KakaStarCodec requires window.Protocol');
    }

    // 保存协议对象，供实例方法统一调用。
    this.protocol = protocol;

    /**
     * 路由名称到数字编码的映射。
     *
     * 示例：
     * {
     *   'connector.entryHandler.entry': 1,
     *   'chat.chatHandler.send': 2
     * }
     *
     * Object.create(null) 创建无原型对象，
     * 适合用作纯字典，避免原型链属性影响路由查询。
     */
    this.routeToCode = Object.create(null);


    /**
     * 路由名称到数字编码的映射。
     *
     * 示例：
     * {
     *   'connector.entryHandler.entry': 1,
     *   'chat.chatHandler.send': 2
     * }
     *
     * Object.create(null) 创建无原型对象，
     * 适合用作纯字典，避免原型链属性影响路由查询。
     */
    this.codeToRoute = Object.create(null);
  }

    /**
   * 设置服务端下发的路由压缩字典，并同步建立反向字典。
   *
   * @param {Object} dictionary 路由名称到数字编码的映射。
   */
  KakaStarCodec.prototype.setRouteDictionary = function (dictionary) {

    // 保存正向字典；空值时使用空字典。
    this.routeToCode = dictionary || Object.create(null);
    // 每次更新字典时重新生成反向索引。
    this.codeToRoute = Object.create(null);
    // 遍历所有路由名称。
    for (var route in this.routeToCode) {
      // 只处理对象自身属性，保证字典数据安全可靠。
      if (Object.prototype.hasOwnProperty.call(this.routeToCode, route)) {
        // 建立“编码 -> 路由名称”的反向映射。
        this.codeToRoute[this.routeToCode[route]] = route;
      }
    }
  };

    /**
   * 编码一条业务消息。
   *
   * @param {number} id 请求 ID。
   *   大于 0：请求消息，服务端响应会携带相同 ID。
   *   等于 0：通知消息，无需等待对应响应。
   *
   * @param {string} route 业务路由，例如 'chat.chatHandler.send'。
   * @param {Object} body 业务消息体，会被 JSON 序列化。
   * @returns {Uint8Array|Array} 编码后的 Message 字节数据。
   */
  KakaStarCodec.prototype.encodeMessage = function (id, route, body) {
    
    //Message模块 我觉得后面得要封一下
    var message = this.protocol.Message;

    // 0 表示原始字符串路由，1 表示压缩后的数字路由。
    var compressedRoute = 0;
    // 默认直接传输完整路由名称。
    var encodedRoute = route;

    /**
     * 路由存在于字典时，改为传输更短的数字编码。
     * 路由编码建议从 1 开始分配。
     */
    if (this.routeToCode[route]) {
      encodedRoute = this.routeToCode[route];
      compressedRoute = 1;
    }

    // 调用协议模块的 Message.encode 方法进行编码
    /**
     * Message.encode 参数：
     * 1. id：请求 ID；
     * 2. type：请求或通知；
     * 3. compressedRoute：路由是否使用数字压缩；
     * 4. encodedRoute：路由名称或路由数字编码；
     * 5. body：JSON 字符串转为协议字节数据。
     */
    // JSON.stringify 将对象序列化为字符串，
    // protocol.strencode 将字符串转为字节数组。
    return message.encode(
      id,
      id > 0 ? message.TYPE_REQUEST : message.TYPE_NOTIFY,
      compressedRoute,
      encodedRoute,
      this.protocol.strencode(JSON.stringify(body))
    );
  };

  /**
   * 解码一条业务消息。
   *
   * @param {Uint8Array|ArrayBuffer|Array} data Message 原始字节数据。
   * @param {Object} requestRoutes 请求 ID 到请求路由的映射。
   *   客户端发送请求时通常保存：
   *   requestRoutes[id] = route;
   *
   * @returns {Object|null}
   *   解码成功时返回消息对象；
   *   路由无法恢复时返回 null。
   */
  KakaStarCodec.prototype.decodeMessage = function (data, requestRoutes) {

    // 先解析 Message 的基础字段：id、route、body、compressRoute 等。
    // 这么看 核心解码 其实是在 protocol.Message.decode 里面做的
    var message = this.protocol.Message.decode(data);

    // 这时候已经得到了 id
    /**
     * 响应消息通常只携带 id。
     * 通过此前保存的 requestRoutes 找回原始请求路由。
     */
    //emm 意思是消息分为两种 1种是 响应消息 1种是通知消息
    if (message.id > 0) {
      message.route = requestRoutes[message.id];
      delete requestRoutes[message.id];
      if (!message.route) {
        return null;
      }
    }

    /**
     * 服务端主动推送或通知消息可携带压缩路由码。
     * 使用 codeToRoute 将数字编码还原为完整路由名称。
     */
    // 估计这个应该只会对通知消息起作用吧 毕竟 响应消息的路由是通过 requestRoutes 找回来的
    if (message.compressRoute) {
      message.route = this.codeToRoute[message.route];
      if (!message.route) {
        return null;
      }
    }

    /**
     * message.body 当前为协议字节数据：
     * 1. strdecode 还原 JSON 字符串；
     * 2. JSON.parse 还原业务对象。
     */
    // 不理解为啥要写得这么紧凑
    // 就不能分开两行 1行strdecode 1行 JSON.parse 一行从字节变成string 再一行从string变成对象吗
    message.body = JSON.parse(this.protocol.strdecode(message.body));
    return message;
  };

    /**
   * 编码外层 Package 数据包。
   *
   * Package 常用于区分握手、心跳、数据消息等传输层类型。
   *
   * @param {number} type Package 类型。
   * @param {Uint8Array|Array} body Package 内容。
   * @returns {Uint8Array|Array} 编码后的数据包。
   */
  KakaStarCodec.prototype.encodePackage = function (type, body) {
    return this.protocol.Package.encode(type, body);
  };

  /**
   * 解码外层 Package 数据包。
   *
   * @param {Uint8Array|ArrayBuffer|Array} data 原始包数据。
   * @returns {Object} 包含 type、body 等字段的 Package 对象。
   */
  KakaStarCodec.prototype.decodePackage = function (data) {
    return this.protocol.Package.decode(data);
  };

  root.KakaStarCodec = KakaStarCodec;
})(window);
