(function (exports, ByteArray, global) {
  var Protocol = exports;

  var PKG_HEAD_BYTES = 4;
  var MSG_FLAG_BYTES = 1;
  var MSG_ROUTE_CODE_BYTES = 2;
  var MSG_ID_MAX_BYTES = 5;
  var MSG_ROUTE_LEN_BYTES = 1;

  var MSG_ROUTE_CODE_MAX = 0xffff;

  var MSG_COMPRESS_ROUTE_MASK = 0x1;
  var MSG_TYPE_MASK = 0x7;

  var Package = Protocol.Package = {};
  var Message = Protocol.Message = {};

  Package.TYPE_HANDSHAKE = 1;
  Package.TYPE_HANDSHAKE_ACK = 2;
  Package.TYPE_HEARTBEAT = 3;
  Package.TYPE_DATA = 4;
  Package.TYPE_KICK = 5;

  Message.TYPE_REQUEST = 0;
  Message.TYPE_NOTIFY = 1;
  Message.TYPE_RESPONSE = 2;
  Message.TYPE_PUSH = 3;

  /**
   * pomele client encode
   * id message id;
   * route message route
   * msg message body
   * socketio current support string
   */
  Protocol.strencode = function(str) {
    var byteArray = new ByteArray(str.length * 3);
    var offset = 0;
    for(var i = 0; i < str.length; i++){
      var charCode = str.charCodeAt(i);
      var codes = null;
      if(charCode <= 0x7f){
        codes = [charCode];
      }else if(charCode <= 0x7ff){
        codes = [0xc0|(charCode>>6), 0x80|(charCode & 0x3f)];
      }else{
        codes = [0xe0|(charCode>>12), 0x80|((charCode & 0xfc0)>>6), 0x80|(charCode & 0x3f)];
      }
      for(var j = 0; j < codes.length; j++){
        byteArray[offset] = codes[j];
        ++offset;
      }
    }
    var _buffer = new ByteArray(offset);
    copyArray(_buffer, 0, byteArray, 0, offset);
    return _buffer;
  };

  /**
   * client decode
   * msg String data
   * return Message Object
   */
  Protocol.strdecode = function(buffer) {
    var bytes = new ByteArray(buffer);
    var array = [];
    var offset = 0;
    var charCode = 0;
    var end = bytes.length;
    while(offset < end){
      if(bytes[offset] < 128){
        charCode = bytes[offset];
        offset += 1;
      }else if(bytes[offset] < 224){
        charCode = ((bytes[offset] & 0x3f)<<6) + (bytes[offset+1] & 0x3f);
        offset += 2;
      }else{
        charCode = ((bytes[offset] & 0x0f)<<12) + ((bytes[offset+1] & 0x3f)<<6) + (bytes[offset+2] & 0x3f);
        offset += 3;
      }
      array.push(charCode);
    }
    return String.fromCharCode.apply(null, array);
  };

  /**
   * Package protocol encode.
   *
   * Pomelo package format:
   * +------+-------------+------------------+
   * | type | body length |       body       |
   * +------+-------------+------------------+
   *
   * Head: 4bytes
   *   0: package type,
   *      1 - handshake,
   *      2 - handshake ack,
   *      3 - heartbeat,
   *      4 - data
   *      5 - kick
   *   1 - 3: big-endian body length
   * Body: body length bytes
   *
   * @param  {Number}    type   package type
   * @param  {ByteArray} body   body content in bytes
   * @return {ByteArray}        new byte array that contains encode result
   */
  Package.encode = function(type, body){
    var length = body ? body.length : 0;
    var buffer = new ByteArray(PKG_HEAD_BYTES + length);
    var index = 0;
    buffer[index++] = type & 0xff;
    buffer[index++] = (length >> 16) & 0xff;
    buffer[index++] = (length >> 8) & 0xff;
    buffer[index++] = length & 0xff;
    if(body) {
      copyArray(buffer, index, body, 0, length);
    }
    return buffer;
  };

  /**
   * Package protocol decode.
   * See encode for package format.
   *
   * @param  {ByteArray} buffer byte array containing package content
   * @return {Object}           {type: package type, buffer: body byte array}
   */
  Package.decode = function(buffer){
    var offset = 0;
    var bytes = new ByteArray(buffer);
    var length = 0;
    var rs = [];
    while(offset < bytes.length) {
      var type = bytes[offset++];
      length = ((bytes[offset++]) << 16 | (bytes[offset++]) << 8 | bytes[offset++]) >>> 0;
      var body = length ? new ByteArray(length) : null;
      copyArray(body, 0, bytes, offset, length);
      offset += length;
      rs.push({'type': type, 'body': body});
    }
    return rs.length === 1 ? rs[0]: rs;
  };

  /**
   * Message protocol encode.
   *
   * @param  {Number} id            message id
   * @param  {Number} type          message type
   * @param  {Number} compressRoute whether compress route
   * @param  {Number|String} route  route code or route string
   * @param  {Buffer} msg           message body bytes
   * @return {Buffer}               encode result
   */
  /**
   * 将业务 Message 编码为二进制字节数组。
   *
   * 最终数据结构：
   *
   * +--------+------------+----------------+-------------+----------+
   * | flag   | id         | route          | body        |          |
   * +--------+------------+----------------+-------------+----------+
   * | 1 byte | 0~多字节   | 路由编码或文本 | 业务字节数据 |          |
   * +--------+------------+----------------+-------------+----------+
   *
   * 字段是否存在由 type 决定：
   * - request：flag + id + route + body
   * - notify：flag + route + body
   * - response：flag + id + body
   * - push：flag + route + body
   *
   * @param {Number} id
   *   消息 ID。
   *   请求和响应通过相同 ID 建立对应关系。
   *
   * @param {Number} type
   *   消息类型，例如 request、notify、response、push。
   *
   * @param {Number} compressRoute
   *   路由压缩标记：
   *   1 表示 route 使用数字编码；
   *   0 表示 route 使用完整字符串。
   *
   * @param {Number|String} route
   *   路由信息：
   *   - 压缩路由：数字编码，例如 12；
   *   - 普通路由：字符串，例如 'chat.chatHandler.send'。
   *
   * @param {Buffer|ByteArray} msg
   *   已完成序列化的业务消息体字节数据。
   *
   * @return {ByteArray}
   *   完整 Message 的二进制数据。
   */

  Message.encode = function(id, type, compressRoute, route, msg){
    
    // caculate message max length
    /**
     * 计算消息总长度，提前分配精确大小的 ByteArray。
     *
     * Message 固定包含 flag；
     * id、route、body 根据消息类型和实际内容追加。
     */
    var idBytes = msgHasId(type) ? calculateMsgIdBytes(id) : 0;
    var msgLen = MSG_FLAG_BYTES + idBytes;

    /**
     * 根据消息类型写入 route。
     * 压缩路由写数字编码；
     * 普通路由写长度和字符串字节数据。
     */
    /**
    * 请求、通知、推送等带路由的消息进入此分支。
    * 响应消息通常通过 id 找回原始请求路由。
    */
    if(msgHasRoute(type)) {
      if(compressRoute) {

        /**
         * 压缩路由使用固定长度的数字编码。
         * 例如：
         * 'chat.chatHandler.send' -> 12
         */

        if(typeof route !== 'number'){
          throw new Error('error flag for number route!');
        }
        
        // 压缩路由码的固定字节长度。
        msgLen += MSG_ROUTE_CODE_BYTES;
      } else {

        /**
         * 普通路由格式：
         *
         * +------------+--------------------+
         * | routeLength| route bytes        |
         * +------------+--------------------+
         * | 1 byte     | routeLength bytes  |
         * +------------+--------------------+
         */

        msgLen += MSG_ROUTE_LEN_BYTES;
        if(route) {

          // 将字符串路由转换为协议可写入的字节数据。
          route = Protocol.strencode(route);
          if(route.length>255) {
            throw new Error('route maxlength is overflow');
          }

          // 加上完整路由字符串的字节长度。
          msgLen += route.length;
        }
      }
    }

    if(msg) {
      msgLen += msg.length;
    }

    // 一次性创建目标字节数组，避免编码过程反复扩容。
    var buffer = new ByteArray(msgLen);
    var offset = 0;

    // add flag
    /**
     * 写入 flag。
     *
     * flag 通常保存：
     * - 消息类型 type；
     * - 路由是否压缩 compressRoute。
     */
    offset = encodeMsgFlag(type, compressRoute, buffer, offset);

    // add message id
    if(msgHasId(type)) {
      offset = encodeMsgId(id, buffer, offset);
    }

    // add route
    if(msgHasRoute(type)) {
      offset = encodeMsgRoute(compressRoute, route, buffer, offset);
    }

    // add body
      /**
     * 最后写入业务消息体。
     * msg 通常来自：
     *
     * Protocol.strencode(JSON.stringify(body))
     */
    if(msg) {
      offset = encodeMsgBody(msg, buffer, offset);
    }

    return buffer;
  };

  /**
   * Message protocol decode.
   *
   * @param  {Buffer|Uint8Array} buffer message bytes
   * @return {Object}            message object
   */
  Message.decode = function(buffer) {
    var bytes =  new ByteArray(buffer);
    var bytesLen = bytes.length || bytes.byteLength;
    var offset = 0;
    var id = 0;
    var route = null;

    // parse flag
    var flag = bytes[offset++];
    var compressRoute = flag & MSG_COMPRESS_ROUTE_MASK;
    var type = (flag >> 1) & MSG_TYPE_MASK;

    // parse id
    if(msgHasId(type)) {
      var m = parseInt(bytes[offset]);
      var i = 0;
      do{
        var m = parseInt(bytes[offset]);
        id = id + ((m & 0x7f) * Math.pow(2,(7*i)));
        offset++;
        i++;
      }while(m >= 128);
    }

    // parse route
    if(msgHasRoute(type)) {
      if(compressRoute) {
        route = (bytes[offset++]) << 8 | bytes[offset++];
      } else {
        var routeLen = bytes[offset++];
        if(routeLen) {
          route = new ByteArray(routeLen);
          copyArray(route, 0, bytes, offset, routeLen);
          route = Protocol.strdecode(route);
        } else {
          route = '';
        }
        offset += routeLen;
      }
    }

    // parse body
    var bodyLen = bytesLen - offset;
    var body = new ByteArray(bodyLen);

    copyArray(body, 0, bytes, offset, bodyLen);

    return {'id': id, 'type': type, 'compressRoute': compressRoute,
            'route': route, 'body': body};
  };

  var copyArray = function(dest, doffset, src, soffset, length) {
    if('function' === typeof src.copy) {
      // Buffer
      src.copy(dest, doffset, soffset, soffset + length);
    } else {
      // Uint8Array
      for(var index=0; index<length; index++){
        dest[doffset++] = src[soffset++];
      }
    }
  };

  // 根据消息类型判断是否包含 id 字段
  // 其实就是 request 和 response 类型的消息才有 id 字段
  var msgHasId = function(type) {
    return type === Message.TYPE_REQUEST || type === Message.TYPE_RESPONSE;
  };

  // 是否需要route reqest notify push 都需要route
  // 这里很好理解 response因为有id作为索引 只是为了唤起 cb 所以是不需要 route的
  var msgHasRoute = function(type) {
    return type === Message.TYPE_REQUEST || type === Message.TYPE_NOTIFY ||
           type === Message.TYPE_PUSH;
  };

  // 计算消息 id 占用的字节数
  /**
   * 计算消息 ID 编码后占用的字节数。
   *
   * 消息 ID 使用变长整数编码：
   * 每个字节存储 7 位有效数据；
   * 剩余 1 位通常用于表示后续是否还有数据。
   *
   * 示例：
   * - 0 ~ 127：占 1 字节
   * - 128 ~ 16383：占 2 字节
   * - 16384 ~ 2097151：占 3 字节
   *
   * @param {Number} id 消息 ID。
   * @return {Number} 编码该 ID 所需的字节数。
   */
    /**
     * 每轮处理 7 位：
     * - len 记录当前已使用的字节数；
     * - id >>= 7 移除已处理的低 7 位。
     *
     * do...while 保证 id 为 0 时也会占用 1 个字节。
     */

  var calculateMsgIdBytes = function(id) {
    var len = 0;
    do {
      len += 1;
      id >>= 7;
    } while(id > 0);
    return len;
  };

  var encodeMsgFlag = function(type, compressRoute, buffer, offset) {
    if(type !== Message.TYPE_REQUEST && type !== Message.TYPE_NOTIFY &&
       type !== Message.TYPE_RESPONSE && type !== Message.TYPE_PUSH) {
      throw new Error('unkonw message type: ' + type);
    }

    buffer[offset] = (type << 1) | (compressRoute ? 1 : 0);

    return offset + MSG_FLAG_BYTES;
  };

  var encodeMsgId = function(id, buffer, offset) {
    do{
      var tmp = id % 128;
      var next = Math.floor(id/128);

      if(next !== 0){
        tmp = tmp + 128;
      }
      buffer[offset++] = tmp;

      id = next;
    } while(id !== 0);

    return offset;
  };

  var encodeMsgRoute = function(compressRoute, route, buffer, offset) {
    if (compressRoute) {
      if(route > MSG_ROUTE_CODE_MAX){
        throw new Error('route number is overflow');
      }

      buffer[offset++] = (route >> 8) & 0xff;
      buffer[offset++] = route & 0xff;
    } else {
      if(route) {
        buffer[offset++] = route.length & 0xff;
        copyArray(buffer, offset, route, 0, route.length);
        offset += route.length;
      } else {
        buffer[offset++] = 0;
      }
    }

    return offset;
  };

  var encodeMsgBody = function(msg, buffer, offset) {
    copyArray(buffer, offset, msg, 0, msg.length);
    return offset + msg.length;
  };

  if(typeof(window) != "undefined") {
    window.Protocol = Protocol;
  }
})(typeof(window)=="undefined" ? module.exports : (this.Protocol = {}),typeof(window)=="undefined"  ? Buffer : Uint8Array, this);
