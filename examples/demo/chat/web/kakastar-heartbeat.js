(function (root) {
  'use strict';

  var HEARTBEAT_GAP_THRESHOLD = 100;

  /**
   * Manages the heartbeat lifecycle for one websocket connection.
   *
   * The Pitaya server sends a heartbeat package to start a heartbeat cycle.
   * This object delays the client heartbeat response by the negotiated interval,
   * tracks the latest traffic time and reports a timeout to its owner.
   */
  function KakaStarHeartbeat(sendHeartbeat, onTimeout) {
    this.sendHeartbeat = sendHeartbeat;
    this.onTimeout = onTimeout;

    this.interval = 0;
    this.timeout = 0;
    this.nextTimeoutAt = 0;
    this.heartbeatTimer = null;
    this.timeoutTimer = null;
  }

  /**
   * Applies the heartbeat interval provided by the handshake response.
   * The server expresses the interval in seconds; timers use milliseconds.
   */
  KakaStarHeartbeat.prototype.configure = function (intervalSeconds) {
    this.stop();
    this.interval = intervalSeconds ? intervalSeconds * 1000 : 0;
    this.timeout = this.interval * 2;
  };

  /**
   * Handles one heartbeat package from the server.
   * A pending send timer represents the active heartbeat cycle.
   */
  KakaStarHeartbeat.prototype.handleHeartbeat = function () {
    if (!this.interval) {
      return;
    }

    this.clearTimeoutTimer();

    if (this.heartbeatTimer) {
      return;
    }

    var self = this;
    this.heartbeatTimer = setTimeout(function () {
      self.heartbeatTimer = null;
      self.sendHeartbeat();
      self.nextTimeoutAt = Date.now() + self.timeout;
      self.timeoutTimer = setTimeout(function () {
        self.checkTimeout();
      }, self.timeout);
    }, this.interval);
  };

  /**
   * Records incoming websocket traffic as proof that the connection is alive.
   */
  KakaStarHeartbeat.prototype.touch = function () {
    if (this.timeout) {
      this.nextTimeoutAt = Date.now() + this.timeout;
    }
  };

  /**
   * Clears every timer and discards the current heartbeat cycle.
   * Call this during disconnect and before applying a new handshake interval.
   */
  KakaStarHeartbeat.prototype.stop = function () {
    clearTimeout(this.heartbeatTimer);
    this.heartbeatTimer = null;
    this.clearTimeoutTimer();
    this.nextTimeoutAt = 0;
  };

  KakaStarHeartbeat.prototype.clearTimeoutTimer = function () {
    clearTimeout(this.timeoutTimer);
    this.timeoutTimer = null;
  };

  KakaStarHeartbeat.prototype.checkTimeout = function () {
    var remaining = this.nextTimeoutAt - Date.now();
    if (remaining > HEARTBEAT_GAP_THRESHOLD) {
      var self = this;
      this.timeoutTimer = setTimeout(function () {
        self.checkTimeout();
      }, remaining);
      return;
    }

    this.timeoutTimer = null;
    this.onTimeout();
  };

  root.KakaStarHeartbeat = KakaStarHeartbeat;
})(window);
