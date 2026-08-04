(function (root) {
  'use strict';

  /**
   * Minimal event emitter used by KakaStarClient.
   * The public API mirrors the event API exposed by the legacy starx client.
   */
  function KakaStarEmitter() {
    this._listeners = Object.create(null);
  }

  KakaStarEmitter.prototype.on =
  KakaStarEmitter.prototype.addEventListener = function (eventName, callback) {
    if (typeof callback !== 'function') {
      return this;
    }

    var listeners = this._listeners[eventName] || (this._listeners[eventName] = []);
    listeners.push(callback);
    return this;
  };

  KakaStarEmitter.prototype.once = function (eventName, callback) {
    var self = this;

    function onceCallback() {
      self.off(eventName, onceCallback);
      callback.apply(self, arguments);
    }

    onceCallback._originalCallback = callback;
    return this.on(eventName, onceCallback);
  };

  KakaStarEmitter.prototype.off =
  KakaStarEmitter.prototype.removeListener =
  KakaStarEmitter.prototype.removeAllListeners =
  KakaStarEmitter.prototype.removeEventListener = function (eventName, callback) {
    if (arguments.length === 0) {
      this._listeners = Object.create(null);
      return this;
    }

    var listeners = this._listeners[eventName];
    if (!listeners) {
      return this;
    }

    if (arguments.length === 1) {
      delete this._listeners[eventName];
      return this;
    }

    for (var i = 0; i < listeners.length; i++) {
      var listener = listeners[i];
      if (listener === callback || listener._originalCallback === callback) {
        listeners.splice(i, 1);
        break;
      }
    }

    return this;
  };

  KakaStarEmitter.prototype.emit = function (eventName) {
    var listeners = this._listeners[eventName];
    if (!listeners) {
      return this;
    }

    var args = Array.prototype.slice.call(arguments, 1);
    listeners = listeners.slice();
    for (var i = 0; i < listeners.length; i++) {
      listeners[i].apply(this, args);
    }

    return this;
  };

  KakaStarEmitter.prototype.listeners = function (eventName) {
    return this._listeners[eventName] || [];
  };

  KakaStarEmitter.prototype.hasListeners = function (eventName) {
    return this.listeners(eventName).length > 0;
  };

  root.KakaStarEmitter = KakaStarEmitter;
})(window);
