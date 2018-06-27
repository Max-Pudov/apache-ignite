/*
 * @license
 * angular-socket-io v0.7.0
 * (c) 2014 Brian Ford http://briantford.com
 * License: MIT
 */

angular.module('btford.socket-io', []).
  provider('socketFactory', function() {

      'use strict';

      // when forwarding events, prefix the event name
      const defaultPrefix = 'socket:';

      // expose to provider
      this.$get = ['$rootScope', '$timeout', function($rootScope, $timeout) {

          const asyncAngularify = function(socket, callback) {
              return callback ? function() {
                  const args = arguments;
                  // $timeout(function() {
                  callback.apply(socket, args);
                  // }, 0);
              } : angular.noop;
          };

          return function socketFactory(options) {
              options = options || {};
              const socket = options.ioSocket || io.connect();
              const prefix = options.prefix === void 0 ? defaultPrefix : options.prefix;
              const defaultScope = options.scope || $rootScope;

              const addListener = function(eventName, callback) {
                  socket.on(eventName, callback.__ng = asyncAngularify(socket, callback));
              };

              const addOnceListener = function(eventName, callback) {
                  socket.once(eventName, callback.__ng = asyncAngularify(socket, callback));
              };

              const wrappedSocket = {
                  on: addListener,
                  addListener,
                  once: addOnceListener,

                  emit(eventName, data, callback) {
                      const lastIndex = arguments.length - 1;
                      callback = arguments[lastIndex];
                      if (typeof callback === 'function') {
                          callback = asyncAngularify(socket, callback);
                          arguments[lastIndex] = callback;
                      }
                      return socket.emit(...arguments);
                  },

                  removeListener(ev, fn) {
                      if (fn && fn.__ng)
                          arguments[1] = fn.__ng;

                      return socket.removeListener(...arguments);
                  },

                  removeAllListeners() {
                      return socket.removeAllListeners(...arguments);
                  },

                  disconnect(close) {
                      return socket.disconnect(close);
                  },

                  connect() {
                      return socket.connect();
                  },

                  // when socket.on('someEvent', fn (data) { ... }),
                  // call scope.$broadcast('someEvent', data)
                  forward(events, scope) {
                      if (events instanceof Array === false)
                          events = [events];

                      if (!scope)
                          scope = defaultScope;

                      events.forEach(function(eventName) {
                          const prefixedEvent = prefix + eventName;
                          const forwardBroadcast = asyncAngularify(socket, function() {
                              Array.prototype.unshift.call(arguments, prefixedEvent);
                              scope.$broadcast(...arguments);
                          });
                          scope.$on('$destroy', function() {
                              socket.removeListener(eventName, forwardBroadcast);
                          });
                          socket.on(eventName, forwardBroadcast);
                      });
                  }
              };

              return wrappedSocket;
          };
      }];
  });
