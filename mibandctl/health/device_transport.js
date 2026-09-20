'use strict';

var contact = null;
var manager = null;
var model = null;
var did = null;
var callbackRefs = {};
var closed = false;
var pending = null;
var nextRequestToken = 1;

function bytesFromBase64(value) {
  return Java.use('android.util.Base64').decode(value, 0);
}

function base64FromBytes(value) {
  return Java.use('android.util.Base64').encodeToString(value, 2);
}

function decodeHeader(data) {
  var bytes = Array.from(data).map(function (value) { return value & 255; });
  var index = 0;
  function varint() {
    var value = 0, shift = 0, octet;
    do {
      if (index >= bytes.length || shift >= 56) throw new Error('malformed protobuf header');
      octet = bytes[index++];
      value += (octet & 127) * Math.pow(2, shift);
      shift += 7;
    } while (octet & 128);
    return value;
  }
  var result = {};
  while (index < bytes.length && (result.type === undefined || result.subtype === undefined)) {
    var tag = varint(), field = tag >>> 3, wire = tag & 7;
    if (wire === 0) {
      var value = varint();
      if (field === 1) result.type = value;
      if (field === 2) result.subtype = value;
    } else if (wire === 2) {
      var length = varint();
      index += length;
    } else if (wire === 1) index += 8;
    else if (wire === 5) index += 4;
    else throw new Error('unsupported protobuf header wire type');
    if (index > bytes.length) throw new Error('truncated protobuf header');
  }
  if (result.subtype === undefined) result.subtype = 0;
  return result;
}

function connected() {
  try { return model !== null && model.isDeviceConnected(); } catch (_) { return false; }
}

function completePending(token, result) {
  if (pending === null || pending.token !== token) return;
  var current = pending;
  pending = null;
  clearTimeout(current.timer);
  delete callbackRefs[token];
  current.resolve(result);
}

function observePacket(token, packetDid, data, responseBasis) {
  if (closed || pending === null || pending.token !== token ||
      packetDid.toString() !== did || data === null) return;
  try {
    var header = decodeHeader(data);
    if (header.type !== pending.type || header.subtype !== pending.subtype) return;
    completePending(token, {
      status: 'ok', sent: true, response: base64FromBytes(data), response_basis: responseBasis
    });
  } catch (_) {}
}

function requestCallback(token) {
  var CallbackInterface = Java.use(
    'com.xiaomi.fitness.device.contact.export.OnSyncCallback'
  );
  var PacketSerializer = Java.use('udh');
  var suffix = Date.now().toString() + Math.floor(Math.random() * 100000).toString();
  var Callback = Java.registerClass({
    name: 'org.mibandmcp.runtime.DeviceRequestCallback' + suffix,
    implements: [CallbackInterface],
    methods: {
      onSuccess: function (packetDid, _type, result) {
        if (pending === null || pending.token !== token) return;
        try {
          var code = result === null ? 255 : Number(result.getCode());
          if (code !== 0) {
            completePending(token, {status: 'error', sent: true, error: 'device callback code ' + code});
            return;
          }
          var packet = result.getPacket();
          if (packet !== null) {
            observePacket(token, packetDid, PacketSerializer.i(packet), 'native_callback_packet');
          }
        } catch (error) {
          completePending(token, {status: 'error', sent: true, error: String(error)});
        }
      },
      onError: function (_packetDid, _type, code) {
        completePending(token, {status: 'error', sent: true, error: 'device callback error ' + Number(code)});
      }
    }
  });
  var callback = Callback.$new();
  callbackRefs[token] = callback;
  return callback;
}

function waitForConnection(timeoutMs) {
  return new Promise(function (resolve) {
    Java.perform(function () {
      if (closed) return resolve({status: 'closed'});
      if (connected()) return resolve({status: 'ok', reconnect_attempted: false});
      var deadline = Date.now() + timeoutMs;
      Java.scheduleOnMainThread(function () {
        if (closed) return resolve({status: 'closed'});
        try {
          manager.connectDevice.overload('java.lang.String').call(manager, did);
        }
        catch (error) { return resolve({status: 'error', error: String(error)}); }
        var checking = false;
        var timer = setInterval(function () {
          if (checking) return;
          checking = true;
          Java.perform(function () {
            try {
              if (closed) {
                clearInterval(timer);
                resolve({status: 'closed'});
              } else if (connected()) {
                clearInterval(timer);
                resolve({status: 'ok', reconnect_attempted: true, reconnect_succeeded: true});
              } else if (Date.now() >= deadline) {
                clearInterval(timer);
                resolve({status: 'not_connected', reconnect_attempted: true, reconnect_succeeded: false});
              }
            } finally { checking = false; }
          });
        }, 250);
      });
    });
  });
}

function begin(retryConnect, timeoutMs) {
  return new Promise(function (resolve) {
    Java.perform(function () {
      if (closed) return resolve({status: 'closed'});
      try {
        var DeviceContact = Java.use('com.xiaomi.fitness.device.contact.export.DeviceContact');
        var DeviceSyncExt = Java.use('com.xiaomi.fitness.device.contact.export.DeviceSyncExtKt');
        contact = Java.cast(
          DeviceSyncExt.getInstance(DeviceContact.Companion.value),
          Java.use('com.xiaomi.fitness.device.contact.DeviceContactImpl')
        );
        var found = false;
        Java.choose('com.xiaomi.fitness.device.manager.WearableDeviceManagerImpl', {
          onMatch: function (value) {
            if (closed) return 'stop';
            found = true;
            try {
              manager = value;
              model = Java.cast(
                manager.getCurrentDeviceModel(),
                Java.use('com.xiaomi.fitness.device.manager.DeviceModelClient')
              );
              if (model === null) {
                resolve({status: 'error', error: 'current wearable model is unavailable'});
                return 'stop';
              }
              did = model.getDid().toString();
              if (connected()) {
                resolve({status: 'ok', reconnect_attempted: false});
                return 'stop';
              }
              var ready = retryConnect
                ? waitForConnection(timeoutMs)
                : Promise.resolve({status: 'not_connected', reconnect_attempted: false});
              ready.then(function (result) {
                Java.perform(function () {
                  if (result.status !== 'ok' || closed) return resolve(result);
                  try {
                    resolve(result);
                  } catch (error) {
                    resolve({status: 'error', error: String(error)});
                  }
                });
              }).catch(function (error) {
                resolve({status: 'error', error: String(error)});
              });
            } catch (error) {
              resolve({status: 'error', error: String(error)});
            }
            return 'stop';
          },
          onComplete: function () {
            if (!found && !closed) resolve({status: 'error', error: 'wearable manager unavailable'});
          }
        });
      } catch (error) { resolve({status: 'error', error: String(error)}); }
    });
  });
}

function reconnect(timeoutMs) {
  return waitForConnection(timeoutMs);
}

function request(packetBase64, responseType, responseSubtype, timeoutMs) {
  return new Promise(function (resolve) {
    Java.perform(function () {
      if (closed) return resolve({status: 'closed', sent: false});
      if (!connected()) return resolve({status: 'not_connected', sent: false});
      if (pending !== null) return resolve({status: 'busy', sent: false, error: 'another packet request is pending'});
      try {
        var token = nextRequestToken++;
        var timer = setTimeout(function () {
          Java.perform(function () {
            if (pending === null || pending.token !== token) return;
            completePending(token, {status: connected() ? 'timeout' : 'not_connected', sent: true});
          });
        }, timeoutMs);
        pending = {token: token, type: responseType, subtype: responseSubtype, resolve: resolve, timer: timer};
        var callback = requestCallback(token);
        var task = contact.call.overload(
          'java.lang.String', 'int', '[B', 'boolean',
          'com.xiaomi.fitness.device.contact.export.OnSyncCallback', 'int'
        ).call(contact, did, 101, bytesFromBase64(packetBase64), true, callback, timeoutMs);
        if (task < 0) completePending(token, {status: 'error', sent: false, error: 'Xiaomi Health rejected packet request'});
      } catch (error) {
        if (pending !== null) completePending(pending.token, {status: 'error', sent: false, error: String(error)});
      }
    });
  });
}

function sendPacket(packetBase64) {
  return new Promise(function (resolve) {
    Java.perform(function () {
      if (closed) return resolve({status: 'closed', sent: false});
      if (!connected()) return resolve({status: 'not_connected', sent: false});
      try {
        var task = contact.call.overload(
          'java.lang.String', 'int', '[B', 'boolean',
          'com.xiaomi.fitness.device.contact.export.OnSyncCallback', 'int'
        ).call(contact, did, 101, bytesFromBase64(packetBase64), false, null, 8000);
        resolve(task < 0 ? {status: 'error', sent: false, error: 'Xiaomi Health rejected packet'}
                         : {status: 'ok', sent: true, task_id: task});
      } catch (error) { resolve({status: 'error', sent: false, error: String(error)}); }
    });
  });
}

function cleanup() {
  closed = true;
  if (pending !== null) completePending(pending.token, {status: 'closed', sent: true});
  callbackRefs = {};
  return new Promise(function (resolve) {
    Java.perform(function () {
      resolve({status: 'ok', hook_restored: true});
    });
  });
}

rpc.exports = {begin: begin, reconnect: reconnect, request: request, send: sendPacket, cleanup: cleanup};
