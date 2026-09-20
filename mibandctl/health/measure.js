'use strict';

var contact = null;
var did = null;
var observer = null;
var startCallback = null;
var stopCallback = null;
var timeoutHandle = null;
var begun = false;
var stopping = false;

function ensureConnected(manager, model, callback) {
  if (model !== null && model.isDeviceConnected()) return callback(true, false);
  send({phase: 'reconnect_start'});
  var deadline = Date.now() + 10000;
  Java.scheduleOnMainThread(function () {
    if (stopping) return callback(false, true);
    try { manager.connectDevice(); }
    catch (error) {
      send({phase: 'reconnect_error', error: String(error)});
      return callback(false, true);
    }
    var checking = false;
    var timer = setInterval(function () {
      if (checking) return;
      checking = true;
      Java.perform(function () {
        try {
          if (stopping) {
            clearInterval(timer);
            callback(false, true);
          } else if (model.isDeviceConnected()) {
            clearInterval(timer);
            send({phase: 'reconnect_success'});
            callback(true, true);
          } else if (Date.now() >= deadline) {
            clearInterval(timer);
            send({phase: 'reconnect_error', error: 'wearable reconnect timed out'});
            callback(false, true);
          }
        } finally { checking = false; }
      });
    }, 250);
  });
}

function decodeProto(bytes) {
  var out = {};
  var index = 0;

  function varint() {
    var value = 0;
    var shift = 0;
    while (index < bytes.length && shift < 56) {
      var octet = bytes[index++];
      value += (octet & 127) * Math.pow(2, shift);
      if ((octet & 128) === 0) return value;
      shift += 7;
    }
    throw new Error('malformed protobuf varint');
  }

  while (index < bytes.length) {
    var tag = varint();
    var field = tag >>> 3;
    var wire = tag & 7;
    if (field === 0) throw new Error('invalid protobuf field');
    if (wire === 0) {
      out[field] = varint();
    } else if (wire === 2) {
      var length = varint();
      if (length < 0 || index + length > bytes.length) {
        throw new Error('truncated protobuf field');
      }
      out[field] = bytes.slice(index, index + length);
      index += length;
    } else if (wire === 1) {
      index += 8;
    } else if (wire === 5) {
      index += 4;
    } else {
      throw new Error('unsupported protobuf wire type ' + wire);
    }
    if (index > bytes.length) throw new Error('truncated protobuf value');
  }
  return out;
}

function removeObserver() {
  if (contact !== null && observer !== null) {
    try { contact.removeRawObserver(observer); } catch (_) {}
    observer = null;
  }
}

function requestStop() {
  if (stopping) return;
  stopping = true;
  if (timeoutHandle !== null) {
    clearTimeout(timeoutHandle);
    timeoutHandle = null;
  }
  Java.perform(function () {
    if (contact === null || did === null || stopCallback === null) {
      removeObserver();
      send({phase: 'stop_unavailable', error: 'authenticated transport is unavailable'});
      return;
    }
    try {
      contact.call.overload(
        'java.lang.String', 'int', '[B', 'boolean',
        'com.xiaomi.fitness.device.contact.export.OnSyncCallback', 'int'
      ).call(
        contact, did, 101, Java.array('byte', [8, 8, 16, 46]),
        false, stopCallback, 8000
      );
    } catch (error) {
      removeObserver();
      send({phase: 'stop_error', error: String(error)});
    }
  });
}

function beginMeasurement(timeoutMs) {
  if (begun) throw new Error('measurement already started');
  begun = true;
  Java.perform(function () {
    try {
      var DeviceContact = Java.use('com.xiaomi.fitness.device.contact.export.DeviceContact');
      var DeviceSyncExt = Java.use('com.xiaomi.fitness.device.contact.export.DeviceSyncExtKt');
      var DeviceContactImpl = Java.use('com.xiaomi.fitness.device.contact.DeviceContactImpl');
      contact = Java.cast(
        DeviceSyncExt.getInstance(DeviceContact.Companion.value),
        DeviceContactImpl
      );

      var CallbackInterface = Java.use(
        'com.xiaomi.fitness.device.contact.export.OnSyncCallback'
      );
      var suffix = Date.now().toString() + Math.floor(Math.random() * 100000).toString();
      var StartCallback = Java.registerClass({
        name: 'org.mibandmcp.runtime.StartCallback' + suffix,
        implements: [CallbackInterface],
        methods: {
          onSuccess: function (_did, _type, syncResult) {
            var code = syncResult === null ? 255 : syncResult.getCode();
            send({phase: 'start_ack', code: code});
            if (code !== 0) requestStop();
          },
          onError: function (_did, _type, code) {
            send({phase: 'start_error', code: code});
            requestStop();
          }
        }
      });
      startCallback = StartCallback.$new();

      var StopCallback = Java.registerClass({
        name: 'org.mibandmcp.runtime.StopCallback' + suffix,
        implements: [CallbackInterface],
        methods: {
          onSuccess: function (_did, _type, syncResult) {
            var code = syncResult === null ? 255 : syncResult.getCode();
            removeObserver();
            send({phase: 'stop_ack', code: code});
          },
          onError: function (_did, _type, code) {
            removeObserver();
            send({phase: 'stop_error', code: code});
          }
        }
      });
      stopCallback = StopCallback.$new();

      var ObserverInterface = Java.use(
        'com.xiaomi.fitness.device.contact.export.RawPacketObserver'
      );
      var Observer = Java.registerClass({
        name: 'org.mibandmcp.runtime.RealtimeObserver' + suffix,
        implements: [ObserverInterface],
        methods: {
          onRawPacket: function (packetDid, _channel, data) {
            try {
              if (did === null || packetDid.toString() !== did) return;
              var bytes = Array.from(data).map(function (value) { return value & 255; });
              var packet = decodeProto(bytes);
              if (packet[1] !== 8 || packet[2] !== 47) return;
              var health = decodeProto(packet[10] || []);
              var stats = decodeProto(health[39] || []);
              var heartRate = stats[4] || null;
              send({phase: 'realtime', heart_rate: heartRate});
              if (heartRate > 10 && heartRate <= 255) requestStop();
            } catch (error) {
              send({phase: 'observer_error', error: String(error)});
              requestStop();
            }
          }
        }
      });
      observer = Observer.$new();

      var found = false;
      Java.choose('com.xiaomi.fitness.device.manager.WearableDeviceManagerImpl', {
        onMatch: function (manager) {
          found = true;
          var DeviceModelClient = Java.use(
            'com.xiaomi.fitness.device.manager.DeviceModelClient'
          );
          var model = Java.cast(manager.getCurrentDeviceModel(), DeviceModelClient);
          if (model === null) {
            send({phase: 'not_connected', error: 'wearable model is unavailable'});
            return 'stop';
          }
          did = model.getDid().toString();
          ensureConnected(manager, model, function (connected) {
            if (!connected || stopping) {
              send({phase: 'not_connected', error: 'wearable is not connected'});
              return;
            }
            contact.addRawObserver(observer);
            contact.call.overload(
              'java.lang.String', 'int', '[B', 'boolean',
              'com.xiaomi.fitness.device.contact.export.OnSyncCallback', 'int'
            ).call(
              contact, did, 101, Java.array('byte', [8, 8, 16, 45]),
              false, startCallback, 8000
            );
            timeoutHandle = setTimeout(requestStop, Math.max(100, timeoutMs));
          });
          return 'stop';
        },
        onComplete: function () {
          if (!found) send({phase: 'manager_missing', error: 'wearable manager is unavailable'});
        }
      });
    } catch (error) {
      removeObserver();
      send({phase: 'setup_error', error: String(error)});
    }
  });
  return true;
}

rpc.exports = {
  begin: beginMeasurement,
  stop: function () {
    requestStop();
    return true;
  }
};
