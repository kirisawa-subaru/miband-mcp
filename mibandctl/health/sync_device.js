'use strict';

var contact = null;
var did = null;
var begun = false;
var closed = false;
var sawStart = false;
var startHook = null;
var finishHook = null;

function ensureConnected(manager, model, callback) {
  if (model !== null && model.isDeviceConnected()) return callback(true);
  send({phase: 'reconnect_start'});
  var deadline = Date.now() + 10000;
  Java.scheduleOnMainThread(function () {
    if (closed) return callback(false);
    try { manager.connectDevice(); }
    catch (error) {
      send({phase: 'reconnect_error', error: String(error)});
      return callback(false);
    }
    var checking = false;
    var timer = setInterval(function () {
      if (checking) return;
      checking = true;
      Java.perform(function () {
        try {
          if (closed) {
            clearInterval(timer);
            callback(false);
          } else if (model.isDeviceConnected()) {
            clearInterval(timer);
            send({phase: 'reconnect_success'});
            callback(true);
          } else if (Date.now() >= deadline) {
            clearInterval(timer);
            send({phase: 'reconnect_error', error: 'wearable reconnect timed out'});
            callback(false);
          }
        } finally { checking = false; }
      });
    }, 250);
  });
}

function restoreHooks() {
  var restored = true;
  if (startHook !== null) {
    try { startHook.implementation = null; } catch (_) { restored = false; }
  }
  if (finishHook !== null) {
    try { finishHook.implementation = null; } catch (_) { restored = false; }
  }
  if (restored) {
    startHook = null;
    finishHook = null;
  }
  return restored;
}

function reportError(error) {
  try { send({phase: 'setup_error', error: String(error)}); } catch (_) {}
}

function cleanup() {
  // Close synchronously so queued Java.perform/choose callbacks cannot start
  // work after cleanup has already reported success.
  closed = true;
  Java.perform(function () {
    send({phase: 'cleanup_done', hook_restored: restoreHooks()});
  });
  return true;
}

function installHooks() {
  var SyncObservers = Java.use('com.xiaomi.fitness.device.contact.SyncObservers');
  startHook = SyncObservers.onStart.overload(
    'com.xiaomi.fitness.device.manager.export.WearableDeviceModel'
  );
  finishHook = SyncObservers.onFinish.overload(
    'com.xiaomi.fitness.device.manager.export.WearableDeviceModel', 'int'
  );
  var originalStart = startHook;
  var originalFinish = finishHook;

  startHook.implementation = function (model) {
    var result = originalStart.call(this, model);
    try {
      if (!closed && model !== null && model.getDid().toString() === did) {
        sawStart = true;
        send({phase: 'start'});
      }
    } catch (error) { reportError(error); }
    return result;
  };

  finishHook.implementation = function (model, code) {
    var result = originalFinish.call(this, model, code);
    try {
      if (!closed && sawStart && model !== null && model.getDid().toString() === did) {
        send({
          phase: 'finish',
          code: code,
          last_sync_time_after: Number(contact.getLastSyncDataTime())
        });
      }
    } catch (error) { reportError(error); }
    return result;
  };
}

function begin() {
  if (begun) throw new Error('background sync already requested');
  begun = true;
  Java.perform(function () {
    if (closed) {
      send({phase: 'cancelled', error: 'background sync was cancelled before setup'});
      return;
    }
    try {
      var DeviceContact = Java.use('com.xiaomi.fitness.device.contact.export.DeviceContact');
      var DeviceSyncExt = Java.use('com.xiaomi.fitness.device.contact.export.DeviceSyncExtKt');
      contact = Java.cast(
        DeviceSyncExt.getInstance(DeviceContact.Companion.value),
        Java.use('com.xiaomi.fitness.device.contact.DeviceContactImpl')
      );
      if (!contact.isIDLE()) {
        send({phase: 'busy', error: 'Xiaomi Health device sync is already in progress'});
        return;
      }

      var found = false;
      Java.choose('com.xiaomi.fitness.device.manager.WearableDeviceManagerImpl', {
        onMatch: function (manager) {
          if (closed) return 'stop';
          found = true;
          try {
            var model = Java.cast(
              manager.getCurrentDeviceModel(),
              Java.use('com.xiaomi.fitness.device.manager.DeviceModelClient')
            );
            if (model === null) {
              send({phase: 'not_connected', error: 'wearable model is unavailable'});
              return 'stop';
            }
            did = model.getDid().toString();
            ensureConnected(manager, model, function (connected) {
              if (!connected || closed) {
                if (!closed) send({phase: 'not_connected', error: 'wearable is not connected'});
                return;
              }
              installHooks();
              if (closed) {
                restoreHooks();
                return;
              }
              Java.scheduleOnMainThread(function () {
                if (closed) return;
                try {
                  send({
                    phase: 'request',
                    last_sync_time_before: Number(contact.getLastSyncDataTime()),
                    is_auto: false
                  });
                  if (closed) return;
                  contact.syncData.overload('java.lang.String', 'boolean').call(
                    contact, did, false
                  );
                } catch (error) {
                  restoreHooks();
                  reportError(error);
                }
              });
            });
          } catch (error) {
            restoreHooks();
            reportError(error);
          }
          return 'stop';
        },
        onComplete: function () {
          if (!closed && !found) {
            send({phase: 'manager_missing', error: 'wearable manager is unavailable'});
          }
        }
      });
    } catch (error) {
      restoreHooks();
      reportError(error);
    }
  });
  return true;
}

rpc.exports = {begin: begin, cleanup: cleanup};
