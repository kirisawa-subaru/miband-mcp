'use strict';

var contact = null;
var did = null;
var begun = false;
var closed = false;
var pollTimer = null;

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

function reportError(error) {
  try { send({phase: 'setup_error', error: String(error), stack: error && error.stack ? String(error.stack) : null}); } catch (_) {}
}

function cleanup() {
  // Close synchronously so queued Java.perform/choose callbacks cannot start
  // work after cleanup has already reported success.
  closed = true;
  if (pollTimer !== null) {
    clearInterval(pollTimer);
    pollTimer = null;
  }
  Java.perform(function () {
    send({phase: 'cleanup_done', resources_cleaned: true});
  });
  return true;
}

function begin(timeoutMs) {
  if (begun) throw new Error('background sync already requested');
  begun = true;
  timeoutMs = timeoutMs || 30000;
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
              Java.scheduleOnMainThread(function () {
                if (closed) return;
                var startedAt = Date.now();
                try {
                  var lastSyncTimeBefore = Number(contact.getLastSyncDataTime());
                  send({
                    phase: 'request',
                    last_sync_time_before: lastSyncTimeBefore,
                    model_status_before: Number(model.getDeviceStatus()),
                    is_connected_before: !!model.isDeviceConnected(),
                    is_idle_before: !!contact.isIDLE(),
                    is_auto: false
                  });
                  if (closed) return;
                  contact.syncData.overload('java.lang.String', 'boolean').call(
                    contact, did, false
                  );
                  var lastState = null;
                  pollTimer = setInterval(function () {
                    Java.perform(function () {
                      if (closed) return;
                      try {
                        var lastSync = Number(contact.getLastSyncDataTime());
                        var state = {
                          last_sync_time: lastSync,
                          model_status: Number(model.getDeviceStatus()),
                          is_connected: !!model.isDeviceConnected(),
                          is_idle: !!contact.isIDLE()
                        };
                        if (lastState === null || JSON.stringify(state) !== JSON.stringify(lastState)) {
                          send({phase: 'state', state: state});
                          lastState = state;
                        }
                        if (lastSync > Number(lastSyncTimeBefore)) {
                          clearInterval(pollTimer);
                          pollTimer = null;
                          send({phase: 'finish', last_sync_time_after: lastSync,
                            model_status_after: state.model_status,
                            is_connected_after: state.is_connected,
                            is_idle_after: state.is_idle,
                            elapsed_ms: Date.now() - startedAt});
                        } else if (Date.now() - startedAt >= timeoutMs) {
                          clearInterval(pollTimer);
                          pollTimer = null;
                          send({phase: 'sync_timeout', state: state});
                        }
                      } catch (error) {
                        clearInterval(pollTimer);
                        pollTimer = null;
                        reportError(error);
                      }
                    });
                  }, 250);
                } catch (error) {
                  reportError(error);
                }
              });
            });
          } catch (error) {
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
      reportError(error);
    }
  });
  return true;
}

rpc.exports = {begin: begin, cleanup: cleanup};
