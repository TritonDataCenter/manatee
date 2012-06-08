var assert = require('assert');
var EventEmitter = require('events').EventEmitter;
var shelljs = require('shelljs');
var spawn = require('child_process').spawn;
var util = require('util');
var assertions = require('./assert');



///--- Globals

var assertFunction = assertions.assertFunction;
var assertNumber = assertions.assertNumber;
var assertObject = assertions.assertObject;
var assertString = assertions.assertString;

function BackupSender(options) {
  assertObject('options', options);
  assertObject('options.log', options.log);
  assertString('options.zfsSendPath', options.zfsSendPath);
  assertString('options.dataset', options.dataset);
  assertString('options.snapshotDir', options.snapshotDir);
  assertObject('options.queue', options.queue);

  EventEmitter.call(this);

  this.log = options.log;
  this.zfs_send = options.zfsSendPath;
  this.dataset = options.dataset;
  this.snapshotDir = options.snapshotDir;
  this.queue = options.queue;

  var self = this;

  this.queue.on('push', function(backupJob) {
    send(self, backupJob, function(err) {
      if (err) {
        self.emit('err', err);
      } else {
        self.emit('done', backupJob);
      }
    });
  });

  this.log.debug('Initializing BackupSender with options', options);
}

module.exports = BackupSender;
util.inherits(BackupSender, EventEmitter);

function send(self, backupJob, callback) {
  var log = self.log;

  getLatestSnapshot(self, function(snapshot) {
    log.info('sending latest snapshot %s to ', snapshot, backupJob);

    log.info('running cmd: %s %s %s %s', self.zfs_send, backupJob.host,
             backupJob.port, snapshot);
    var zfsSend = spawn(self.zfs_send, [backupJob.host, backupJob.port,
                                        snapshot]);

    zfsSend.stdout.on('data', function(data) {
      log.debug('zfsSend stdout: ', data.toString());
    });

    var msg;
    zfsSend.stderr.on('data', function(data) {
      var dataStr = data.toString();
      log.error('zfsSend stderr: ', dataStr);
      if (msg) {
        msg += dataStr;
      } else {
        msg = dataStr;
      }
      msg += data;
    });

    zfsSend.on('exit', function(code) {
      if (code != 0) {
        var err = {
          msg: msg,
          code: code,
          backupJob: backupJob
        };
        backupJob.done = 'failed';
        log.error('unable to compete zfs_send', err);
        return callback(err);
      }

      backupJob.done = true;
      log.info('completed backup job', backupJob);
      return callback();
    });
  });
}

function getLatestSnapshot(self, callback) {
  var log = self.log;
  log.debug('getting snapshots from dir ', self.snapshotDir);
  var snapshots = shelljs.ls(self.snapshotDir);
  log.debug('got the following snapshots', snapshots);
  snapshots.sort(compareNumbers);

  var latestSnapshot = snapshots[snapshots.length - 1];
  latestSnapshot = self.dataset + '@' + latestSnapshot;
  log.debug('latest snapshot is %s', latestSnapshot);
  return callback(latestSnapshot);
}

function compareNumbers(a, b) {
  return a - b;
}
