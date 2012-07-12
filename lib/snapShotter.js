var assert = require('assert-plus');
var EventEmitter = require('events').EventEmitter;
var shelljs = require('shelljs');
var spawn = require('child_process').spawn;
var util = require('util');
var pg = require('pg');
var Client = pg.Client;

function SnapShotter(options) {
  assert.object(options, 'options');
  assert.object(options.log, 'options.log');
  assert.string(options.dataset, 'options.dataset');
  assert.number(options.pollInterval, 'options.pollInterval');
  assert.string(options.snapshotDir, 'options.snapshotDir');
  assert.number(options.snapshotNumber, 'options.snapshotNumber');
  assert.string(options.pgUrl, 'options.pgUrl');

  EventEmitter.call(this);

  this.log = options.log;
  this.pollInterval = options.pollInterval;
  this.dataset = options.dataset;
  this.snapshotDir = options.snapshotDir;
  this.snapshotNumber = options.snapshotNumber;
  this.pgUrl = options.pgUrl;

  this.log.info('initialized snapshotter with options', options);
}

module.exports = SnapShotter;
util.inherits(SnapShotter, EventEmitter);

SnapShotter.prototype.start = function start(callback) {
  var self = this;
  var log = self.log;

  log.info('starting snapshotter daemon');

  var snapshot = self.dataset + '@' + Date.now();
  // manually start the first time as setInterval waits the interval before
  // starting
  self.createSnapshot(function(err) {
    if (err) {
      self.emit('err', err);
    }
  });

  var intervalId = setInterval(function() {

    self.createSnapshot(function(err) {
      if (err) {
        self.emit('err', err);
      }
    });

    var snapshots = shelljs.ls(self.snapshotDir);
    // sort snapshots by earliest ones first
    snapshots.sort(function(a, b) {
      return a - b;
    });

    log.debug('got snapshots', snapshots);
    // delete snapshots
    if (snapshots.length > self.snapshotNumber) {
      log.info('deleting snapshots as number of snapshots %s exceeds threshold',
               snapshots.length, self.snapshotNumber);

      for (var i = 0; i < snapshots.length - self.snapshotNumber; i++) {
        var delSnapshot = self.dataset + '@' + snapshots[i];

        deleteSnapshot(self, delSnapshot, function(err) {
          if (err) {
            self.emit('err', err);
          }
        });
      }
    }
  }, self.pollInterval);

  log.info('started snapshotter daemon');
  return callback();
};

/**
 * Creates a zfs snapshot of the current postgres data directory.
 * 3 steps to creating a snapshot.
 * 1) pg_start_backup.
 * 2) write snapshot to zfs.
 * 3) pg_stop_backup.
 */
SnapShotter.prototype.createSnapshot =
  function createSnapshot(callback)
{
  var self = this;
  var snapshot = self.dataset + '@' + Date.now();
  var log = self.log;
  log.info('creating snapshot', snapshot);
  pgStartBackup(self, function(backupErr, result) {
    writeSnapshot(self, snapshot, function(snapshotErr) {
      pgStopBackup(self, function(stopErr, result) {
        if (stopErr || snapshotErr || backupErr) {
          var err = {
            backupErr: backupErr,
            snapshotErr: snapshotErr,
            stopErr: stopErr
          };
          log.warn('error encountered while creating snapshot', err);
        }

        return callback(err);
      });
    });
  });
}

/**
 * Write a zfs snapshot to disk.
 * @param String snapshot The name of the snapshot.
 */
function writeSnapshot(self, snapshot, callback) {
  var log = self.log;
  log.info('writing snapshot', snapshot);
  var writeSnapshot = spawn('zfs', ['snapshot', snapshot]);

  writeSnapshot.stdout.on('data', function(data) {
    log.debug('writeSnapshot stdout: ', data.toString());
  });

  var msg;
  writeSnapshot.stderr.on('data', function(data) {
    var dataStr = data.toString();
    log.error('writeSnapshot stderr: ', dataStr);
    if (msg) {
      msg += dataStr;
    } else {
      msg = dataStr;
    }
    msg += data;
  });

  writeSnapshot.on('exit', function(code) {
    if (code != 0) {
      var err = {
        msg: msg,
        code: code,
      };
      log.error('unable to compete zfs_send', err);
      return callback(err);
    }

    log.info('completed snapshot', snapshot);
    return callback();
  });
}

/**
 * Delete a zfs snapshot from disk
 * @param String snapshot The name of the snapshot.
 */
function deleteSnapshot(self, snapshot, callback) {
  var log = self.log;
  log.info('deleting snapshot', snapshot);

  var delSnapshot = spawn('zfs', ['destroy', snapshot]);

  delSnapshot.stdout.on('data', function(data) {
    log.debug('delSnapshot stdout: ', data.toString());
  });

  var msg;
  delSnapshot.stderr.on('data', function(data) {
    var dataStr = data.toString();
    log.error('delSnapshot stderr: ', dataStr);
    if (msg) {
      msg += dataStr;
    } else {
      msg = dataStr;
    }
    msg += data;
  });

  delSnapshot.on('exit', function(code) {
    if (code != 0) {
      var err = {
        msg: msg,
        code: code,
      };
      log.error('unable to compete zfs_send', err);
      return callback(err);
    }

    log.info('completed snapshot', snapshot);
    return callback();
  });
}

function pgStopBackup(self, callback) {
  self.log.info('stopping pg_start_backup');
  var queryString = 'SELECT pg_stop_backup();';
  queryDb(self, queryString, function(err, result) {
    return callback(err, result);
  });
}

function pgStartBackup(self, callback) {
  self.log.info('starting pg_start_backup');
  var label = new Date().getTime();
  var queryString = 'SELECT pg_start_backup(\'' + label + '\', true);';
  queryDb(self, queryString, function(err, result) {
    return callback(err, result);
  });
}

function queryDb(self, query, callback) {
  var log = self.log;
  log.debug('entering querydb %s', query);

  var client = new Client(self.pgUrl);
  client.connect(function(err) {
    if (err) {
      log.warn({err: err}, 'can\'t connect to pg with err');
      client.end();
      return callback(err);
    }
    log.debug('connected to pg, running query %s', query);
    client.query(query, function(err, result) {
      client.end();
      if (err) {
        log.warn({err: err}, 'error whilst querying pg ');
      }
      return callback(err, result);
    });
  });
}
