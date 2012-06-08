var assert = require('assert');
var assertions = require('./assert');
var common = require('./common');
var EventEmitter = require('events').EventEmitter;
var restify = require('restify');
var shelljs = require('shelljs');
var spawn = require('child_process').spawn;
var util = require('util');


///--- Globals

var assertFunction = assertions.assertFunction;
var assertNumber = assertions.assertNumber;
var assertObject = assertions.assertObject;
var assertString = assertions.assertString;

function BackupClient(options) {
  assertObject('options', options);
  assertObject('options.log', options.log);
  assertString('options.dataset', options.dataset);
  assertString('options.snapshotDir', options.snapshotDir);
  assertString('options.serverUrl', options.serverUrl);
  assertString('options.zfsHost', options.zfsHost);
  assertNumber('options.zfsPort', options.zfsPort);
  assertNumber('options.pollInterval', options.pollInterval);
  assertString('options.zfsRecvPath', options.zfsRecvPath);

  EventEmitter.call(this);

  var self = this;

  this.log = options.log;
  this.dataset = options.dataset;
  this.snapshotDir = options.snapshotDir;
  this.serverUrl= options.serverUrl;
  this.zfsHost = options.zfsHost;
  this.zfsPort = options.zfsPort;
  this.zfs_recv = options.zfsRecvPath;
  this.pollInterval = options.pollInterval;
  this.client = restify.createJsonClient({
    url: self.serverUrl,
    version: '*'
  });

  this.log.debug('initializing BackupClient with options', options);
}

module.exports = BackupClient;
util.inherits(BackupClient, EventEmitter);

BackupClient.prototype.restore = function restore(callback) {
  var self = this;

  self.on('err', function(err) {
    self.log.error('zfs receive failed', err);
    return callback(err);
  });

  self.on('done', function() {
    self.log.info('sucessfully received backup image');
    return callback();
  });

  deleteSnapshots(self, function(err) {
    if (err) {
      self.log.error('unable to delete snapshots', err);
      return callback(err);
    }

    startZfsRecv(self, function() {
      postRestoreRequest(self, function(jobPath, err) {
        if (err) {
          self.log.error('posting restore request failed', err);
          return callback(err);
        }
        // Polling actually won't work, because only the receiver knows whether
        // the recv was successful or not. Send exits 0 regardless of recv.
        // However we still poll in case zfs_recv stalls and this will allow us
        // to safely exit should there be a server side error
        //pollRestoreCompletion(self, jobPath, function(job, err) {
          //if (err) {
            //self.log.error('getting request status failed', err);
            //return callback(err);
          //}
          //self.log.info('server side is complete');
        //});
      });
    });
  })
};

function postRestoreRequest(self, callback) {
  var request = {
    host: self.zfsHost,
    port: self.zfsPort,
    dataset: self.dataset
  };

  self.log.debug('Sending %j restore request', request);
  self.client.post('/backup', request, function(err, req, res, obj) {
    if (err) {
      self.log.error('error posting restore request', err);
      return callback(null, err);
    }

    self.log.info('successfully posted restore request with response',
      obj);
    return callback(obj.jobPath);
  });
}

/**
 *  Polls the restore service for the status of the backup job.
 *
 *  @param {string} jobPath The REST path of the backup job.
 *  @param {function} callback The callback of the form f(job, err) where job
 *  is the job object returned from the server, and err indicates an error
 *  either polling for the job or in the job itself.
 *
 */
function pollRestoreCompletion(self, jobPath, callback) {
  var intervalId = setInterval(function() {
    self.log.debug('getting restore job status, ', jobPath, intervalId);
    self.client.get(jobPath, function(err, req, res, obj) {
      if (err) {
        self.log.error('error getting restore job status', err);
        clearInterval(intervalId);
        return callback(null, err);
      }

      self.log.debug('got restore job status', obj);

      if (obj.done === true) {
        self.log.info('restore job is done');
        clearInterval(intervalId)
        return callback(obj);
      } else if (obj.done == 'failed') {
        self.log.error('restore job failed');
        clearInterval(intervalId);
        return callback(null, obj);
      } else {
        return;
      }
    });
  }, self.pollInterval);
}

function startZfsRecv(self, callback) {
  var log = self.log;

  log.info('receiving latest snapshot to ', self.dataset);
  log.info('running cmd %s %s %s -F  %s', self.zfs_recv, 0, self.zfsPort,
           self.dataset);
  var zfsRecv = spawn('pfexec', [self.zfs_recv, 0, self.zfsPort, '-F', self.dataset]);
  //var zfsRecv = spawn(self.zfs_recv, [0, self.zfsPort, '-F', self.dataset]);

  var msg;

  zfsRecv.stdout.on('data', function(data) {
    log.debug('zfsRecv stdout: ', data.toString());
  });

  zfsRecv.stderr.on('data', function(data) {
    var dataStr = data.toString();
    log.error('zfsRecv stderr: ', dataStr);
    if (msg) {
      msg += dataStr;
    } else {
      msg = dataStr;
    }
    msg += data;
  });

  zfsRecv.on('exit', function(code) {
    if (code != 0) {
      var err = {
        msg: msg,
        code: code,
      };
      log.error('unable to compete zfs_recv', err);
      self.emit('err', err);
    } else {
      self.emit('done');
    }
  });

  // Callback to indicate zfs_recv has started
  return callback();
}

/**
 * Deletes any snapshots on the system before attempting a restore
 */
function deleteSnapshots(self, callback) {
  var log = self.log;
  log.debug('getting snapshots from dir ', self.snapshotDir);
  var snapshots = shelljs.ls(self.snapshotDir);
  log.debug('got the following snapshots', snapshots);

  var deleted = 0;
  if (snapshots.length === 0) {
    log.info('no snapshots to delete, returning');
    return callback();
  }
  snapshots.forEach(function(snapshot) {
    snapshot = self.dataset + '@' + snapshot;
    log.info('deleting snapshot %s', snapshot);
    var delSnapshot = spawn('pfexec', ['zfs', 'destroy', snapshot]);
    //var delSnapshot = spawn('zfs', ['destroy', snapshot]);

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
        log.error('unable to delete zfs snapshot', err);
        return callback(err);
      } else {
        deleted++;
        log.info('deleted snapshot', snapshot);
        if (deleted === snapshots.length) {
          return callback();
        }
      }
    });
  });
}
