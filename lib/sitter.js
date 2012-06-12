var EventEmitter = require('events').EventEmitter;
var PostgresMan = require('./postgresMan');
var SnapShotter = require('./snapShotter');
var Shard = require('./shard');
var assert = require('assert');
var assertions = require('./assert');
var common = require('./common');
var Shard = require('./shard');
var util = require('util');

///--- Globals

var assertFunction = assertions.assertFunction;
var assertNumber = assertions.assertNumber;
var assertObject = assertions.assertObject;
var assertString = assertions.assertString;

function Sitter(options) {
  assertObject('options', options);
  assertObject('options.log', options.log);
  assertObject('options.shardCfg', options.shardCfg);
  assertObject('options.postgresManCfg', options.postgresManCfg);
  assertObject('options.snapShotterCfg', options.snapShotterCfg);
  assertString('options.backupUrl', options.backupUrl);
  assertNumber('options.xlogInterval', options.xlogInterval);
  assertNumber('options.xlogSlack', options.xlogSlack);
  assertNumber('options.xlogTimeout', options.xlogTimeout);

  EventEmitter.call(this);
  this.log = options.log;

  this.log.info('new Sitter with options', options);

  this.shard = new Shard(options.shardCfg);
  this.postgresMan = new PostgresMan(options.postgresManCfg);
  this.snapShotter = new SnapShotter(options.snapShotterCfg);
  this.backupUrl = options.backupUrl;
  this.xlogInterval = options.xlogInterval;
  this.xlogIntervalId = null;
  this.xlogSlack = options.xlogSlack;
  this.xlogTimeout = options.xlogTimeout;
  this.timeSinceXlogUpdate = new Date().getTime();
  this.myLastXlog = 0;
}

module.exports = Sitter;
util.inherits(Sitter, EventEmitter);

/**
 * Join shard
 * depending on role in shard - start postgres
 * if role changes - restart postgres
 */
Sitter.prototype.init = function init() {
  var self = this;
  var log = self.log;
  var shard = self.shard;
  var postgresMan = self.postgresMan;

  postgresMan.on('pg_err', function(err) {
    log.fatal('pg health check failed, shutting down', err);
    process.exit(err);
  });

  self.on('xlog_err', function(err) {
    log.fatal('writing xlog failed, shutting down', err);
    process.exit(err);
  });

  shard.on('primary', function(standbys) {
    // stop healthcheck before any restarts
    postgresMan.stopHealthCheck(function(err) {

      postgresMan.primary(standbys, function(err) {
        if (err) {
          log.fatal('unable to start as primary with err', err);
          process.exit(err);
        }
        // start health check
        postgresMan.startHealthCheck(function() {
          if (err) {
            log.fatal('unable to start healthcheck', err);
            process.exit(err);
          }

          writeRegistrar(self, standbys, function(err) {
            if (err) {
              log.fatal(err);
              process.exit(err);
            }
            // primary transition elicits a snapshot - we need to snapshot
            // in case standbys try and recover from the primary as it's unclear
            // when the last snapshot on this peer was.
            self.snapShotter.createSnapshot(function(err) {
              if (err && err.snapshotErr) {
                log.fatal(err);
                process.exit(err);
              }
            });
          });
        });
      });
    });
  });

  shard.on('standby-change', function(standbys) {
    log.info('standby changed');
    // stop healthcheck before any restarts
    postgresMan.stopHealthCheck(function() {

      postgresMan.primary(standbys, function(err) {
        if (err) {
          log.fatal('unable to start as primary with err', err);
          process.exit(err);
        }

        // start health check
        postgresMan.startHealthCheck(function(err) {
          if (err) {
            log.fatal('unable to start healthcheck', err);
            process.exit(err);
          }

          writeRegistrar(self, standbys, function(err) {
            if (err) {
              log.fatal(err);
              process.exit(err);
            }
          });
        });
      });
    });
  });

  shard.on('standby', function(shardInfo) {
    // check if db dir exists
    // if exists, start and slave
    // if slave fails, restore slave
    // continue monitoring slave status. If unable to slave, then restore

    // stop healthcheck before any restarts
    postgresMan.stopHealthCheck(function(err) {

      postgresMan.standby(shardInfo.primary, shardInfo.backupUrl, function(err) {
        if (err) {
          log.fatal('unable to start as standby with err', err);
          process.exit(err);
        }

        postgresMan.startHealthCheck(function(err) {
          if (err) {
            log.fatal('unable to start healthcheck', err);
            process.exit(err);
          }
        });
      });

    });
  });

  shard.on('primary-change', function(shardInfo) {
    // stop healthcheck before any restarts
    // same as standby
    postgresMan.stopHealthCheck(function() {

      // TODO: check for if shardinfo DNE
      postgresMan.standby(shardInfo.primary, shardInfo.backupUrl, function(err) {
        if (err) {
          log.fatal('unable to start as standby with err', err);
          process.exit(err);
        }
        postgresMan.startHealthCheck(function(err) {
          if (err) {
            log.fatal('unable to start healthcheck', err);
            process.exit(err);
          }
        });
      });
    });
  });

  shard.init();
}

function writeRegistrar(self, standbys, callback) {
  var log = self.log;
  var shard = self.shard;
  log.info('writing registrar', standbys, callback);
  // write registrarInfo
  var shardInfo =  {
    primary: self.postgresMan.url
  }

  if (standbys[0]) {
    shardInfo.sync = standbys[0];
  }

  if (standbys[1]) {
    shardInfo.async = standbys[1];
  }
  shardInfo.backupUrl = self.backupUrl;

  shard.writeRegistrar(shardInfo, function(err) {
    if (err) {
      log.error('unable to start as primary with err', err);
    }

    return callback(err);
  });
}

function startXlogUpdate(self, callback) {
  var log = self.log;
  log.info('starting periodic xlog update with period', self.xlogInterval);
  self.xlogIntervalId = setInterval(function() {
    updateXlogLocation(self);
  }, self.xlogInterval);

  return callback();
}

function stopXlogUpdate(self, callback) {
  var log = self.log;
  log.info('stopping xlog update');
  if (self.xlogIntervalId) {
    clearInterval(self.xlogIntervalId);
    self.xlogIntervalId = null;
  }

  return callback();
}

/**
 * Updates the current xlog location in ZK. Note this is only executed on the
 * primary
 */
function updateXlogLocation(self, callback) {
  var log = self.log;
  log.info('updating xlog location');
  // get the xlog
  self.postgresMan.xlogLocation(function(err, xlog) {
    if (err) {
      log.error('unable to get xlog location', err);
      self.emit('xlog_err', err);
    }

    if (callback && err) {
      return callback(err);
    }

    xlog = JSON.stringify(xlog.rows[0]);
    var prevXlogLocation = self.xlogLocation;
    self.xlogLocation = xlog;
    if (prevXlogLocation === xlog) {
      log.debug('xlog location not changed, skipping write to zk',
               xlog, prevXlogLocation);
      if (callback) {
        return callback();
      }
    } else {
      log.info('writing new xlog');
      self.shard.writeXlog(xlog, function(err) {
        if (err) {
          log.error('unable to write xlog location', err);
          self.emit('xlog_err', err);
        }

        self.emit('xlog');
        if (callback) {
          return callback(err);
        }
      });
    }
  });
}

