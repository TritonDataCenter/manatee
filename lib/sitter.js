var EventEmitter = require('events').EventEmitter;
var PostgresMan = require('./postgresMan');
var SnapShotter = require('./snapShotter');
var Shard = require('./shard');
var assert = require('assert-plus');
var Shard = require('./shard');
var util = require('util');

/**
 * The postgres sitter, keeps track of a postgres instance's role within a shard
 * This is the coordinator that manages interactions between zookeeper and
 * postgres.
 */
function Sitter(options) {
  assert.object(options, 'options');
  assert.object(options.log, 'options.log');
  assert.object(options.shardCfg, 'options.shardCfg');
  assert.object(options.postgresManCfg, 'options.postgresManCfg');
  assert.object(options.snapShotterCfg, 'options.snapShotterCfg');
  assert.string(options.backupUrl, 'options.backupUrl');
  assert.number(options.ttl, 'options.ttl');

  EventEmitter.call(this);
  this.log = options.log;

  this.log.info('new Sitter with options', options);

  this.shard = new Shard(options.shardCfg);
  this.postgresMan = new PostgresMan(options.postgresManCfg);
  this.snapShotter = new SnapShotter(options.snapShotterCfg);
  this.backupUrl = options.backupUrl;
  this.ttl = options.ttl;
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
    log.fatal({err: err}, 'pg health check failed, shutting down');
    process.exit(err);
  });

  // Transition to primary status
  shard.on('primary', function(standbys) {
    // stop healthcheck before any restarts
    postgresMan.stopHealthCheck(function(err) {

      postgresMan.primary(standbys, function(err) {
        if (err) {
          log.fatal({err: err}, 'unable to start as primary with err');
          process.exit(err);
        }
        // start health check
        postgresMan.startHealthCheck(function() {
          if (err) {
            log.fatal({err: err}, 'unable to start healthcheck');
            process.exit(err);
          }

          writeRegistrar(self, standbys, function(err) {
            if (err) {
              log.fatal({err: err}, 'unable to write registrar');
              process.exit(err);
            }
            // primary transition elicits a snapshot - we need to snapshot
            // in case standbys try and recover from the primary as it's unclear
            // when the last snapshot on this peer was created.
            self.snapShotter.createSnapshot(function(err) {
              if (err && err.snapshotErr) {
                log.fatal({err: err},
                          'unable to create snapshot on primary transition');
                process.exit(err);
              }
            });
          });
        });
      });
    });
  });

  // Standbys in shard have changed.
  shard.on('standby-change', function(standbys) {
    log.info('standby changed');
    // stop healthcheck before any restarts
    postgresMan.stopHealthCheck(function() {

      postgresMan.primary(standbys, function(err) {
        if (err) {
          log.fatal({err: err}, 'unable to start as primary with err');
          process.exit(err);
        }

        // start health check
        postgresMan.startHealthCheck(function(err) {
          if (err) {
            log.fatal({err: err}, 'unable to start healthcheck');
            process.exit(err);
          }

          writeRegistrar(self, standbys, function(err) {
            if (err) {
              log.fatal({err: err}, 'unable to write registrar');
              process.exit(err);
            }
          });
        });
      });
    });
  });

  // Transition to standby status
  shard.on('standby', function(shardInfo) {
    // stop healthcheck before any restarts
    postgresMan.stopHealthCheck(function(err) {

      postgresMan.standby(shardInfo.primary,
                          shardInfo.backupUrl, function(err) {
        if (err) {
          log.fatal({err: err}, 'unable to start as standby with err');
          process.exit(err);
        }

        postgresMan.startHealthCheck(function(err) {
          if (err) {
            log.fatal({err: err}, 'unable to start healthcheck');
            process.exit(err);
          }
        });
      });
    });
  });

  // Primary in shard has changed
  shard.on('primary-change', function(shardInfo) {
    // stop healthcheck before any restarts
    postgresMan.stopHealthCheck(function() {

      postgresMan.standby(shardInfo.primary,
                          shardInfo.backupUrl, function(err) {
        if (err) {
          log.fatal({err: err}, 'unable to start as standby with err');
          process.exit(err);
        }
        postgresMan.startHealthCheck(function(err) {
          if (err) {
            log.fatal({err: err}, 'unable to start healthcheck');
            process.exit(err);
          }
        });
      });
    });
  });

  shard.init();
};

/**
 * Write the status of the shard to zookeeper, this is only invoked on primary
 * transitions.
 * @param String[] standbys The list of standby peers in the shard.
 */
function writeRegistrar(self, standbys, callback) {
  var log = self.log;
  var shard = self.shard;
  log.info('writing registrar', standbys);
  // write registrarInfo
  var shardInfo = {
    type: 'database',
    database: {
      primary: self.postgresMan.url
    },
    ttl: self.ttl
  };

  if (standbys[0]) {
    shardInfo.database.sync = standbys[0];
  }

  if (standbys[1]) {
    shardInfo.database.async = standbys[1];
  }

  shardInfo.database.backupUrl = self.backupUrl;

  shard.writeRegistrar(shardInfo, function(err) {
    return callback(err);
  });
}
