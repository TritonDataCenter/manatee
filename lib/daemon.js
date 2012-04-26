var confparser = require('./confParser');
var common = require('./common');
var fs = require('fs');
var pg = require('pg');
var PostgresMan = require('./postgresMan');
var Shard = require('./shard');
var sprintf = require('util').format;
var url = require('url');

var SYNCHRONOUS_STANDBY_NAMES = 'synchronous_standby_names';
var SYNCHRONOUS_COMMIT = 'synchronous_commit';
var PRIMARY_CONNINFO = 'primary_conninfo';
var DEFAULT_TRANSACTION_READ_ONLY = 'default_transaction_read_only';

var MODE = {
  PRIMARY: 0,
  STANDBY: 1,
  READONLY: 2
};

/**
 * TODO: Need to add messamging mechanism to communicate with postgres.
 * @constructor
 * @param {object} options The constructor options.
 */
function Daemon(options) {
  this.log = options.log;

  this.log.debug('new daemon with', options);

  this.shard = null;

  this.postgresMan = new PostgresMan(options.postgresCfg);

  /**
   * Url of the postgres instance, postgresql:\\ip:port
   */
  this.url = options.url;

  /**
   * the application_name of this postgres instance, maps to
   * the shard.role of this peer.
   */
  this.appName = null;

  this.zkCfg = options.zkCfg;

  this.shardId = options.shardId;

  /**
   * The znode in zk under which registrar information is stored.
   * It's assumed this znode exists
   */
  this.registrarPath = options.registrarPath;

  /**
   * path to recovery.conf
   */
  this.recoveryPath = options.recoveryPath;

  /**
   * path to postgresql.conf
   */
  this.postgresqlPath = options.postgresqlPath;

  this.recoveryTemplate = options.recoveryTemplate;

  this.postgresqlTemplate = options.postgresqlTemplate;

  /**
   * Enter read only mode when there's only 1 peer
   */
  this.readOnly = options.readOnly || false;

  /**
   * The current mode of the postgres instance. one of:
   * readonly, standby, primary
   */
  this.mode = null;
}

/**
 * Initializes the Postgres daemon.
 * @param {function} callback The callback of the form f(err).
 */
Daemon.prototype.init = function init(callback) {
  var self = this;
  var log = self.log;
  log.info('initializing failover daemon');
  // acquire a shard

  self.shard = new Shard('/' + self.shardId, self.registrarPath,
                         self.zkCfg, self.url, log);

  self.shard.on('init', function(shard) {
    // determine state
    // if >= 2 peers, start, else go to readonly mode.
    // if >= 2 peers and is primary, start
    // if standby, start
    log.info('shard initialized, transitioning daemon state');
    return self.transition(callback);
  });

  //TODO: unclear what to do for error handling here
  self.shard.on('error', function(err) {
    var shardErr = err;
    log.error('got error from shard %j, going to readonly mode', err);
    // goto readonly mode
    self.readOnly(function(err) {
      if (err) {
        return callback(err);
        // TODO: Shutdown postgres
      }
      // disconnect
      self.shard.disconnect();
      return callback(shardErr);
      // try and re init
    });
  });

  self.shard.init();
};

/**
 * Error handler when there are errors with the shard or postgres. If there
 * is an error, safely shuts down postgres before shutting down the daemon.
 * @param {object} err An error object.
 * @param {function} callback The callback of the form f().
 */
Daemon.prototype.checkErr = function checkErr(err, callback) {
  var self = this;
  var log = self.log;
  var postgresMan = self.postgresMan;
  if (err) {
    log.fatal('shutting down daemon and postgres due to error', err);
    postgresMan.shutdown(function(method, err) {
      if (err) {
        log.error('error while shutting down postgres', err);
      }
      log.info('shutdown postgres with method %s ', method);
      process.exit(1);
    });
  } else {
    return callback();
  }
};

/**
 * Determines the role of the current peer within the postgres shard.
 * @param {function} callback The callback of the form f(err).
 */
Daemon.prototype.transition = function transition(callback) {
  var self = this;
  var log = self.log;

  log.debug('entering transition');

  switch (self.shard.role) {
   case common.ROLES.PRIMARY:
      self.appName = 'primary';
      self.primary(callback);
      break;
    case common.ROLES.SYNC:
      self.appName = 'sync';
      self.standby(callback);
      break;
    case common.ROLES.ASYNC:
      self.appName = 'async';
      self.standby(callback);
      break;
  }
};

/**
 * @param {function} callback The callback of the form f(err).
 */
Daemon.prototype.standby = function standby(callback) {
  //TODO: Currently this assumes that the db has already been backed up from the
  //primary. We'll need to actually backup before the transistion

  var self = this;
  var log = self.log;
  var shard = self.shard;
  log.info('in standby mode');

  log.debug('primary url', shard.primary.url);
  var primaryUrl = url.parse(shard.primary.url);
  // update primary_conninfo to point to the new host, port pair
  self.mode = MODE.STANDBY;
  updatePrimaryConnInfo(function(err) {
    // check for error and restart postgres
    return self.checkErr(err, function() {
      return self.postgresMan.restart(callback);
    });
  });

  function updatePrimaryConnInfo(callback) {
    confparser.read(self.recoveryTemplate, function(err, conf) {
      if (err) {
        return callback(err);
      }
      var value = sprintf('\'host=%s port=%s user=%s application_name=%s\'',
                          primaryUrl.hostname, primaryUrl.port,
                          primaryUrl.auth, self.url);

      confparser.set(conf, PRIMARY_CONNINFO, value);
      log.info('updating primary conn info %s', value);
      return confparser.write(self.recoveryPath, conf, callback(err));
    });
  };
};

/**
 * @param {function} callback The callback of the form f(err).
 */
Daemon.prototype.primary = function primary(callback) {
  var self = this;
  var log = self.log;

  log.debug('entering transition to primary, updating mode to 0');
  self.mode = MODE.PRIMARY;

  // initialize the db, note it's always safe to do so, as initdb will not
  // overwrite a pre-existing DB

  log.debug('initializing DB');
  /**
   * The steps for primary transition is as follows:
   * 1) Try and initialize the db
   * 2) If db init was successful, this means that there was no db before.
   *    Start the db with vanilla configs (syncronous_commit = local) and
   *    create any dbs.
   * 3) Update postgresql.conf with standby names, and set syncronous_commit=on
   * 4) Restart db
   */
  self.postgresMan.initDb(function(err) {
    self.checkErr(err, function(){
      // if primary, delete recovery.conf
      self.deleteFile(self.recoveryPath, function(err) {
        if (err) {
          return callback(err);
        }
        // if there is no sync peer, then it would appear that we are alone.
        // move to readonly mode if configured. TODO: Make readonly mode configurable
        if (!self.shard.sync && self.readOnly) {
          log.error('only 1 peer in shard, moving to readonly mode');
          return self.readOnly(callback);
        } else {
          // update conf to reflect sync and async standbys and restart postgres
          return updateStandbys(function(err) {
            self.checkErr(err, function() {
              return self.postgresMan.restart(callback);
            });
          });
        }
      });

      // Updates synchronous_standby_names to the new ones in ZK
      // Note: We conly update the synchronous name such that the async doesn't
      // get auto promoted to sync
      var updateStandbys = function updateStandbys(callback) {
        confparser.read(self.postgresqlTemplate, function(err, conf) {;
          log.debug('default conf values', conf);
          if (err) {
            return callback(err);
          }
          // update standby list, note shard.async and shard.async may not exist
          var value = '\'';
          if (self.shard.sync) {
            value += self.shard.sync.url;
          }
          //if (self.shard.async) {
            //value += ', ' + self.shard.async.url;
          //}
          value += '\'';
          confparser.set(conf, SYNCHRONOUS_STANDBY_NAMES, value);
          log.info('updating postgresql config %s with new standby names %s',
            self.postgresqlPath, value);
          confparser.write(self.postgresqlPath, conf, function(err) {
            return callback(err);
          });
        });
      };
    });
  });

};

/**
 * @param {function} callback The callback of the form f(err).
 */
Daemon.prototype.readOnly = function readOnly(callback) {
  var self = this;
  var log = self.log;
  log.info('transitioning to readonly mode');
  self.mode = MODE.READONLY;

  // update default_transaction_read_only to on for readonly mode
  confparser.read(self.postgresqlTemplate, function(err, conf) {
    if (err) {
      return callback(err);
    }

    confparser.set(conf, DEFAULT_TRANSACTION_READ_ONLY, 'on');

    log.info('updating postgresql config %s to read only',
              self.postgresqlPath);

    confparser.write(self.postgresqlPath, conf, function(err) {
      // check for error, if there is an error, shutdown and exit
      self.checkErr(err, function() {
        // restart postgres
        return self.postgresMan.restart(callback);
      });
    });
  });
};

/**
 * deletes a file from disk
 * @param {String} path The path of the file on disk.
 * @param {function} callback The callback in the form f(err).
 */
Daemon.prototype.deleteFile = function deleteFile(path, callback) {
  var self = this;
  var log = self.log;
  log.debug('entering delete with %s', path, callback);
  fs.stat(path, function(err, stats) {
    // if file exists, delete it
    if (stats) {
      fs.unlink(path, function(err, stats) {
        if (err) {
          return callback(err);
        }
        log.debug('deleted file', path);
        return callback();
      });
    } else {
      log.debug('file doesnt exist', path);
      return callback();
    }
  });
};

/**
 * Failover Daemon
 */
module.exports = Daemon;

