var assert = require('assert-plus');
var BackupClient = require('./backupClient');
var ConfParser = require('../lib/confParser');
var EventEmitter = require('events').EventEmitter;
var fs = require('fs');
var pg = require('pg');
var Client = pg.Client;
var shelljs = require('shelljs');
var spawn = require('child_process').spawn;
var sprintf = require('util').format;
var url = require('url');
var util = require('util');

/**
 * postgresql.conf keys
 */
var SYNCHRONOUS_STANDBY_NAMES = 'synchronous_standby_names';
var SYNCHRONOUS_COMMIT = 'synchronous_commit';
var PRIMARY_CONNINFO = 'primary_conninfo';

/**
 * The postgres manager which manages interactions with postgres.
 * Responsible for initializing, starting, stopping, and health checking a
 * running postgres instance.
 */
function PostgresMan(options) {
  assert.object(options, 'options');
  assert.object(options.log, 'options.log');
  assert.string(options.dataDir, 'options.dataDir');
  assert.string(options.postgresPath, 'options.postgresPath');
  assert.string(options.pgInitDbPath, 'options.pgInitDbPath');
  assert.string(options.hbaConf, 'options.hbaConf');
  assert.string(options.postgresConf, 'options.postgresConf');
  assert.string(options.recoveryConf, 'options.recoveryConf');
  assert.string(options.url, 'options.url');
  assert.string(options.dbUser, 'options.dbUser');
  assert.object(options.backupClientCfg, 'options.backupClientCfg');
  assert.number(options.healthChkInterval, 'options.healthChkInterval');
  assert.number(options.opsTimeout, 'options.opsTimeout');

  EventEmitter.call(this);

  this.log = options.log;
  var log = this.log;
  /**
   * The child postgres process
   */
  this.postgres = null;

  /**
   * The dir on disk where the postgres instance is located
   */
  this.dataDir = options.dataDir;

  /**
   * paths to the postgres commands
   */
  this.pgInitDbPath = options.pgInitDbPath;
  this.postgresPath = options.postgresPath;

  /**
   * Paths to the pg configs
   */
  this.hbaConf = options.hbaConf;
  this.postgresConf = options.postgresConf;
  this.recoveryConf = options.recoveryConf;
  this.hbaConfPath = this.dataDir + '/' + 'pg_hba.conf';
  this.postgresConfPath = this.dataDir + '/' + 'postgresql.conf';
  this.recoveryConfPath = this.dataDir + '/' + 'recovery.conf';

  /**
   * the url of this postgers instance
   */
  this.url = options.url;

  /**
   * The postgres user
   */
  this.dbUser = options.dbUser;

  /**
   * Cfg for the backup client
   */
  this.backupClientCfg = options.backupClientCfg;

  /**
   * The health check interval in ms
   */
  this.healthChkInterval = options.healthChkInterval;
  this.healthChkIntervalId = null;

  /**
   * Postgres operation timeout. Any postgres operation will fail upon exceeding
   * this timeout.
   */
  this.opsTimeout = options.opsTimeout;

  log.info('new postgres man with options', options);
}

module.exports = PostgresMan;
util.inherits(PostgresMan, EventEmitter);

/**
 * Initializes the postgres data directory for a new DB. This can fail if the
 * db has already been initialized - this is okay, as startdb will fail if init
 * didn't finish succesfully.
 *
 * This function should only be called by the primary of the shard. Standbys
 * will not need to initialize but rather restore from a already running
 * primary.
 * @param {function} callback The callback of the form f(err).
 */
PostgresMan.prototype.initDb = function initDb(self, callback) {
  var log = self.log;
  var msg = '';
  log.info('initializing postgres instance with path %s', self.dataDir);
  shelljs.mkdir('-p', self.dataDir);
  var postgres = spawn(self.pgInitDbPath, ['-D', self.dataDir]);

  postgres.stdout.on('data', function(data) {
    log.debug('postgres stdout: ', data.toString());
  });

  postgres.stderr.on('data', function(data) {
    var dataStr = data.toString();
    log.warn('postgres stderr: ', dataStr);
    if (msg) {
      msg += dataStr;
    } else {
      msg = dataStr;
    }
    msg += data;
  });

  postgres.on('exit', function(code) {
    var err;
    if (code !== 0) {
      err = {
        msg: msg,
        code: code
      };
      log.warn('unable to initDb postgres with err: ', err);
    }

    log.debug('copying pg_hba.conf to data dir');
    shelljs.cp('-f', self.hbaConf, self.dataDir + '/pg_hba.conf');
    log.debug('copying postgresql.conf to data dir');
    shelljs.cp('-f', self.postgresConf, self.dataDir + '/postgresql.conf');

    /*
     * if there's no error, then the db is freshly initialized start the db
     */
    if (!err) {
      log.debug('starting db after init');
      self.start(self, function(err, postgres) {
        if (err) {
          log.error('unable to start db after init with err', err);
          return callback(err);
        }

        return callback();
      });
    } else {
      /*
       * Note the initdb error is never passed back on initDB due to the fact
       * that initdb might fail because it's already been intialized and there's
       * no way of distinguishing for now. self is okay as startdb will err
       * if the db is not initialized.
       */
      log.warn('error during initDb, continuing with init process', err);
      return callback();
    }
  });
};

/**
 * Transition the postgres instance to primary mode.
 * @param String[] The list of standbys in the shard.
 * @param function callback The callback of the form f(err).
 */
PostgresMan.prototype.primary = function primary(standbys, callback) {
  var self = this;
  var log = self.log;

  log.info('Changing postgres mode to primary.');

  /**
  * The steps for primary transition is as follows:
  * 1) Try and initialize the db.
  * 2) If db init was successful, this means that there was no db before.
  *    Start the db with vanilla configs (synchronous_commit = local) and
  *    create any dbs.
  * 3) Update postgresql.conf with standby names, and set syncronous_commit=on
  * 4) Restart db
  */

  // 0,1) initialize the db, note it's always safe to do so, as initdb will not
  // overwrite a pre-existing DB
  self.initDb(self, function(err) {
    if (err) {
      return callback(err);
    }
    // 2) delete recovery.conf
    deleteFile(self, self.recoveryConfPath, function() {
      // if there is no sync peer, then it would appear that we are alone.
      // move to readonly mode if configured
      if (standbys.length === 0 && self.readOnly) {
        log.warn('only 1 peer in shard, moving to readonly mode');
        //TODO: add readonly logic - currently this is not implemented.
      } else {
        /*
         * NOTE: PG does not block writes if standbyslist is empty. Hence
         * writes can still occur. By default this is what we want, to always
         * take writes.
         *
         * 3) update conf to reflect sync and async standbys and restart pg
         */
        return updateStandbySetSyncCommit(self, standbys, function(err) {
          if (err) {
            return callback(err);
          }
          // 4)
          restart(self, function(err) {
            return callback(err);
          });
        });
      }
    });
  });

  // Updates synchronous_standby_names and sets synchronous_commit=on
  function updateStandbySetSyncCommit(self, standbys, callback) {
    var log = self.log;
    log.info('updating standby names in conf', standbys);
    ConfParser.read(self.postgresConf, function(err, conf) {
      log.debug('default conf values', conf);
      if (err) {
        return callback(err);
      }
      // update standby list
      var value = '\'';
      standbys.forEach(function(standby, index) {
        if (index > 0) {
          value += ', ';
        }
        value += standby;
      });
      value += '\'';
      ConfParser.set(conf, SYNCHRONOUS_STANDBY_NAMES, value);
      // set synchronous_commit = on;
      ConfParser.set(conf, SYNCHRONOUS_COMMIT, 'on');

      log.info('updating postgresql config %s with new standby names %s',
        self.postgresConfPath, value);
      log.info('writing configs %j to %s', conf, self.postgresConf);
      ConfParser.write(self.postgresConfPath, conf, function(err) {
        return callback(err);
      });
    });
  }
};

/**
 * Transitions a postgres instance to standby state.
 * @param {string} primaryUrl The postgres url of the primary.
 * @param {string} backupUrl The http url of the primary's backup service.
 * @param {function} callback The callback of the form f(err).
 */
PostgresMan.prototype.standby = function standby(primaryUrl,
                                                 backupUrl, callback)
{
  //TODO: Need to figure out when there's insufficient xlogs in the cache to
  //perform a full restore
  var self = this;
  var log = self.log;
  log.info('going to standby mode', primaryUrl, backupUrl);

  primaryUrl = url.parse(primaryUrl);
  // update primary_conninfo to point to the new (host, port) pair
  updatePrimaryConnInfo(function(err) {
    // check for error and restart postgres
    if (err) {
      return callback(err);
    }

    // If you can't restart, this indicates that a restore is needed
    // from the primary, so do that
    return restart(self, function(err) {
      if (err) {
        restore(self, backupUrl, function(err) {
          if (err) {
            return callback(err);
          }
          // updatePrimaryConnInfo again as the backup wiped it out
          updatePrimaryConnInfo(function(err) {
            if (err) {
              return callback(err);
            }
            return restart(self, callback);
          });
        });
      } else {
        return callback();
      }
    });
  });

  /**
   * Update the primary connection info in postgresql.conf
   */
  function updatePrimaryConnInfo(callback) {
    ConfParser.read(self.recoveryConf, function(err, conf) {
      if (err) {
        return callback(err);
      }
      var value = sprintf('\'host=%s port=%s user=%s application_name=%s\'',
                          primaryUrl.hostname, primaryUrl.port,
                          primaryUrl.auth, self.url);

      ConfParser.set(conf, PRIMARY_CONNINFO, value);
      log.info('updating primary conn info %s', value);
      return ConfParser.write(self.recoveryConfPath, conf, callback(err));
    });
  }

  /**
   * Restores the current postgres instance from the primary via zfs_recv.
   */
  function restore(self, backupUrl, callback) {
    var log = self.log;
    log.info('restoring db from primary', backupUrl);
    var cfg = self.backupClientCfg;
    cfg.serverUrl = backupUrl;
    var client = new BackupClient(cfg);

    client.restore(function(err) {
      if (err) {
        log.error('backup failed');
        return callback(err);
      }
      log.info('finished backup, chowning datadir');
      updateOwner(self, function(err) {
        return callback(err);
      });
    });
  }
};

/**
 * Start the postgres instance.
 * @param {function} callback The callback of the form f(err, process).
 */
PostgresMan.prototype.start = function start(self, callback) {
  var log = self.log;
  var msg = '';
  log.info('spawning postgres', self.postgresPath, self.dataDir);

  var postgres = spawn(self.postgresPath, ['-D', self.dataDir]);
  self.postgres = postgres;
  var time = new Date().getTime();

  postgres.on('exit', function(code, signal) {
    var err;
    if (code !== 0) {
      err = {
        msg: msg,
        code: code
      };
    }

    log.info('postgres exited with', err, code, signal);
    self.emit('shutdown', err);
  });

  var intervalId;
  checkHealth();

  function checkHealth() {
    intervalId = setInterval(function() {
      health(self, function(err) {
        if (err) {
          var timeSinceStart = new Date().getTime() - time;
          log.info('db has not started', err, timeSinceStart, self.opsTimeout);
          if (timeSinceStart > self.opsTimeout) {
            log.fatal('timeout on start db', timeSinceStart);
            clearInterval(intervalId);
            return callback(err, postgres);
          }
        } else {
          log.info('db has started');
          clearInterval(intervalId);
          return callback(null, postgres);
        }
      });
    }, 1000);
  }
};

/**
 * Starts the periodic health checking of the pg instance.
 * emits pg_err if healthchk fails
 */
PostgresMan.prototype.startHealthCheck = function startHealthCheck(callback) {
  var self = this;
  var log = self.log;
  log.info('starting healthcheck');

  if (self.healthChkIntervalId) {
    log.warn('health check already running');
    return callback('health check already running');
  } else {
    self.healthChkIntervalId = setInterval(function() {
      health(self, function(err) {
        if (err) {
          self.emit('pg_err', err);
        }
      });
    }, self.healthChkInterval);
    return callback();
  }

};

PostgresMan.prototype.stopHealthCheck = function stopHealthCheck(callback) {
  var self = this;
  var log = self.log;
  log.info('stopping healthcheck', self.healthChkIntervalId);

  if (self.healthChkIntervalId) {
    clearInterval(self.healthChkIntervalId);
    self.healthChkIntervalId = null;
  } else {
    log.warn('healthcheck not running');
  }
  return callback();
};

/**
 * Update the owner of the the pg data dir back to the postgres user.
 * ZFS receive operations resets the dir back to root ownership. pg does not
 * allow root ownership on a pg data dir.
 */
function updateOwner(self, callback) {
  var log = self.log;
  log.info('chowning the datadir to current user', self.user, self.dataDir);
  var msg = '';
  var chown = spawn('pfexec', ['chown', '-R', self.dbUser, self.dataDir]);

  chown.stdout.on('data', function(data) {
    log.debug('chown stdout: ', data.toString());
  });

  chown.stderr.on('data', function(data) {
    var dataStr = data.toString();
    log.error('chown stderr: ', dataStr);
    if (msg) {
      msg += dataStr;
    } else {
      msg = dataStr;
    }
    msg += data;
  });

  chown.on('exit', function(code) {
    var err;
    if (code !== 0) {
      err = {
        msg: msg,
        code: code
      };
      log.error('unable to chown datadir with err: ', err);
    }

    callback(err);
  });
}

/**
 * deletes a file from disk
 * @param {String} path The path of the file on disk.
 * @param {function} callback The callback in the form f(err).
 */
function deleteFile(self, path, callback) {
  var log = self.log;
  log.info('deleting file', path);
  shelljs.rm(path);
  return callback();
}

function queryDb(self, query, callback) {
  var log = self.log;
  log.trace('entering querydb %s', query);

  var client = new Client(self.url);
  client.connect(function(err) {
    if (err) {
      log.warn('can\'t connect to pg with err', err);
      client.end();
      return callback(err);
    }
    log.trace('connected to pg, running query %s', query);
    client.query(query, function(err, result) {
      client.end();
      if (err) {
        log.warn('error whilst querying pg ', err);
      }
      return callback(err, result);
    });
  });
}

/**
 * Restarts the postgres instance. If no pg instance is running, this will just
 * start pg.
 * @param {function} callback The callback of the form f(err).
 */
function restart(self, callback) {
  var log = self.log;
  log.info('restarting postgres');

  // check health first to see if db is running
  health(self, function(err) {
    if (err) {
      log.info('db not running');
      return self.start(self, callback);
    } else {
      log.info('db is running');
      self.stop(self, self.postgres, function(err) {
        if (err) {
          return callback(err);
        }
        return self.start(self, callback);
      });
    }
  });
}

/**
 * stops the running postgres instance.
 * @param {function} callback The callback of the form f(err).
 *
 * SIGTERM, SIGINT, SIGQUIT, SIGKILL
 * The first will wait for all clients to terminate before quitting, the second
 * will forcefully disconnect all clients, and the third will quit immediately
 * without proper shutdown, resulting in a recovery run during restart.
 */
PostgresMan.prototype.stop = function stop(self, postgres, callback) {
  var log = self.log;
  log.info('shutting down postgres');

  var successful;
  postgres.on('exit', function(code, signal) {
    log.info('postgres exited with', code, signal);
    successful = true;
    return callback();
  });

  postgres.kill('SIGINT');
  // simply wait opsTimeout before executing SIGQUIT
  setTimeout(function() {
    if (!successful) {
      log.warn('could not shutdown pg with SIGINT before timeout SIGQUIT');
      postgres.kill('SIGQUIT');
    }

    // set another timeout and SIGKILL
    setTimeout(function() {
      if (!successful) {
        log.warn('could not shutdown pg with SIGTERM before timeout SIGKILL');
        postgres.kill('SIGKILL');
      }
      // set another timeout and return error
      setTimeout(function() {
        if (!successful) {
          log.error('could not shutdown pg with SIGKILL');
          return callback('error');
        }
      });
    }, self.opsTimeout);

  }, self.opsTimeout);
};

/**
 * Check the health status of the running postgres db.
 * @param {function} callback The callback of the form f(err), where err
 * indicates an unhealthy db.
 */
function health(self, callback) {
  var log = self.log;
  log.trace('pgsql health check');
  queryDb(self, 'select now() as when', function(err) {
    if (err) {
      log.warn('pg health check failed');
    }
    if (callback) {
      return callback(err);
    }
  });
}
