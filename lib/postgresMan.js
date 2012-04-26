var ConfParser = require('../lib/confParser');
var shelljs = require('shelljs');
var spawn = require('child_process').spawn;
var util = require('util');
var sprintf = util.format;

var STATUS = {
  UP: 0,
  DOWN: 1,
  ERROR: -1
};

function PostgresMan(options) {
  this.log = options.log;
  var log = this.log;
  log.info('new PostgresMan with options', options);
  /**
   * The child postgres process
   */
  this.postgres = null;
  this.dataDir = options.dataDir;
  this.logFile = options.logFile;
  this.pgCtlPath = options.pgCtlPath;
  this.pgCreateDbPath = options.pgCreateDbPath || 'createdb';
  this.pgInitDbPath = options.pgInitDbPath;
  this.pgHbaPath = options.pgHbaPath;
  this.dbName = options.dbName;

  log.info('PostgreMan initialized', this);
}
module.exports = PostgresMan;

/**
 * initializes the postgres data directory for a new DB.
 * @param {function} callback The callback of the form f(err).
 */
PostgresMan.prototype.initDb = function initDb(callback) {
  var log = this.log;
  var msg = '';
  var self = this;
  log.info('initializing postgres instance with path %s', this.dataDir);
  shelljs.mkdir('-p', this.dataDir);

  var postgres = spawn(this.pgInitDbPath, ['-D', this.dataDir]);

  postgres.stdout.on('data', function(data) {
    log.debug('postgres stdout: ', data.toString());
  });

  postgres.stderr.on('data', function(data) {
    var dataStr = data.toString();
    log.error('postgres stderr: ', dataStr);
    if (msg) {
      msg += dataStr;
    } else {
      msg = dataStr;
    }
    msg += data;
  });

  postgres.on('exit', function(code) {
    var err;
    if (code != 0) {
      err = {
        msg: msg,
        code: code
      };
      log.error('unable to initDb postgres with err: ', err);
    }

    log.debug('copying pg_hba.conf to data dir');
    shelljs.cp('-f', self.pgHbaPath, self.dataDir + '/pg_hba.conf');

    /*
     * if there's no error, this means that the db is freshly initialized,
     * create the db
     */
    if (!err) {
      self.start(function(err) {
        if (err) {
          return callback(err);
        }
        // We may want to shutdown the DB after we create the db because
        // due to the fact that synchronous_commit = local and not = on.
        // A restart is insufficient to reset this parameter.
        self.createDb(self.dbName, function(err) {
          if (err) {
            log.error('unable to create db', err);
            return callback(err);
          }
          return self.shutdown(callback);
        });
      })
    } else {
      // Note the initdb error is never passed back on initDB due to the fact that
      // initdb might fail because it's already been intialized and there's
      // no way of distinguishing for now. This is okay as startdb will err
      // if the db is not initialized.
      callback();
    }
  });
};

PostgresMan.prototype.createDb = function createDb(name, callback) {
  var log = this.log;
  var msg = '';
  log.info('creating db');
  var postgres = spawn(this.pgCreateDbPath, [name]);

  postgres.stdout.on('data', function(data) {
    log.debug('postgres stdout: ', data.toString());
  });

  postgres.stderr.on('data', function(data) {
    var dataStr = data.toString();
    log.error('postgres stderr: ', dataStr);
    if (msg) {
      msg += dataStr;
    } else {
      msg = dataStr;
    }
    msg += data;
  });

  postgres.on('exit', function(code) {
    var err;
    if (code != 0) {
      err = {
        msg: msg,
        code: code
      };
      log.error('unable to createdb with err: ', err);
    }

    callback(err);
  });
};

/**
 * Start a postgres instance.
 * @param {function} callback The callback of the form f(err).
 */
PostgresMan.prototype.start = function start(callback) {
  var log = this.log;
  var msg = '';
  log.info('spawning postgres');
  var postgres = spawn(this.pgCtlPath,
   ['-w', '-D', this.dataDir, '-l', this.logFile, 'start']);

  postgres.stdout.on('data', function(data) {
    log.debug('postgres stdout: ', data.toString());
  });

  postgres.stderr.on('data', function(data) {
    var dataStr = data.toString();
    log.error('postgres stderr: ', dataStr);
    if (msg) {
      msg += dataStr;
    } else {
      msg = dataStr;
    }
    msg += data;
  });

  postgres.on('exit', function(code) {
    if (code != 0) {
      var err = {
        msg: msg,
        code: code
      };
      log.error('unable to start postgres with err: ', err);
    }

    callback(err);
  });
};

/**
 * stops the running postgres instance.
 * @param {function} callback The callback of the form f(err).
 */
PostgresMan.prototype.stop = function stop(callback) {
  var log = this.log;
  var msg = '';
  var postgres = spawn(this.pgCtlPath,
    ['stop', '-w', '-D', this.dataDir, '-m', 'fast']);

  postgres.stdout.on('data', function(data) {
    log.debug('postgres stdout: ', data.toString());
  });

  postgres.stderr.on('data', function(data) {
    var dataStr = data.toString();
    log.error('postgres stderr: ', dataStr);
    if (msg) {
      msg += dataStr;
    } else {
      msg = dataStr;
    }
    msg += data;
  });

  postgres.on('exit', function(code) {
    var err;
    if (code != 0) {
      err = {
        msg: msg,
        code: code
      };
      log.error('unable to stop postgres with err: ', err);
    }

    return callback(err);
  });
};

PostgresMan.prototype.immediatestop = function immediateStop(callback) {
  var log = this.log;
  var msg = '';
  var postgres = spawn(this.pgCtlPath,
    ['stop', '-w', '-D', this.dataDir, '-m', 'immediate']);

  postgres.stdout.on('data', function(data) {
    log.debug('postgres stdout: ', data.toString());
  });

  postgres.stderr.on('data', function(data) {
    var dataStr = data.toString();
    log.error('postgres stderr: ', dataStr);
    if (msg) {
      msg += dataStr;
    } else {
      msg = dataStr;
    }
    msg += data;
  });

  postgres.on('exit', function(code) {
    if (code != 0) {
      var err = {
        msg: msg,
        code: code
      };
      log.error('unable to stop postgres with err: ', err);
    }

    callback(err);
  });
};

PostgresMan.prototype.kill = function kill(callback) {
  var log = this.log;
  var msg = '';
  var postgres = spawn('killall', 'postgres');

  postgres.stdout.on('data', function(data) {
    log.debug('killall stdout: ', data.toString());
  });

  postgres.stderr.on('data', function(data) {
    var dataStr = data.toString();
    log.error('killall stderr: ', dataStr);
    if (msg) {
      msg += dataStr;
    } else {
      msg = dataStr;
    }
    msg += data;
  });

  postgres.on('exit', function(code) {
    if (code != 0) {
      var err = {
        msg: msg,
        code: code
      };
      log.error('unable to kill postgres with err: ', err);
    }

    callback(err);
  });
};

/**
 * Shuts down the postgres process with increasing severity.
 * 1) pg_ctl -m fast stop
 * 2) pg_ctl -m immediate stop
 * 3) killall postgres
 *
 * @param {function} callback The callback of the for f(string, err) where
 * string is the method used to shutdown, which is one of, fast, immediate, or
 * kill.
 */
PostgresMan.prototype.shutdown = function shutdown(callback) {
  var self = this;
  self.stop(function(err) {
    if (err) {
      self.immediatestop(function(err) {
        if (err) {
          self.kill(function(err) {
            return callback(err);
          });
        } else {
          return callback();
        }
      });
    } else {
      return callback();
    }
  });
};

/**
 * Retarts the postgres instance. If no pg instance is running, this will just
 * start pg. If no pg instance is running, this will just start pg.
 * @param {function} callback The callback of the form f(err).
 */
PostgresMan.prototype.restart = function restart(callback) {
  var self = this;
  var log = this.log;
  var msg = '';

  // first stat postgres
  return self.stat(function(stat, err) {
    if (err) {
      return callback(err);
    }
    switch (stat) {
      case 0:
        return restart(callback);
      case 1:
        return self.start(callback);
    }
  });

  function restart(callback) {
    log.info('restarting postgres');
    var postgres = spawn(self.pgCtlPath,
      ['-w', '-D', self.dataDir, '-l', self.logFile, 'restart']);

    postgres.stdout.on('data', function(data) {
      log.debug('postgres stdout: ', data.toString());
    });

    postgres.stderr.on('data', function(data) {
      var dataStr = data.toString();
      log.error('postgres stderr: ', dataStr);
      if (msg) {
        msg += dataStr;
      } else {
        msg = dataStr;
      }
      msg += data;
    });

    postgres.on('exit', function(code) {
      if (code != 0) {
        var err = {
          msg: msg,
          code: code
        };
        log.error('unable to restart postgres with err: ', err);
      }

      return callback(err);
    });
  }
};

/**
 * Checks the status of the postgres instance.
 * @param {function} callback The callback of the form f(stat, err), where stat
 * returns either a 0 if postgres is running, or 1 if postgres is down.
 */
PostgresMan.prototype.stat = function stat(callback) {
  var log = this.log;
  var errMsg;
  var msg = '';
  log.info('checking postgres status');
  var postgres = spawn(this.pgCtlPath, ['status', '-D', this.dataDir]);

  postgres.stdout.on('data', function(data) {
    msg += data.toString();
    log.debug('postgres stdout: ', msg);
  });

  postgres.stderr.on('data', function(data) {
    var dataStr = data.toString();
    log.warn('postgres stderr: ', dataStr);
    if (errMsg) {
      errMsg += dataStr;
    } else {
      errMsg = dataStr;
    }
    errMsg += data;
  });

  // pg_ctl status returns 1 when there are no pg processes running.
  postgres.on('exit', function(code) {
    switch (code) {
      case 0:
        log.info('postgres is running');
        return callback(STATUS.UP);
        break;
      case 1:
        log.info('postgres is not running');
        return callback(STATUS.DOWN);
        break;
      default:
        log.error('error getting pg status: %s', errMsg);
        var err = {
          errMsg: msg,
          code: code
        };
        return callback(STATUS.ERR, err);
        break;
    }
  });
};

