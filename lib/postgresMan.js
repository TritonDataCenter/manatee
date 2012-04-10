var ConfParser = require('../lib/confParser');
var spawn = require('child_process').spawn;
var util = require('util');
var sprintf = util.format;

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
  this.pgPath = options.pgPath;

  this.startCmd = sprintf(START_CMD, this.pgPath, this.dataDir, this.logFile);
  this.stopCmd = sprintf(STOP_CMD, this.pgPath, this.dataDir);
  this.statCmd = sprintf(STAT_CMD, this.pgPath, this.dataDir);

  log.info('PostgreMan initialized', this);
}
module.exports = PostgresMan;

PostgresMan.prototype.primary = function primary(standbys, callback) {

};

PostgresMan.prototype.standby = function standby(primary, callback) {

};

PostgresMan.prototype.readonly = function readonly(callback) {

};

PostgresMan.prototype.start = function start(callback) {
  var log = this.log;
  var msg = '';
  log.info('spawning postgres with command', this.startCmd);
  var postgres = spawn(this.pgPath,
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

PostgresMan.prototype.stop = function stop(callback) {
  var log = this.log;
  var msg = '';
  var postgres = spawn(this.pgPath,
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

PostgresMan.prototype.restart = function restart(callback) {
  var log = this.log;
  var msg = '';
  log.info('spawning postgres with command', this.startCmd);
  var postgres = spawn(this.pgPath,
   ['-w', '-D', this.dataDir, '-l', this.logFile, 'restart']);

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

PostgresMan.prototype.stat = function stat(callback) {
  var log = this.log;
  var msg;
  log.info('stating postgres');
  var postgres = spawn(this.pgPath, ['status', '-D', this.dataDir]);

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
    if (code != 0 || msg) {
      var err = {
        msg: msg,
        code: code
      };
      log.error('unable to stat postgres with err: ', err);
    }

    callback(err);
  });
};

