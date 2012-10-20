// Copyright (c) 2012, Joyent, Inc. All rights reserved.
var assert = require('assert-plus');
var BackupClient = require('./backupClient');
var ConfParser = require('./confParser');
var EventEmitter = require('events').EventEmitter;
var fs = require('fs');
var pg = require('pg');
var Client = pg.Client;
var shelljs = require('shelljs');
var spawn = require('child_process').spawn;
var sprintf = require('util').format;
var SnapShotter = require('./snapShotter');
var url = require('url');
var util = require('util');
var vasync = require('vasync');
var verror = require('verror');

/**
* postgresql.conf keys
*/
var SYNCHRONOUS_STANDBY_NAMES = 'synchronous_standby_names';
var SYNCHRONOUS_COMMIT = 'synchronous_commit';
var PRIMARY_CONNINFO = 'primary_conninfo';
var READ_ONLY = 'default_transaction_read_only';

/**
 * postgresql.conf values
 */
var PRIMARY_CONNINFO_STR = '\'host=%s port=%s user=%s application_name=%s\'';

/**
* The postgres manager which manages interactions with postgres.
* Responsible for initializing, starting, stopping, and health checking a
* running postgres instance.
*/
function PostgresMgr(options) {
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
        assert.object(options.snapShotterCfg, 'options.snapShotterCfg');
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
        * the url of this postgres instance
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

        this.snapShotter = new SnapShotter(options.snapShotterCfg);

        /**
        * The health check interval in ms
        */
        this.healthChkInterval = options.healthChkInterval;
        this.healthChkIntervalId = null;

        /**
        * Postgres operation timeout. Any postgres operation will fail upon
        * exceeding this timeout.
        */
        this.opsTimeout = options.opsTimeout;

        /**
         * Pg Client
         */
        this.pgClient = null;

        log.trace('new postgres man', options);
}

module.exports = PostgresMgr;
util.inherits(PostgresMgr, EventEmitter);

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
PostgresMgr.prototype.initDb = function initDb(self, callback) {
        var log = self.log;
        var msg = '';
        log.info({
                dataDir: self.dataDir
        }, 'PostgresMgr.initDb: entering');

        fs.stat(self.dataDir, function(err, stats) {
                if (err || !stats.isDirectory()) {
                        return callback(new verror.VError(
                                        err,
                                        'postgres datadir ' +
                                        self.dataDir +
                                        'DNE'));
                }

                var postgres = spawn(self.pgInitDbPath, ['-D', self.dataDir]);

                postgres.stdout.on('data', function(data) {
                        log.trace('postgres stdout: ', data.toString());
                });

                postgres.stderr.on('data', function(data) {
                        var dataStr = data.toString();
                        log.info('postgres stderr: ', dataStr);
                        msg += dataStr;
                        msg += data;
                });

                postgres.on('exit', function(code) {
                        if (code !== 0) {
                                var err2 = new verror.VError(msg, code);
                                log.info({
                                        err: err2,
                                        dataDir: self.dataDir
                                },'PostgresMgr.initDb: ' +
                                'unable to initDb postgres');
                        }

                        log.info({
                                dataDir: self.dataDir,
                                hbaConf: self.hbaConf
                        }, 'PostgresMgr.initDb: ' +
                        'copying pg_hba.conf to data dir');
                        shelljs.cp('-f', self.hbaConf,
                                   self.dataDir + '/pg_hba.conf');

                        log.info({
                                dataDir: self.dataDir,
                                postgresqlConf: self.postgresConf
                        }, 'PostgresMgr.initDb:' +
                        ' copying postgresql.conf to data dir');
                        shelljs.cp('-f',
                                   self.postgresConf,
                                   self.dataDir + '/postgresql.conf');

                        return callback();
                });

                return true;
        });

};

/**
* Transition the postgres instance to primary mode.
* @param String The standby.
* @param function callback The callback of the form f(err).
*/
PostgresMgr.prototype.primary = function primary(stdby, callback) {
        var self = this;
        var log = self.log;

        log.info({
                url: self.url,
                standby: stdby
        },'PostgresMgr.primary: entering.');

        var replErrMsg = 'could not verify standby replication status, ' +
                'possible WAL corruption remaining in readonly mode';
        var tasks = [
                function _stopHealthCheck(_, cb) {
                        self.stopHealthCheck(cb);
                },
                function _initDb(_, cb) {
                        self.initDb(self, cb);
                },
                function _deleteRecoveryConf(_, cb) {
                        deleteFile(self, self.recoveryConfPath, function() {
                                return cb();
                        });
                },
                function _updateConfigs(_, cb) {
                        var confOpts = {};
                        confOpts[SYNCHRONOUS_COMMIT] = 'remote_write';
                        confOpts[READ_ONLY] = 'on';
                        updatePgConf(self, confOpts, cb);
                },
                function _restart(_, cb) {
                        restart(self, cb);
                },
                function _snapshot(_, cb) {
                        self.snapShotter.createSnapshot(cb);
                },
                /**
                 * The standby is only updated after the snapshot operation.
                 * This is because if Postgres is started with a sync standby,
                 * and the standby dies, the snapshot operation will hang,
                 * causing the entire process to hang. Thus, we update the
                 * standby only after the snapshot has been taken, and send
                 * sighup to postgres to pick up the new standby.
                 */
                function _updateStandby(_, cb) {
                        if (stdby) {
                                var confOpts = {};
                                confOpts[SYNCHRONOUS_COMMIT] = 'remote_write';
                                confOpts[SYNCHRONOUS_STANDBY_NAMES] =
                                        '\'' + stdby + '\'';
                                confOpts[READ_ONLY] = 'on';

                                updatePgConf(self, confOpts, cb);
                        } else {
                                cb();
                        }
                },
                /**
                 * Only perform the next 3 functions if there is a standby, if
                 * the standby expired we actually want to stay in readonly
                 * mode, so we skip.
                 */
                function _checkReplStatus(_, cb) {
                        if (stdby) {
                                checkRepl(self, function(err) {
                                        // don't pass back the error, because
                                        // we want to stay up and running in
                                        // standby mode so that data is
                                        // available.
                                        if (err) {
                                                log.fatal({err: err},
                                                        replErrMsg);
                                                cb();
                                        } else {
                                                _.cont = true;
                                                cb();
                                        }
                                });
                        } else {
                                cb();
                        }
                },
                function _enableWrites(_, cb) {
                        if (stdby && _.cont) {
                                var confOpts = {};
                                confOpts[SYNCHRONOUS_COMMIT] = 'remote_write';
                                confOpts[SYNCHRONOUS_STANDBY_NAMES] =
                                '\'' + stdby + '\'';
                                updatePgConf(self, confOpts, cb);
                        } else {
                                cb();
                        }
                },
                function _sighup(_, cb) {
                        if (stdby && _.cont) {
                                sighup(self, cb);
                        } else {
                                cb();
                        }
                },
                function _startHealthCheck(_, cb) {
                        self.startHealthCheck(cb);
                }
        ];

        vasync.pipeline({funcs: tasks, arg: {}}, function(err) {
                if (err) {
                        log.error({
                                err: err,
                                standby: stdby,
                                url: self.url
                        }, 'PostgresMgr.primary: error');
                        return callback(err);
                } else {
                        log.info({
                                standby: stdby,
                                url: self.url
                        }, 'PostgresMgr.primary: complete');
                        return callback();
                }
        });
};

/**
 * Updates the standby of the current node. This assumes the current node is
 * already a primary. This does not SIGINT postgres, only SIGHUP.
 */
PostgresMgr.prototype.updateStandby = function updateStandby(stdby, callback) {
        var self = this;
        var log = self.log;

        log.info({
                url: self.url,
                standby: stdby
        },'PostgresMgr.updateStandby: entering.');

        var replErrMsg = 'could not verify standby replication status, ' +
                'possible WAL corruption remaining in readonly mode';
        var tasks = [
                function _stopHealthCheck(_, cb) {
                        self.stopHealthCheck(cb);
                },
                function _updateConfigs(_, cb) {
                        var confOpts = {};
                        confOpts[SYNCHRONOUS_COMMIT] = 'remote_write';
                        if (stdby) {
                                confOpts[SYNCHRONOUS_STANDBY_NAMES] =
                                        '\'' + stdby + '\'';
                        }
                        confOpts[READ_ONLY] = 'on';
                        updatePgConf(self, confOpts, cb);
                },
                function _sighup(_, cb) {
                        sighup(self, cb);
                },
                /**
                 * Only perform the next 3 functions if there is a standby, if
                 * the standby expired we actually want to stay in readonly
                 * mode, so we skip.
                 */
                function _checkReplStatus(_, cb) {
                        if (stdby) {
                                checkRepl(self, function(err) {
                                        // don't pass back the error, because
                                        // we want to stay up and running in
                                        // standby mode so that data is
                                        // available.
                                        if (err) {
                                                log.fatal({err: err},
                                                        replErrMsg);
                                                cb();
                                        } else {
                                                _.cont = true;
                                                cb();
                                        }
                                });
                        } else {
                                cb();
                        }
                },
                function _enableWrites(_, cb) {
                        if (stdby && _.cont) {
                                var confOpts = {};
                                confOpts[SYNCHRONOUS_COMMIT] = 'remote_write';
                                confOpts[SYNCHRONOUS_STANDBY_NAMES] =
                                '\'' + stdby + '\'';
                                updatePgConf(self, confOpts, cb);
                        } else {
                                cb();
                        }
                },
                function _sighupAgain(_, cb) {
                        if (stdby && _.cont) {
                                sighup(self, cb);
                        } else {
                                cb();
                        }
                },
                function _startHealthCheck(_, cb) {
                        self.startHealthCheck(cb);
                }
        ];

        vasync.pipeline({funcs: tasks, arg: {}}, function(err) {
                if (err) {
                        log.info({
                                standby: stdby,
                                url: self.url
                        }, 'PostgresMgr.updateStandby: error');
                        return callback(new verror.VError(err,
                                'PostgresMgr.updateStandby failed'));
                } else {
                        log.info({
                                standby: stdby,
                                url: self.url
                        }, 'PostgresMgr.updateStandby: complete');
                        return callback();
                }
        });

};

/**
 * Transitions a postgres instance to standby state.
 * @param {string} primaryUrl The postgres url of the primary.
 * @param {string} backupUrl The http url of the primary's backup service.
 * @param {function} callback The callback of the form f(err).
 */
PostgresMgr.prototype.standby = function standby(primUrl,
                                                 backupUrl,
                                                 callback) {

        var self = this;
        var log = self.log;
        var primaryUrl = url.parse(primUrl);

        log.info({
                primaryUrl: primaryUrl.href,
                backupUrl: backupUrl
        }, 'PostgresMgr.standby: entering');

        /**
         * If tasks 1-3 error out, then a restore of the database is taken from
         * the primary. This is controlled by the _.isRestore flag attached to
         * the vasync args.
         */
        var tasks = [
                function _stopHealthCheck(_, cb) {
                        self.stopHealthCheck(cb);
                },
                // update primary_conninfo to point to the new (host, port) pair
                function _updatePrimaryConnInfo(_, cb) {
                        updatePrimaryConnInfo(function(err) {
                                if (err) {
                                        _.isRestore = err;
                                }
                                return cb();
                        });
                },
                // set synchronous_commit to off to enable async replication
                function _setSyncCommitOff(_, cb) {
                        if (_.isRestore) {
                                return cb();
                        } else {
                                var opts = {};
                                opts[SYNCHRONOUS_COMMIT] = 'off';
                                updatePgConf(self, opts, function(err) {
                                        if (err) {
                                                _.isRestore = err;
                                        }
                                        return cb();
                                });
                        }

                        return true;
                },
                // restart pg to enable the config changes.
                function _restart(_, cb) {
                        if (_.isRestore) {
                                return cb();
                        } else {
                                restart(self, function(err) {
                                        if (err) {
                                                _.isRestore = err;
                                        }
                                        return cb();
                                });
                        }

                        return true;
                },
                // following run only if _.isRestore is needed
                function _restore(_, cb) {
                        if (!_.isRestore) {
                                return cb();
                        } else {
                                restore(cb);
                        }

                        return true;
                },
                function _updatePrimaryConnInfoAgain(_, cb) {
                        if (!_.isRestore) {
                                return cb();
                        } else {
                                updatePrimaryConnInfo(cb);
                        }

                        return true;
                },
                function _setSyncCommitOffAgain(_, cb) {
                        if (!_.isRestore) {
                                return cb();
                        } else {
                                var opts = {};
                                opts[SYNCHRONOUS_COMMIT] = 'off';
                                updatePgConf(self, opts, function(err) {
                                        if (err) {
                                                _.isRestore = err;
                                        }
                                        return cb();
                                });
                        }

                        return true;
                },
                function _restartAgain(_, cb) {
                        if (!_.isRestore) {
                                return cb();
                        } else {
                                restart(self, cb);
                        }

                        return true;
                },
                function _startHealthCheck(_, cb) {
                        self.startHealthCheck(cb);
                }
        ];

        /**
        * Update the primary connection info in recovery.conf
        */
        function updatePrimaryConnInfo(cb) {
                var opts = {};
                var value = sprintf(
                        PRIMARY_CONNINFO_STR,
                        primaryUrl.hostname,
                        primaryUrl.port,
                        primaryUrl.auth,
                        self.url
                );
                opts[PRIMARY_CONNINFO] = value;
                updateRecoveryConf(self, opts, cb);
        }

        /**
        * Restores the current postgres instance from the primary via zfs_recv.
        */
        function restore(cb) {
                var cfg = self.backupClientCfg;
                cfg.serverUrl = backupUrl;
                var client = new BackupClient(cfg);

                log.info({
                        backupClientCfg: cfg
                }, 'PostgresMgr.standby: restoring db from primary');

                client.restore(function(err2) {
                        if (err2) {
                                log.info({
                                        err: err2,
                                        backupUrl: backupUrl
                                }, 'PostgresMgr.standby: ' +
                                'could not restore from primary');
                                return cb(err2);
                        }
                        log.info('PostgresMgr.standby: finished backup,' +
                                 ' chowning datadir');
                        updateOwner(self, function(err3) {
                                return cb(err3);
                        });

                        return true;
                });
        }

        vasync.pipeline({funcs: tasks, arg: {}}, function(err) {
                if (err) {
                        log.info({
                                err:err,
                                primaryUrl: primaryUrl.href,
                                backupUrl: backupUrl
                        }, 'PostgresMgr.standby: error');
                        return callback(err);
                } else {
                        log.info({
                                primaryUrl: primaryUrl.href,
                                backupUrl: backupUrl
                        }, 'PostgresMgr.standby: complete');
                        return callback();
                }
        });
};

/**
* Start the postgres instance.
* @param {function} callback The callback of the form f(err, process).
*/
PostgresMgr.prototype.start = function start(self, cb) {
        var log = self.log;
        var msg = '';
        var intervalId = null;

        /**
         * Always reset and clear the healthcheck before callback
         */
        var callback = function(err, pg2) {
                if (intervalId) {
                        clearInterval(intervalId);
                        intervalId = null;
                        log.info('clearing healthcheck');
                }

                cb(err, pg2);
        };

        log.info({
                postgresPath: self.postgresPath,
                dataDir: self.dataDir
        },'PostgresMgr.start: entering');

        var postgres = spawn(self.postgresPath, ['-D', self.dataDir]);
        self.postgres = postgres;

        postgres.stdout.once('data', function(data) {
                log.trace('postgres stdout: ', data.toString());
        });

        postgres.stderr.once('data', function(data) {
                var dataStr = data.toString();
                log.trace('postgres stderr: ', dataStr);
                if (msg) {
                        msg += dataStr;
                } else {
                        msg = dataStr;
                }
                msg += data;
        });

        postgres.on('exit', function(code, signal) {
                var err = null;
                if (code !== 0) {
                        err = new verror.VError(msg, code);
                        log.info({
                                postgresPath: self.postgresPath,
                                dataDir: self.dataDir,
                                code: code,
                                signal: signal,
                                err:err
                        }, 'Postgresman.start: postgres -D exited with err');

                        return callback(err);
                }

                log.info({
                        postgresPath: self.postgresPath,
                        dataDir: self.dataDir,
                        code: code,
                        signal: signal
                }, 'Postgresman.start: postgres -D exited with code 0');

                // wait for callback from healthcheck
                return true; // linter complains
        });

        // Wait for db to comeup via healthcheck
        var time = new Date().getTime();
        intervalId = setInterval(function() {
                // clearInterval() may not stop any already enqueued
                // healthchecks, so we only return callback if intervalId is
                // not null
                if (!intervalId) {
                        log.info('aborting cleared start.healthcheck');
                        return true;
                }
                health(self, function(err) {
                        var timeSinceStart = new Date().getTime() - time;
                        if (err) {
                                log.info({
                                        err: err,
                                        timeSinceStart: timeSinceStart,
                                        opsTimeout: self.opsTimeout,
                                        postgresPath: self.postgresPath,
                                        dataDir: self.dataDir
                                }, 'Postgresman.start: db has not started');

                                if (timeSinceStart > self.opsTimeout) {
                                        log.info({
                                                timeSinceStart: timeSinceStart,
                                                opsTimeout: self.opsTimeout,
                                                postgresPath: self.postgresPath,
                                                dataDir: self.dataDir
                                        }, 'Postgresman.start: start timeout');

                                        self.stop(function() {
                                                return callback(err, postgres);
                                        });
                                }
                        } else {
                                log.info({
                                        timeSinceStart: timeSinceStart,
                                        opsTimeout: self.opsTimeout,
                                        postgresPath: self.postgresPath,
                                        dataDir: self.dataDir
                                }, 'Postgresman.start: db has started');
                                return callback(null, postgres);
                        }

                        return true;
                });

                return true;
        }, 1000);
};

/**
* Starts the periodic health checking of the pg instance.
* emits error if healthchk fails
*/
PostgresMgr.prototype.startHealthCheck = function startHealthCheck(callback) {
        var self = this;
        var log = self.log;
        log.info('Postgresman.starthealthCheck: entering');

        var started = false;
        if (self.healthChkIntervalId) {
                log.info('Postgresman.starthealthCheck: ' +
                         'health check already running');
                return callback();
        } else {
                self.healthChkIntervalId = setInterval(function() {
                        if (!self.healthChkIntervalId) {
                                log.info('aborting healthcheck no intervalId');
                                return;
                        }
                        health(self, function(err) {
                                if (err) {
                                        log.warn({err: err},
                                                'Postgresman.starthealthCheck:'
                                                + 'health check failed');
                                        self.emit('error', err);
                                } else {
                                        // return callback once healthcheck
                                        // has started
                                        if (!started) {
                                                started = true;
                                                log.info('healthcheck started');
                                                return callback();
                                        }
                                }

                                return true;
                        });
                }, self.healthChkInterval);
        }

        return true;
};

PostgresMgr.prototype.stopHealthCheck = function stopHealthCheck(callback) {
        var self = this;
        var log = self.log;
        log.info({
                healthChkIntervalId: self.healthChkIntervalId
        }, 'Postgresman.stopHealthCheck: entering');

        if (self.healthChkIntervalId) {
                clearInterval(self.healthChkIntervalId);
                self.healthChkIntervalId = null;
        } else {
                log.info('Postgresman.stopHealthCheck: not running');
        }
        return callback();
};

/**
* stops the running postgres instance.
* @param {function} callback The callback of the form f(err).
*
* SIGTERM, SIGINT, SIGQUIT, SIGKILL
* The first will wait for all clients to terminate before quitting, the second
* will forcefully disconnect all clients, and the third will quit immediately
* without proper shutdown, resulting in a recovery run during restart.
*/
PostgresMgr.prototype.stop = function stop(callback) {
        var self = this;
        var log = self.log;
        log.info('PostgresMgr.stop: entering');

        var successful;
        var postgres = self.postgres;
        if (!postgres) {
                var msg = 'postgres handle DNE,' +
                        ' was postgres started by another process?';
                var err = new verror.VError(msg);
                log.info({
                        err: err,
                        postgresHandle: postgres,
                        datadir: self.dataDir
                }, msg);

                return callback(err);
        }
        postgres.on('exit', function(code, signal) {
                log.info({
                        code: code,
                        signal: signal
                }, 'PostgresMgr.stop: postgres exited with');
                successful = true;
                return callback();
        });

        log.info('PostgresMgr.stop: trying SIGINT');
        postgres.kill('SIGINT');
        // simply wait opsTimeout before executing SIGQUIT
        setTimeout(function() {
                if (!successful) {
                        log.info('PostgresMgr.stop: trying SIGQUIT');
                        postgres.kill('SIGQUIT');
                }
                // set another timeout and SIGKILL
                setTimeout(function() {
                        if (!successful) {
                                log.info('PostgresMgr.stop: trying SIGKILL');
                                postgres.kill('SIGKILL');
                        }
                        // set another timeout and return error
                        setTimeout(function() {
                                if (!successful) {
                                        log.error('PostgresMgr.stop: failed');
                                        var err2 = new verror.VError(
                                                'SIGKILL failed');
                                        return callback(err2);
                                }
                                return true;
                        });
                }, self.opsTimeout);

        }, self.opsTimeout);

        return true;
};

// private functions

/**
* Update the owner of the the pg data dir back to the postgres user.
* ZFS receive operations resets the dir back to root ownership. pg does not
* allow root ownership on a pg data dir.
*/
function updateOwner(self, callback) {
        var log = self.log;
        log.info({
                self: self.user,
                dataDir: self.dataDir
        }, 'Postgresman.updateOwner: entering');
        var msg = '';
        var chown = spawn('pfexec', ['chown', '-R', self.dbUser, self.dataDir]);

        chown.stdout.on('data', function(data) {
                log.trace('chown stdout: ', data.toString());
        });

        chown.stderr.on('data', function(data) {
                var dataStr = data.toString();
                log.info('Postgresman.updateOwner: chown stderr: ', dataStr);
                msg += dataStr;
                msg += data;
        });

        chown.on('exit', function(code) {
                var err;
                if (code !== 0) {
                        err = new verror.VError(msg, code);
                        log.info({
                                self: self.user,
                                dataDir: self.dataDir,
                                err: err
                        }, 'PostgresMgr.updateOwner: unable to chown datadir');
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
        log.info({path: path}, 'PostgresMgr.deleteFile: entering');
        shelljs.rm(path);
        return callback();
}

function queryDb(self, query, callback) {
        var log = self.log;
        log.trace({
                query: query
        }, 'Postgresman.query: entering.');

        var client = new Client(self.url);
        client.connect(function(err) {
                if (err) {
                        log.info({err: err},
                                 'Postgresman.query: can\'t connect to pg');
                        client.end();
                        return callback(err);
                }
                log.trace({
                        query: query
                }, 'Postgresman.query: connected to pg, executing query');
                client.query(query, function(err2, result) {
                        client.end();
                        if (err2) {
                                log.info({ err: err2 },
                                         'error whilst querying pg');
                        }
                        return callback(err2, result);
                });

                return true;
        });
}

/**
 * sends sighup to postgres, duh.
 */
function sighup(self, callback) {
        var log = self.log;
        log.info('Postgresman.sighup: entering');

        var postgres = self.postgres;
        postgres.kill('SIGHUP');
        callback();
}

/**
 * Update keys in postgresql.conf. Note, keys in the current config not present
 * in the default config will be lost
 */
function updatePgConf(self, options, cb) {
        updateConf(self, options, self.postgresConf, self.postgresConfPath, cb);
}

/**
 * Update keys in recovery.conf. Note, keys in the current config not present
 * in the default config will be lost.
 */
function updateRecoveryConf(self, options, cb) {
        updateConf(self, options, self.recoveryConf, self.recoveryConfPath, cb);
}

function updateConf(self, options, rpath, wpath, cb) {
        var log = self.log;
        log.info({
                options: options,
                rpath: rpath,
                wpath: wpath
        }, 'updating config');

        ConfParser.read(rpath, function(err, conf) {
                if (err) {
                        log.info({
                                err: err,
                                options: options,
                                postgresConf: rpath
                        },'unable to read config');
                        return cb(err);
                }

                for (var confKey in options) {
                        log.debug({
                                key: confKey,
                                value: options[confKey]
                        }, 'writing config key');
                        ConfParser.set(conf, confKey, options[confKey]);
                }

                if (options.readonly) {
                        ConfParser.set(conf, READ_ONLY, 'on');
                }

                log.info({
                        conf: conf,
                        options: options,
                        rpath: rpath,
                        wpath: wpath
                }, 'writing configs');

                ConfParser.write(wpath, conf, cb);
                return true;
        });
}

/**
* Restarts the postgres instance. If no pg instance is running, this will just
* start pg.
* @param {function} callback The callback of the form f(err).
*/
function restart(self, callback) {
        var log = self.log;
        log.info('Postgresman.restart: entering');

        // check health first to see if db is running
        health(self, function(err) {
                if (err) {
                        log.info('Postgresman.restart: db not running');
                        return self.start(self, callback);
                } else {
                        log.info('Postgresman.restart: db is running');
                        self.stop(function(err2) {
                                if (err2) {
                                        return callback(err2);
                                } else {
                                        return self.start(self, callback);
                                }
                        });
                }

                return true;
        });
}

/**
* Check the health status of the running postgres db.
* @param {function} callback The callback of the form f(err), where err
* indicates an unhealthy db.
*/
function health(self, callback) {
        var log = self.log;
        log.trace('Postgresman.health: entering');
        try {
                queryDb(self, 'select now() as when', function(err) {
                        if (err) {
                                log.info({err: err}, 'Postgresman.health: ' +
                                         'failed');
                        }
                        if (callback) {
                                return callback(err);
                        }
                        return true;
                });
        } catch (e) {
                log.info({
                        err: e
                }, 'Postgresman.health: pg health chk failed');
                if (callback) {
                        return callback(e);
                }
        }

        return true;
}

function checkRepl(self, cb) {
        var log = self.log;
        var replReplayLoc = null;
        var replStartTime = Date.now();
        var intervalId = setInterval(function() {
        checkReplStatus(self, intervalId,
                function(err, stop, replayLoc)
                {
                        if (err) {
                                clearInterval(intervalId);
                                return cb(new verror.VError(err));
                        }
                        if (!replReplayLoc || replayLoc > replReplayLoc) {
                                log.info({
                                        oldReplayLoc: replReplayLoc,
                                        currReplLoc: replayLoc
                                }, 'replay row incremented, resetting' +
                                ' startTime');
                        replStartTime = Date.now();
                        replReplayLoc = replayLoc;
                        }
                        var diffTime = Date.now() - replStartTime;
                        // stop if caught up, return error if standby
                        // times out
                        if (stop) {
                                log.info({
                                        stop: stop,
                                        diffTime: diffTime,
                                        oldReplayLoc: replReplayLoc,
                                        currReplLoc: replayLoc
                                }, 'stopping replciation check');
                                clearInterval(intervalId);
                                intervalid = null;
                                return cb();
                        } else if (diffTime > 30000) {
                                clearInterval(intervalId);
                                intervalid = null;
                                return cb(new verror.VError(
                                        'standby unable to make forward progress'));
                        } else {
                                log.info({
                                        stop: stop,
                                        diffTime: diffTime,
                                        oldReplayLoc: replReplayLoc,
                                        currReplLoc: replayLoc
                                }, 'continuing replication check');
                        }

                        return (undefined);
                });

        }, 1000);
}

//TODO: stop polling after sometime? what happens if the standby never catches
//up? we are blocked and never get interrupted. We need to make this operation
//interruptable.  TODO: what happens if the query doesn't return?
function checkReplStatus(self, intervalId, callback) {
        var log = self.log;
        log.debug('Postgresman.checkReplStatus: entering');
        if (!intervalId) {
                log.warn('interval cleared, skipping checkReplStatus');
                return callback();
        }
        var queryString = 'select * from pg_stat_replication';
        if (!self.pgClient) {
                log.debug('creating client');
                self.pgClient = new Client(self.url);
                self.pgClient.connect();
        }
        var client = self.pgClient;
        var query = client.query('select * from pg_stat_replication');
        log.debug({
                query: query
        }, 'creating query');

        var result;
        query.once('row', function(row) {
                log.info({
                        row: row
                }, 'got row');
                result = row;
        });

        query.once('error', function(err) {
                var err2 = new verror.VError(
                        'error whilst checking pg_stat_replication');
                return callback(err2);
        });

        query.once('end', function() {
                log.info('query ended!');
                if (!result) {
                        var msg = 'no replication status';
                        var err2 = new verror.VError(msg);
                        return callback(err2);
                }
                var sentLocation = result.sent_location.split('/')[1];
                sentLocation = parseInt(sentLocation, 16);
                var replayLocation = result.replay_location.split('/')[1];
                replayLocation = parseInt(replayLocation, 16);

                log.info({
                        primary: sentLocation,
                        standby: replayLocation
                        }, 'PostgresMgr.checkReplStatus: xlog locations are');

                if (sentLocation === replayLocation) {
                        log.info('exiting chekReplStatus: ' +
                        'synchronous standby caught up');
                        return callback(null, true, replayLocation);
                } else {
                        log.info({
                                row: result
                        }, 'stil waiting for synchronous standby to catch up');
                        return callback(null, null, replayLocation);
                }
        });
}

