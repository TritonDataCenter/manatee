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

/**
 * postgresql.conf values
 */
var PRIMARY_CONNINFO_STR = '\'host=%s port=%s user=%s application_name=%s\'';

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

        log.info('new postgres man', options);
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
        log.info({
                dataDir: self.dataDir
        }, 'PostgresMan.iniDb: entering');

        shelljs.mkdir('-p', self.dataDir);
        var postgres = spawn(self.pgInitDbPath, ['-D', self.dataDir]);

        postgres.stdout.on('data', function(data) {
                log.trace('postgres stdout: ', data.toString());
        });

        postgres.stderr.on('data', function(data) {
                var dataStr = data.toString();
                log.info('postgres stderr: ', dataStr);
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
                        err = new verror.VError(msg, code);
                        log.info({
                                err: err,
                                dataDir: self.dataDir
                        },'PostgresMan.initDb: unable to initDb postgres');
                }
                log.info({
                        dataDir: self.dataDir,
                        hbaConf: self.hbaConf
                }, 'PostgresMan.initDb: copying pg_hba.conf to data dir');
                shelljs.cp('-f', self.hbaConf, self.dataDir + '/pg_hba.conf');
                log.info({
                        dataDir: self.dataDir,
                        postgresqlConf: self.postgresConf
                }, 'PostgresMan.initDb: copying postgresql.conf to data dir');
                shelljs.cp('-f',
                           self.postgresConf,
                           self.dataDir + '/postgresql.conf');

                return callback();
        });
};

/**
* Transition the postgres instance to primary mode.
* @param String The standby.
* @param function callback The callback of the form f(err).
* TODO: transition to read-only mode if there is no standby.
*/
PostgresMan.prototype.primary = function primary(stdby, callback) {
        var self = this;
        var log = self.log;

        log.info({
                url: self.url,
                standby: stdby
        },'PostgresMan.primary: entering.');

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
                        updateStandbySetSyncCommit(cb);
                },
                function _restart(_, cb) {
                        restart(self, cb);
                },
                function _snapshot(_, cb) {
                        self.snapShotter.createSnapshot(cb);
                },
                function _startHealthCheck(_, cb) {
                        self.startHealthCheck(cb);
                }
        ];

        /**
         * Updates synchronous_standby_names and sets synchronous_commit=on
         */
        function updateStandbySetSyncCommit(cb) {
                log.info({
                        standby: stdby,
                        postgresConf: self.postgresConf
                }, 'updating standby names in conf');

                ConfParser.read(self.postgresConf, function(err, conf) {
                        if (err) {
                                log.info({
                                        err: err,
                                        standby: stdby,
                                        postgresConf: self.postgresConf
                                },'unable to read config');
                                return cb(err);
                        }
                        // update standby list
                        if (stdby) {
                                ConfParser.set(conf,
                                               SYNCHRONOUS_STANDBY_NAMES,
                                               '\'' + stdby + '\'');
                        }
                        // set synchronous_commit = remote_write;
                        ConfParser.set(conf,
                                       SYNCHRONOUS_COMMIT,
                                       'remote_write');
                        log.info({
                                conf: conf,
                                postgresConf: self.postgresConf,
                                standby: stdby
                        }, 'writing configs');

                        ConfParser.write(self.postgresConfPath, conf, cb);
                        return true;
                });
        }

        vasync.pipeline({funcs: tasks}, function(err) {
                if (err) {
                        log.info({
                                err: err,
                                standby: stdby,
                                url: self.url
                        }, 'PostgresMan.primary: error');
                        return callback(err);
                } else {
                        log.info({
                                standby: stdby,
                                url: self.url
                        }, 'PostgresMan.primary: complete');
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
PostgresMan.prototype.standby = function standby(primUrl,
                                                 backupUrl,
                                                 callback) {

        var self = this;
        var log = self.log;
        var primaryUrl = url.parse(primUrl);

        log.info({
                primaryUrl: primaryUrl.href,
                backupUrl: backupUrl
        }, 'PostgresMan.standby: entering');

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
                                setSyncCommittOff(function(err) {
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
                                setSyncCommittOff(cb);
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
                ConfParser.read(self.recoveryConf, function(err, conf) {
                        if (err) {
                                log.info({
                                        err: err,
                                        recoveryConf: self.recoveryConf
                                }, 'PostgresMan.standby: ' +
                                'could not update primary conninfo');
                                return cb(err);
                        }
                        var value = sprintf(PRIMARY_CONNINFO_STR,
                                            primaryUrl.hostname,
                                            primaryUrl.port,
                                            primaryUrl.auth,
                                            self.url);

                        log.info({
                                configStr: value,
                                recoveryConf: self.recoveryConf,
                                recoveryConfPath: self.recoveryConfPath
                        }, 'PostgresMan.standby: updating primary conn info');

                        ConfParser.set(conf, PRIMARY_CONNINFO, value);
                        return ConfParser.write(self.recoveryConfPath,
                                                conf,
                                                cb());
                });
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
                }, 'PostgresMan.standby: restoring db from primary');

                client.restore(function(err2) {
                        if (err2) {
                                log.info({
                                        err: err2,
                                        backupUrl: backupUrl
                                }, 'PostgresMan.standby: ' +
                                'could not restore from primary');
                                return cb(err2);
                        }
                        log.info('PostgresMan.standby: finished backup,' +
                                 ' chowning datadir');
                        updateOwner(self, function(err3) {
                                return cb(err3);
                        });

                        return true;
                });
        }

        /**
         * Set sync_commit = off in postgressql.conf
         */
        function setSyncCommittOff(cb) {
                ConfParser.read(self.postgresConf, function(err, conf) {
                        if (err) {
                                log.info({
                                        err: err,
                                        postgresConf: self.postgresConf,
                                        postgresConfPath: self.postgresConfPath
                                }, 'PostgresMan.standby: ' +
                                'could not set sync commit to off');
                                return cb(err);
                        }
                        ConfParser.set(conf, SYNCHRONOUS_COMMIT, 'off');

                        log.info('PostgresMan.standby: ' +
                                 'setting syncronous_commit to off');
                        ConfParser.write(self.postgresConfPath,
                                         conf,
                                         cb);

                        return true;
                });
        }

        vasync.pipeline({funcs: tasks, arg: {}}, function(err) {
                if (err) {
                        log.info({
                                err:err,
                                primaryUrl: primaryUrl.href,
                                backupUrl: backupUrl
                        }, 'PostgresMan.standby: error');
                        return callback(err);
                } else {
                        log.info({
                                primaryUrl: primaryUrl.href,
                                backupUrl: backupUrl
                        }, 'PostgresMan.standby: complete');
                        return callback();
                }
        });
};

/**
* Start the postgres instance.
* @param {function} callback The callback of the form f(err, process).
*/
PostgresMan.prototype.start = function start(self, cb) {
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
        },'PostgresMan.start: entering');

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
* emits pg_err if healthchk fails
*/
PostgresMan.prototype.startHealthCheck = function startHealthCheck(callback) {
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

PostgresMan.prototype.stopHealthCheck = function stopHealthCheck(callback) {
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
                        err = new verror.VError(msg, code);
                        log.info({
                                self: self.user,
                                dataDir: self.dataDir,
                                err: err
                        }, 'PostgresMan.updateOwner: unable to chown datadir');
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
        log.info({path: path}, 'PostgresMan.deleteFile: entering');
        shelljs.rm(path);
        return callback();
}

function queryDb(self, query, callback) {
        var log = self.log;
        log.debug({
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
                log.debug({
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
* Restarts the postgres instance. If no pg instance is running, this will just
* start pg.
* @param {function} callback The callback of the form f(err).
* TODO: are restarts neccessary for standby changes?
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
* stops the running postgres instance.
* @param {function} callback The callback of the form f(err).
*
* SIGTERM, SIGINT, SIGQUIT, SIGKILL
* The first will wait for all clients to terminate before quitting, the second
* will forcefully disconnect all clients, and the third will quit immediately
* without proper shutdown, resulting in a recovery run during restart.
*/
PostgresMan.prototype.stop = function stop(callback) {
        var self = this;
        var log = self.log;
        log.info('PostgresMan.stop: entering');

        var successful;
        var postgres = self.postgres;
        if (!postgres) {
                var msg = 'postgres handle DNE,' +
                        ' was postgers started by another process?';
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
                }, 'PostgresMan.stop: postgres exited with');
                successful = true;
                return callback();
        });

        log.info('PostgresMan.stop: trying SIGINT');
        postgres.kill('SIGINT');
        // simply wait opsTimeout before executing SIGQUIT
        setTimeout(function() {
                if (!successful) {
                        log.info('PostgresMan.stop: trying SIGQUIT');
                        postgres.kill('SIGQUIT');
                }

                // set another timeout and SIGKILL
                setTimeout(function() {
                        if (!successful) {
                                log.info('PostgresMan.stop: trying SIGKILL');
                                postgres.kill('SIGKILL');
                        }
                        // set another timeout and return error
                        setTimeout(function() {
                                if (!successful) {
                                        log.error('PostgresMan.stop: failed');
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

/**
* Check the health status of the running postgres db.
* @param {function} callback The callback of the form f(err), where err
* indicates an unhealthy db.
*/
function health(self, callback) {
        var log = self.log;
        log.debug('Postgresman.health: entering');
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
