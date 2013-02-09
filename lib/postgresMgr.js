// Copyright (c) 2012, Joyent, Inc. All rights reserved.
var assert = require('assert-plus');
var ZfsClient = require('./zfsClient');
var ConfParser = require('./confParser');
var EventEmitter = require('events').EventEmitter;
var fs = require('fs');
var pg = require('pg');
var Client = pg.Client;
var shelljs = require('shelljs');
var spawn = require('child_process').spawn;
var shellSpawner = require('./shellSpawner').spawn;
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
        assert.object(options.zfsClientCfg, 'options.zfsClientCfg');
        assert.object(options.snapShotterCfg, 'options.snapShotterCfg');
        assert.number(options.healthChkInterval, 'options.healthChkInterval');
        assert.number(options.healthChkTimeout, 'options.healthChkTimeout');
        assert.number(options.opsTimeout, 'options.opsTimeout');
        assert.number(options.replicationTimeout, 'options.replicationTimeout');
        assert.optionalBool(options.oneNodeWriteMode, 'options.oneNodeWriteMode');

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
        this.zfsClientCfg = options.zfsClientCfg;

        this.snapShotter = new SnapShotter(options.snapShotterCfg);

        /**
        * The health check interval in ms
        */
        this.healthChkTimeout = options.healthChkTimeout;
        this.healthChkInterval = options.healthChkInterval;
        this.healthChkIntervalId = null;
        this.lastHealthChkTime = Date.now();

        /**
        * Postgres operation timeout. Any postgres operation will fail upon
        * exceeding this timeout.
        */
        this.opsTimeout = options.opsTimeout;

        /**
         * Postgres replication timeout. If a standby hasn't caught up with the
         * primary in this time frame, then this shard may have WAL corruption
         * and is put into a read only state.
         */
        this.replicationTimeout = options.replicationTimeout;

        /** Enable writes when there's only one node in the shard? This is
         * dangerous and should be avoided as this will cause WAL corruption
         */
        this.oneNodeWriteMode = options.oneNodeWriteMode || false;

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

                return (undefined);
        });

};

/**
 * Transition the postgres instance to read only mode, disconnected from all
 * other peers.
 */
PostgresMgr.prototype.readOnly = function readOnly(callback) {
        var self = this;
        var log = self.log;

        log.info({
                url: self.url
        },'PostgresMgr.readonly: entering.');

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
                        confOpts[READ_ONLY] = 'on';
                        updatePgConf(self, confOpts, cb);
                },
                function _restart(_, cb) {
                        restart(self, cb);
                },
                function _startHealthCheck(_, cb) {
                        self.startHealthCheck(cb);
                }
        ];

        vasync.pipeline({funcs: tasks, arg: {}}, function(err) {
                if (err) {
                        log.error({
                                err: err,
                                url: self.url
                        }, 'PostgresMgr.readOnly: error');
                        return callback(err);
                } else {
                        log.info({
                                url: self.url
                        }, 'PostgresMgr.readOnly: complete');
                        return callback();
                }
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
                        if (!self.oneNodeWriteMode) {
                                confOpts[READ_ONLY] = 'on';
                        } else {
                                log.warn('enable write mode with only one ' +
                                         'node, may cause WAL corruption!');
                        }
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
                                        if (err) {
                                                log.fatal({err: err},
                                                        replErrMsg);
                                                err.__walCorruption = true;
                                        }
                                        cb(err);
                                });
                        } else {
                                cb();
                        }
                },
                function _enableWrites(_, cb) {
                        if (stdby) {
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
                        if (stdby) {
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
                                // if there is a standby, we always want to
                                // stay in read-only mode
                                confOpts[READ_ONLY] = 'on';
                        } else if (!self.oneNodeWriteMode) {
                                confOpts[READ_ONLY] = 'on';
                        } else {
                                log.warn('enable write mode with only one ' +
                                         'node, may cause WAL corruption!');
                        }

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
                                        if (err) {
                                                log.fatal({err: err},
                                                        replErrMsg);
                                                err.__walCorruption = true;
                                        }
                                        cb(err);
                                });
                        } else {
                                cb();
                        }
                },
                function _enableWrites(_, cb) {
                        if (stdby) {
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
                        if (stdby) {
                                sighup(self, cb);
                        } else {
                                cb();
                        }
                },
                // always re-start healthcheck
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
                        return callback(err);
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
        var backupSnapshot;

        log.info({
                primaryUrl: primaryUrl.href,
                backupUrl: backupUrl
        }, 'PostgresMgr.standby: entering');

        self.zfsClientCfg.serverUrl = backupUrl;
        var zfsClient = new ZfsClient(self.zfsClientCfg);

        /**
         * If tasks 1-4 error out, then a restore of the database is taken from
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
                                _.isRestore = err;
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
                                        _.isRestore = err;
                                        return cb();
                                });
                        }

                        return (undefined);
                },
                // restart pg to enable the config changes.
                function _restart(_, cb) {
                        if (_.isRestore) {
                                return cb();
                        } else {
                                restart(self, function(err) {
                                        _.isRestore = err;
                                        return cb();
                                });
                        }

                        return (undefined);
                },
                // following run only if _.isRestore is needed
                function _restore(_, cb) {
                        if (!_.isRestore) {
                                return cb();
                        } else {
                                restore(function(err) {
                                        // restore the original backup if
                                        // zfs recv fails.
                                        if (err) {
                                                zfsClient.restoreDataset(
                                                        backupSnapshot,
                                                        function() {
                                                                return cb(err);
                                                        }
                                                );
                                        } else {
                                                return cb();
                                        }
                                        return (undefined);
                                });
                        }

                        return (undefined);
                },
                // update primary info since the zfs dataset from the primary
                // will not contain standby information
                function _updatePrimaryConnInfoAgain(_, cb) {
                        if (!_.isRestore) {
                                return cb();
                        } else {
                                updatePrimaryConnInfo(cb);
                        }

                        return (undefined);
                },
                // again because the restore from the primary will have this
                // set to enabled
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

                        return (undefined);
                },
                function _restartAgain(_, cb) {
                        if (!_.isRestore) {
                                return cb();
                        } else {
                                restart(self, function(err) {
                                        // restore the original snapshot if we
                                        // can't restart, which usuallly
                                        // indicates corruption in the received
                                        // dataset
                                        if (err) {
                                                zfsClient.restoreDataset(
                                                        backupSnapshot,
                                                        function() {
                                                                return cb(err);
                                                        }
                                                );
                                        } else {
                                                return cb();
                                        }

                                        return (undefined);
                                });
                        }

                        return (undefined);
                },
                // start health check irregardless of restore
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
                log.info({
                        zfsClientCfg: self.zfsClientCfg
                }, 'PostgresMgr.standby: restoring db from primary');

                zfsClient.restore(function(err2, snapshot) {
                        backupSnapshot = snapshot;
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
                        shellSpawner('pfexec chown -R ' + self.dbUser + ' ' +
                                     self.dataDir, log, cb);
                        return (undefined);
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

        // prevent cb from being called back more than once.
        var calledBack = false;

        /**
         * Always reset and clear the healthcheck before callback
         */
        var callback = function(err, pg2) {
                if (intervalId) {
                        clearInterval(intervalId);
                        intervalId = null;
                        log.info('clearing healthcheck');
                }

                if (calledBack) {
                        return;
                }

                calledBack = true;
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
                if (code !== 0) {
                        var err = new verror.VError(msg, code);
                        log.info({
                                postgresPath: self.postgresPath,
                                dataDir: self.dataDir,
                                code: code,
                                signal: signal,
                                err: err
                        }, 'Postgresman.start: postgres -D exited with err');

                        /* fix for MANTA-997. This callback when invoked more
                         * than once indicates that postgres has exited
                         * unexpectedly -- usually as a result of pg core dump.
                         * Since postgres is started as a child process, when
                         * it unexpectedly exits, start(), which has already
                         * returned when postgres was first started, will
                         * return another callback indicating postgres has
                         * exited.  If this callback is invoked, it manifests
                         * itself by causing vasync to throw a pipeline error.
                         * What we really want is to indicate this as fatal and
                         * exit manatee.
                         */
                        if (calledBack) {
                                var errMsg = 'postgres exited unexpectedly, ' +
                                             'exiting manatee, please check ' +
                                             'for pg core dumps.';
                                log.fatal(errMsg);
                                self.emit('error',
                                          new verror.VError(err, errMsg));
                        }

                        return callback(err);
                }

                log.info({
                        postgresPath: self.postgresPath,
                        dataDir: self.dataDir,
                        code: code,
                        signal: signal
                }, 'Postgresman.start: postgres -D exited with code 0');

                // wait for callback from healthcheck
                return (undefined);
        });

        // Wait for db to comeup via healthcheck
        var time = new Date().getTime();
        intervalId = setInterval(function() {
                // clearInterval() may not stop any already enqueued
                // healthchecks, so we only return callback if intervalId is
                // not null
                if (!intervalId) {
                        log.info('aborting cleared start.healthcheck');
                        return (undefined);
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

                        return (undefined);
                });

                return (undefined);
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

        if (self.healthChkIntervalId) {
                log.info('Postgresman.starthealthCheck: ' +
                         'health check already running');
                return callback();
        } else {
                self.lastHealthChkTime = Date.now();
                self.healthChkIntervalId = setInterval(function() {
                        // prevents health check from running if already
                        // cancelled
                        if (!self.healthChkIntervalId) {
                                log.info('aborting healthcheck no intervalId');
                                return;
                        }
                        var clozure = this;
                        // set a timeout in case health() doesn't return in time
                        clozure.timeoutId = setTimeout(function() {
                                if (clozure.timeoutId) {
                                        self.emit('error',
                                                new verror.VError('PostgresMgr.health() timed out'));
                                }
                        }, self.healthChkTimeout);

                        health(self, function(err) {
                                healthHandler(err, clozure);
                        });
                }, self.healthChkInterval);


                // return callback once healthcheck has been dispatched
                return callback();
        }

        /**
         * only error out when we've exceeded the timeout
         */
        function healthHandler(err, clozure) {
                log.trace({err: err},
                        'postgresMgr.startHealthCheck.health: returned');
                clearTimeout(clozure.timeoutId);
                clozure.timeoutId = null;
                if (err) {
                        var timeElapsed = Date.now() - self.lastHealthChkTime;
                        log.debug({
                                err: err,
                                timeElapsed: timeElapsed,
                                timeOut: self.healthChkTimeout
                        }, 'postgresman.health: failed');
                        if (timeElapsed > self.healthChkTimeout) {
                                var msg = 'PostgresMgr.healthChk: health ' +
                                        'check timed out';

                                log.info({
                                        err: err,
                                        timeElapsed: timeElapsed,
                                        timeOut: self.healthChkTimeout
                                }, msg);
                                self.emit('error', new verror.VError(err, msg));

                        }
                } else {
                        self.lastHealthChkTime = Date.now();
                }
        }


        return (undefined);
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
                                return (undefined);
                        });
                }, self.opsTimeout);

        }, self.opsTimeout);

        return (undefined);
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

function queryDb(self, queryStr, callback) {
        var log = self.log;
        var calledBack = false;
        log.trace({
                query: queryStr
        }, 'Postgresman.query: entering.');

        if (!self.pgClient) {
                self.pgClient = new Client(self.url);
                self.pgClient.once('error', function(err) {
                        self.pgClient.removeAllListeners();
                        log.trace({err: err}, 'got pg client error');
                        // set the client to null on error so we can create a
                        // new client
                        self.pgClient = null;
                        if (!calledBack) {
                                return callback(new verror.VError(err,
                                        'error whilst querying postgres'));
                        }

                        return (undefined);
                });
                self.pgClient.connect();
        }


        var query = self.pgClient.query(queryStr);
        var result = null;
        log.trace('querying', query);
        query.once('row', function(row) {
                log.trace({
                        row: row
                }, 'got row');
                result = row;
        });

        query.once('error', function(err) {
                log.trace({
                        err: err
                }, 'got err');
                var err2 = new verror.VError(
                        'error whilst querying postgres');
                calledBack = true;
                // set the client to null on error so we can create a new client
                self.pgClient = null;
                return callback(err2);
        });

        query.once('end', function() {
                log.trace('query ended!');
                calledBack = true;
                return callback(null, result);
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
        log.debug({
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

                log.trace({
                        conf: conf,
                        options: options,
                        rpath: rpath,
                        wpath: wpath
                }, 'writing configs');

                ConfParser.write(wpath, conf, cb);
                return (undefined);
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

                return (undefined);
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
        queryDb(self, 'select current_time;', function(err) {
                if (err) {
                        log.trace({err: err}, 'Postgresman.health: ' +
                                 'failed');
                }
                return callback(err);
        });

        return (undefined);
}

/**
 * check the replication status of the current pg node. returns error if
 * replication has failed.
 */
function checkRepl(self, cb) {
        var log = self.log;
        var replReplayLoc = null;
        var replStartTime = Date.now();
        var intervalId = setInterval(function() {
        checkReplStatus(self, intervalId,
                function(err, _stop, replayLoc) {
                        if (err) {
                                // only return error once we've exceeded the
                                // timeout
                                log.info({err: new verror.VError(err, 'unable' +
                                ' to query replication status')});
                        } else if (!replReplayLoc || replayLoc > replReplayLoc) {
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
                        if (_stop) {
                                log.info({
                                        stop: _stop,
                                        diffTime: diffTime,
                                        oldReplayLoc: replReplayLoc,
                                        currReplLoc: replayLoc
                                }, 'stopping replication check');
                                clearInterval(intervalId);
                                intervalId = null;
                                return cb();
                        } else if (diffTime > self.replicationTimeout) {
                                //  at this point, we've timed out trying to
                                //  wait/query replication state, so we return
                                //  error
                                clearInterval(intervalId);
                                intervalId = null;
                                return cb(new verror.VError(
                                        'standby unable to make forward progress'));
                        } else {
                                log.info({
                                        stop: _stop,
                                        diffTime: diffTime,
                                        oldReplayLoc: replReplayLoc,
                                        currReplLoc: replayLoc
                                }, 'continuing replication check');
                        }

                        return (undefined);
                });

        }, 1000);
}

function checkReplStatus(self, intervalId, callback) {
        var log = self.log;
        log.debug('Postgresman.checkReplStatus: entering');
        if (!intervalId) {
                return callback();
        }
        queryDb(self, 'select * from pg_stat_replication', function(err, result) {
                log.debug({err: err, result: result}, 'returned from query');
                if (err) {
                        return callback(new verror.VError(err,
                                'unable to query replication stat'));
                }
                /**
                 * empty result actually returns with the timez of request
                 * hence we check whether sync_state exists as well
                 */
                if (!result || !result.sync_state) {
                        var msg = 'no replication status';
                        var err2 = new verror.VError(msg);
                        return callback(err2);
                }
                var sentLocation = result.sent_location.split('/')[1];
                sentLocation = parseInt(sentLocation, 16);
                var flushLocation = result.flush_location.split('/')[1];
                flushLocation = parseInt(flushLocation, 16);

                log.info({
                        primary: sentLocation,
                        standby: flushLocation
                        }, 'PostgresMgr.checkReplStatus: xlog locations are');

                if (sentLocation === flushLocation) {
                        log.info('exiting chekReplStatus: ' +
                                'synchronous standby caught up');
                        return callback(null, true, flushLocation);
                } else {
                        log.info({
                                row: result
                        }, 'stil waiting for synchronous standby to catch up');
                        return callback(null, null, flushLocation);
                }

        });

        return (undefined);
}

