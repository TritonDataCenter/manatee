/**
 * @overview The PostgreSQL wrapper. Handles all interactions with the
 * underlying PG process.
 * @copyright Copyright (c) 2013, Joyent, Inc. All rights reserved.
 *
 *                   _.---.._
 *      _        _.-' \  \    ''-.
 *    .'  '-,_.-'   /  /  /       '''.
 *   (       _                     o  :
 *    '._ .-'  '-._         \  \-  ---]
 *                  '-.___.-')  )..-'
 *                           (_/
 */
var assert = require('assert-plus');
var backoff = require('backoff');
var ZfsClient = require('./zfsClient');
var ConfParser = require('./confParser');
var EventEmitter = require('events').EventEmitter;
var fs = require('fs');
var pg = require('pg');
var Client = pg.Client;
var shelljs = require('shelljs');
var spawn = require('child_process').spawn;
var exec = require('child_process').exec;
var once = require('once');
var posix = require('posix');
var sprintf = require('util').format;
var SnapShotter = require('./snapShotter');
var SyncStateChecker = require('./syncStateChecker.js');
var url = require('url');
var util = require('util');
var vasync = require('vasync');
var verror = require('verror');

/**
 * The manager which manages interactions with PostgreSQL.
 * Responsible for initializing, starting, stopping, and health checking a
 * running postgres instance.
 *
 * @constructor
 *
 * @param {object} options Options object.
 * @param {Bunyan} options.log Bunyan logger.
 * @param {string} options.dataDir Data directory of the PostgreSQL instance.
 * @param {string} options.postgresPath Path to the postgres binary.
 * @param {string} options.pgInitDbPath Path to the initdb binary.
 * @param {string} options.hbaConf Path to the hba config.
 * @param {string} options.postgresConf Path to the PG config.
 * @param {string} options.recoveryConf Path to the PG recovery config.
 * @param {string} options.url URL of this PG instance, e.g.
 * tcp://postgres@10.0.0.0:5432/postgres
 * @param {string} options.dbUser PG user, e.g. postgres.
 * @param {string} options.zfsClientCfg ZFS client config.
 * @param {string} options.snapShotterCfg Snapshotter config.
 * @param {string} options.healthChkInterval Interval of the PG health check in
 * ms.
 * @param {string} options.healthChkTimeout Timeout of the PG health check. If
 * this timeout is exceeded, we assume the PG instance to be dead.
 * @param {string} options.opsTimeout Timeout of init, start and restart
 * operations.
 * @param {string} options.replicationTimeout When a synchronous standby joins,
 * it has to make forward progress before exceeding this timeout.
 * @param {string} options.oneNodeWriteMode Enable writes when there's only 1
 * node in the shard.
 * @param {string} options.syncStateCheckerCfg Sync state checker config.
 *
 * @throws {Error} If the options object is malformed.
 */
function PostgresMgr(options) {
    assert.object(options, 'options');
    assert.object(options.log, 'options.log');

    assert.string(options.dataDir, 'options.dataDir');
    assert.string(options.dbUser, 'options.dbUser');
    assert.string(options.hbaConf, 'options.hbaConf');
    assert.number(options.healthChkInterval, 'options.healthChkInterval');
    assert.number(options.healthChkTimeout, 'options.healthChkTimeout');
    assert.number(options.opsTimeout, 'options.opsTimeout');
    assert.string(options.pgInitDbPath, 'options.pgInitDbPath');
    assert.string(options.postgresConf, 'options.postgresConf');
    assert.string(options.postgresPath, 'options.postgresPath');
    assert.string(options.recoveryConf, 'options.recoveryConf');
    assert.number(options.replicationTimeout, 'options.replicationTimeout');
    assert.object(options.snapShotterCfg, 'options.snapShotterCfg');
    assert.object(options.syncStateCheckerCfg, 'options.syncStateCheckerCfg');
    assert.string(options.url, 'options.url');
    assert.object(options.zfsClientCfg, 'options.zfsClientCfg');

    assert.optionalBool(options.oneNodeWriteMode, 'options.oneNodeWriteMode');

    EventEmitter.call(this);

    /** @type {Bunyan} The bunyan log object */
    this._log = options.log.child({component: 'PostgresMgr'}, true);
    var log = this._log;
    var self = this;

    this._postgres = null; /* The child postgres process */

    /** @type {string} The dir on disk where the postgres instance is located */
    this._dataDir = options.dataDir;

    /*
     * paths to the postgres commands
     */
    /** @type {string} Path to the pg_init binary. */
    this._pgInitDbPath = options.pgInitDbPath;
    /** @type {string} Path to the postgres binary */
    this._postgresPath = options.postgresPath;

    /*
     * Paths to the pg configs
     */
    /** @type {string} Path to the master hba config file */
    this._hbaConf = options.hbaConf;
    /** @type {string} Path to the master postgres config file */
    this._postgresConf = options.postgresConf;
    /** @type {string} Path to the master recovery config file */
    this._recoveryConf = options.recoveryConf;
    /** @type {string} Path to the hba config file */
    this._hbaConfPath = self._dataDir + '/' + 'pg_hba.conf';
    /** @type {string} Path to the postgres config file */
    this._postgresConfPath = self._dataDir + '/' + 'postgresql.conf';
    /** @type {string} Path to the recovery config file */
    this._recoveryConfPath = self._dataDir + '/' + 'recovery.conf';

    /** @type {url} The URL of this postgres instance */
    this._url = url.parse(options.url);
    /** @type {string} The postgres user */
    this._dbUser = options.dbUser;
    /** @type {number} The postgres user uid */
    this._dbUserId = posix.getpwnam(self._dbUser).uid;

    /** @type {object} Configs for the backup client */
    this._zfsClientCfg = options.zfsClientCfg;
    self._zfsClientCfg.dbUser = options.dbUser;

    /** @type {SnapShotter} The pg zfs snapshotter.  */
    this._snapShotter = new SnapShotter(options.snapShotterCfg);

    /*
     * The health check configs.
     */
    /** @type {number} health check timeout in ms */
    this._healthChkTimeout = options.healthChkTimeout;
    /** @type {number} health check interval in ms */
    this._healthChkInterval = options.healthChkInterval;
    /** @type {object} health check intervalId */
    this._healthChkIntervalId = null;
    /** @type {number} timestamp of the last successful healthcheck. */
    this._lastHealthChkTime = Date.now();

    /**
     * @type {number}
     * Postgres operation timeout in ms. Any postgres operation e.g. init,
     * start, restart, will fail upon exceeding this timeout.
     */
    this._opsTimeout = options.opsTimeout;

    /**
     * @type {number}
     * Postgres replication timeout in ms. If a standby hasn't caught up with
     * the primary in this time frame, then this shard may have WAL corruption
     * and is put into a read only state.
     */
    this._replicationTimeout = options.replicationTimeout;

    /**
     * @type {boolean}
     * Enable writes when there's only one node in the shard? This is dangerous
     * and should be avoided as this will cause WAL corruption
     */
    this._oneNodeWriteMode = options.oneNodeWriteMode || false;

    /** @type {pg.Client} pg client used for health checks */
    this._pgClient = null;

    /**
     * @type {SyncStateChecker}
     * Sync state checker used to check replication state
     */
    this._syncStateChecker = new SyncStateChecker(options.syncStateCheckerCfg);

    /*
     * MANATEE-171 delete recovery.conf if it exists. The old recovery.conf
     * could possibly cause us to start slaving from an unintended primary.
     */
    try {
        fs.unlinkSync(self._recoveryConfPath);
    } catch (e) {
        if (e.code !== 'ENOENT') {
            self._log.error({conf: self._recoveryConfPath},
                            'PostgresMgr.new: unable to remove recovery.conf');
            throw new verror.VError(e,
                'PostgresMgr.new: unable to remove recovery.conf');
        }
    }

    log.trace('new postgres manager', options);
}

module.exports = PostgresMgr;
util.inherits(PostgresMgr, EventEmitter);

/**
 * @constant
 * @type {string}
 * @default
 */
PostgresMgr.prototype.SYNCHRONOUS_STANDBY_NAMES = 'synchronous_standby_names';

/**
 * @constant
 * @type {string}
 * @default
 */
PostgresMgr.prototype.SYNCHRONOUS_COMMIT = 'synchronous_commit';

/**
 * @constant
 * @type {string}
 * @default
 */
PostgresMgr.prototype.PRIMARY_CONNINFO = 'primary_conninfo';

/**
 * @constant
 * @type {string}
 * @default
 */
PostgresMgr.prototype.READ_ONLY = 'default_transaction_read_only';

/**
 * postgresql.conf values
 * @constant
 * @type {string}
 * @default
 */
PostgresMgr.prototype.PRIMARY_CONNINFO_STR =
    '\'host=%s port=%s user=%s application_name=%s\'';

/**
 * replication status query.
 * @constant
 * @type {string}
 * @default
 */
PostgresMgr.prototype.PG_STAT_REPLICATION =
    'select * from pg_stat_replication where application_name = \'%s\'';

/**
 * @callback PostgresMgr-cb
 * @param {Error} error
 */

/**
 * Shut down the current PG instance.
 * @param {PostgresMgr-cb} callback
 */
PostgresMgr.prototype.shutdown = function shutdown(callback) {
    var self = this;
    var log = self._log;

    log.info('PostgresMgr.shutdown: entering');

    vasync.pipeline({funcs: [
        function _stopHealthCheck(_, cb) {
            self._stopHealthCheck(cb);
        },
        function _stop(_, cb) {
            self._stop(cb);
        }
    ], arg: {}}, function (err, results) {
        log.info({err: err, results: err ? results: null},
                 'PostgresMgr.shutdown: exiting');
        return callback(err);
    });
};

/**
 * Transition the postgres instance to read only mode, disconnected from all
 * other peers.
 * @param {PostgresMgr-cb} callback
 */
PostgresMgr.prototype.readOnly = function readOnly(callback) {
    var self = this;
    var log = self._log;

    log.info({
        url: self._url
    }, 'PostgresMgr.readonly: entering.');

    vasync.pipeline({funcs: [
        function _stopSyncCheck(_, cb) {
            self._syncStateChecker.stop(false, cb);
        },
        function _stopHealthCheck(_, cb) {
            self._stopHealthCheck(cb);
        },
        function _initDb(_, cb) {
            self._initDb(cb);
        },
        function _deleteRecoveryConf(_, cb) {
            fs.unlink(self._recoveryConfPath, function (e) {
                if (e && e.code !== 'ENOENT') {
                    return cb(e);
                } else {
                    return cb();
                }
            });
        },
        function _updateConfigs(_, cb) {
            var confOpts = {};
            confOpts[self.READ_ONLY] = 'on';
            self._updatePgConf(confOpts, cb);
        },
        function _restart(_, cb) {
            self._restart(cb);
        },
        function _startHealthCheck(_, cb) {
            self._startHealthCheck(cb);
        }
    ], arg: {}}, function (err) {
        if (err) {
            log.error({
                err: err,
                url: self._url
            }, 'PostgresMgr.readOnly: error');
            return callback(err);
        } else {
            log.info({
                url: self._url
            }, 'PostgresMgr.readOnly: complete');
            return callback();
        }
    });
};

/**
 * Transition the PostgreSQL instance to primary mode.
 *
 * @param {String} stdby The standby.
 * @param {PostgresMgr-cb} callback
 */
PostgresMgr.prototype.primary = function primary(stdby, callback) {
    var self = this;
    var log = self._log;

    log.info({
        url: self._url,
        standby: stdby
    }, 'PostgresMgr.primary: entering.');

    var replErrMsg = 'could not verify standby replication status, ' +
        'possible WAL corruption remaining in readonly mode';

    var checkReplEmitter;
    // we don't check for replication status if a standby doesn't exist.
    var finishedCheckRepl = stdby ? false : true;
    var primaryEmitter = new EventEmitter();
    primaryEmitter.cancel = function cancel() {
        log.info({
            finishedCheckRepl: finishedCheckRepl
        }, 'PostgresMgr.primary: cancelling operation');
        // We only try and cancel the replication check. But only after
        if (!finishedCheckRepl) {
            if (checkReplEmitter) {
                log.info('PostgresMgr.primary: replication check ' +
                         'started, cancelling check');
                checkReplEmitter.cancel();
            } else {
                log.info('PostgresMgr.primary: replication check not ' +
                         'started, trying again in 1s.');
                setTimeout(cancel, 1000);
            }
        }
    };

    vasync.pipeline({funcs: [
        function _stopSyncCheck(_, cb) {
            self._syncStateChecker.stop(true, cb);
        },
        function _writePrimaryCookie(_, cb) {
            self._syncStateChecker.writePrimaryCookie(cb);
        },
        function _stopHealthCheck(_, cb) {
            self._stopHealthCheck(cb);
        },
        function _initDb(_, cb) {
            self._initDb(cb);
        },
        function _deleteRecoveryConf(_, cb) {
            fs.unlink(self._recoveryConfPath, function (e) {
                if (e && e.code !== 'ENOENT') {
                    return cb(e);
                } else {
                    return cb();
                }
            });
        },
        function _updateConfigs(_, cb) {
            var confOpts = {};
            confOpts[self.SYNCHRONOUS_COMMIT] = 'remote_write';
            if (!self._oneNodeWriteMode) {
                confOpts[self.READ_ONLY] = 'on';
            } else {
                log.warn('enable write mode with only one ' +
                'node, may cause WAL corruption!');
            }
            self._updatePgConf(confOpts, cb);
        },
        function _restart(_, cb) {
            self._restart(cb);
        },
        function _snapshot(_, cb) {
            self._snapShotter.createSnapshot(Date.now(), cb);
        },
        /**
         * The standby is only updated after the snapshot operation.  This is
         * because if Postgres is started with a sync standby, and the standby
         * dies, the snapshot operation will hang, causing the entire process
         * to hang. Thus, we update the standby only after the snapshot has
         * been taken, and send sighup to postgres to pick up the new standby.
         */
        function _updateStandby(_, cb) {
            if (stdby) {
                var confOpts = {};
                confOpts[self.SYNCHRONOUS_COMMIT] = 'remote_write';
                confOpts[self.SYNCHRONOUS_STANDBY_NAMES] =
                '\'' + stdby + '\'';
                confOpts[self.READ_ONLY] = 'on';

                self._updatePgConf(confOpts, cb);
            } else {
                cb();
            }
        },
        /**
         * Only perform the next 3 functions if there is a standby, if the
         * standby expired we actually want to stay in readonly mode, so we
         * skip.
         */
        function _checkReplStatus(_, cb) {
            if (stdby) {
                primaryEmitter.startedCheckRepl = true;
                checkReplEmitter = self._checkRepl(stdby);
                checkReplEmitter.once('error', function (err) {
                    log.fatal({err: err}, replErrMsg);
                    err.__walCorruption = true;
                    return cb(err);
                });

                checkReplEmitter.once('done', cb);
            } else {
                cb();
            }
        },
        function _enableWrites(_, cb) {
            if (stdby) {
                var confOpts = {};
                confOpts[self.SYNCHRONOUS_COMMIT] = 'remote_write';
                confOpts[self.SYNCHRONOUS_STANDBY_NAMES] =
                '\'' + stdby + '\'';
                self._updatePgConf(confOpts, cb);
            } else {
                cb();
            }
        },
        function _sighup(_, cb) {
            if (stdby) {
                self._sighup(cb);
            } else {
                cb();
            }
        },
        function _startHealthCheck(_, cb) {
            self._startHealthCheck(cb);
        }
    ], arg: {}}, function (err) {
        if (err) {
            log.error({
                err: err,
                standby: stdby,
                url: self._url
            }, 'PostgresMgr.primary: error');
            primaryEmitter.emit('error', err);
        } else {
            log.info({
                standby: stdby,
                url: self._url
            }, 'PostgresMgr.primary: complete');
            primaryEmitter.emit('done');
        }
    });

    return primaryEmitter;
};

/**
 * Updates the standby of the current node. This assumes the current node is
 * already a primary. This does only sends SIGHUP to postgres, not SIGINT.
 *
 * @param {String} stdby The standby.
 * @param {PostgresMgr-cb} callback
 */
PostgresMgr.prototype.updateStandby = function updateStandby(stdby) {
    var self = this;
    var log = self._log;

    log.info({
        url: self._url,
        standby: stdby
    }, 'PostgresMgr.updateStandby: entering.');

    var replErrMsg = 'could not verify standby replication status, ' +
        'possible WAL corruption remaining in readonly mode';

    var checkReplEmitter;
    // we don't check for replication status if a standby doesn't exist.
    var finishedCheckRepl = stdby ? false : true;
    var updateStandbyEmitter = new EventEmitter();
    updateStandbyEmitter.cancel = function cancel() {
        log.info({
            finishedCheckRepl: finishedCheckRepl
        }, 'PostgresMgr.updateStandby: cancelling operation');
        // We only try and cancel the replication check.
        if (!finishedCheckRepl) {
            if (checkReplEmitter) {
                log.info('PostgresMgr.updateStandby: replication check ' +
                         'started, cancelling check');
                checkReplEmitter.cancel();
            } else {
                log.info('PostgresMgr.updateStandby: replication check not ' +
                         'started, trying again in 1s.');
                setTimeout(cancel, 1000);
            }
        }
    };

    vasync.pipeline({funcs: [
        function _stopHealthCheck(_, cb) {
            self._stopHealthCheck(cb);
        },
        function _updateConfigs(_, cb) {
            var confOpts = {};
            confOpts[self.SYNCHRONOUS_COMMIT] = 'remote_write';
            if (stdby) {
                confOpts[self.SYNCHRONOUS_STANDBY_NAMES] = '\'' + stdby + '\'';
                // if there is a standby, we always want to stay in read-only
                // mode
                confOpts[self.READ_ONLY] = 'on';
            } else if (!self._oneNodeWriteMode) {
                confOpts[self.READ_ONLY] = 'on';
            } else {
                log.warn('enable write mode with only one node, may cause ' +
                         'WAL corruption!');
            }

            self._updatePgConf(confOpts, cb);
        },
        function _sighup(_, cb) {
            self._sighup(cb);
        },
        /**
         * Only perform the next 3 functions if there is a standby, if the
         * standby expired we actually want to stay in readonly mode, so we
         * skip.
         */
        function _checkReplStatus(_, cb) {
            if (stdby) {
                checkReplEmitter = self._checkRepl(stdby);
                checkReplEmitter.once('error', function (err) {
                    log.fatal({err: err}, replErrMsg);
                    err.__walCorruption = true;
                    return cb(err);
                });

                checkReplEmitter.once('done', cb);
            } else {
                cb();
            }
        },
        function _enableWrites(_, cb) {
            if (stdby) {
                var confOpts = {};
                confOpts[self.SYNCHRONOUS_COMMIT] = 'remote_write';
                confOpts[self.SYNCHRONOUS_STANDBY_NAMES] = '\'' + stdby + '\'';
                self._updatePgConf(confOpts, cb);
            } else {
                cb();
            }
        },
        function _sighupAgain(_, cb) {
            if (stdby) {
                self._sighup(cb);
            } else {
                cb();
            }
        },
        // always re-start healthcheck
        function _startHealthCheck(_, cb) {
            self._startHealthCheck(cb);
        }
    ], arg: {}}, function (err) {
        if (err) {
            log.info({
                standby: stdby,
                url: self._url
            }, 'PostgresMgr.updateStandby: error');
            updateStandbyEmitter.emit('error', err);
        } else {
            log.info({
                standby: stdby,
                url: self._url
            }, 'PostgresMgr.updateStandby: complete');
            updateStandbyEmitter.emit('done');
        }
    });

    return updateStandbyEmitter;
};

/**
 * Transitions a postgres instance to standby state.
 *
 * @param {string} primUrl The postgres url of the primary.
 * @param {string} backupUrl The http url of the primary's backup service.
 * @param {PostgresMgr-cb} callback
 */
PostgresMgr.prototype.standby = function standby(primUrl, backupUrl, callback) {
    var self = this;
    var log = self._log;
    var primaryUrl = url.parse(primUrl);
    var backupSnapshot;

    log.info({
        primaryUrl: primaryUrl.href,
        backupUrl: backupUrl
    }, 'PostgresMgr.standby: entering');

    self._zfsClientCfg.serverUrl = backupUrl;
    var zfsClient = new ZfsClient(self._zfsClientCfg);

    /**
     * Update the primary connection info in recovery.conf
     */
    function updatePrimaryConnInfo(cb) {
        var opts = {};
        var value = sprintf(
            self.PRIMARY_CONNINFO_STR,
            primaryUrl.hostname,
            primaryUrl.port,
            primaryUrl.auth,
            self._url.href);
        opts[self.PRIMARY_CONNINFO] = value;
        self._updateRecoveryConf(opts, cb);
    }

    /**
     * Restores the current postgres instance from the primary via zfs_recv.
     */
    function restore(cb) {
        log.info({
            zfsClientCfg: self._zfsClientCfg
        }, 'PostgresMgr.standby: restoring db from primary');

        zfsClient.restore(function (err2, snapshot) {
            backupSnapshot = snapshot;
            if (err2) {
                log.info({
                    err: err2,
                    backupUrl: backupUrl
                }, 'PostgresMgr.standby: could not restore from primary');
                return cb(err2);
            }
            var cmd = 'chown -R ' + self._dbUser + ' ' + self._dataDir;
            log.info({
                cmd: cmd
            }, 'PostgresMgr.standby: finished backup, chowning datadir');
            exec(cmd, cb);
        });
    }

    /**
     * If steps 1-4 error out, then a restore of the database is taken from the
     * primary. This is controlled by the _.isRestore flag attached to the
     * vasync args.
     */
    vasync.pipeline({funcs: [
        function _startSyncCheck(_, cb) {
            self._syncStateChecker.start(primUrl, self._url, cb);
        },
        function _stopHealthCheck(_, cb) {
            self._stopHealthCheck(cb);
        },
        // have to stop postgres here first such that we can assert the dataset,
        // other wise some actions will fail with EBUSY
        function stopPostgres(_, cb) {
            log.info('PostgresMgr.initDb: stop postgres');
            self._stop(cb);
        },
        // fix for MANATEE-90, always check that the dataset exists before
        // starting postgres
        function _assertDataset(_, cb) {
            zfsClient.assertDataset(cb);
        },
        // update primary_conninfo to point to the new (host, port) pair
        function _updatePrimaryConnInfo(_, cb) {
            updatePrimaryConnInfo(function (err) {
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
                opts[self.SYNCHRONOUS_COMMIT] = 'off';
                self._updatePgConf(opts, function (err) {
                    _.isRestore = err;
                    return cb();
                });
            }
        },
        // restart pg to enable the config changes.
        function _restart(_, cb) {
            if (_.isRestore) {
                return cb();
            } else {
                self._restart(function (err) {
                    _.isRestore = err;
                    return cb();
                });
            }
        },
        // following run only if _.isRestore is needed
        function _restore(_, cb) {
            if (!_.isRestore) {
                return cb();
            } else {
                restore(function (err) {
                    // restore the original backup if zfs recv fails.
                    if (err) {
                        zfsClient.restoreDataset(backupSnapshot, function () {
                            return cb(err);
                        });
                    } else {
                        return cb();
                    }
                });
            }
        },
        // update primary info since the zfs dataset from the primary will not
        // contain standby information
        function _updatePrimaryConnInfoAgain(_, cb) {
            if (!_.isRestore) {
                return cb();
            } else {
                updatePrimaryConnInfo(cb);
            }
        },
        // again because the restore from the primary will have this set to
        // enabled
        function _setSyncCommitOffAgain(_, cb) {
            if (!_.isRestore) {
                return cb();
            } else {
                var opts = {};
                opts[self.SYNCHRONOUS_COMMIT] = 'off';
                self._updatePgConf(opts, function (err) {
                    if (err) {
                        _.isRestore = err;
                    }
                    return cb();
                });
            }
        },
        function _restartAgain(_, cb) {
            if (!_.isRestore) {
                return cb();
            } else {
                self._restart(function (err) {
                    // restore the original snapshot if we can't restart, which
                    // usuallly indicates corruption in the received dataset
                    if (err) {
                        zfsClient.restoreDataset(backupSnapshot, function () {
                            return cb(err);
                        });
                    } else {
                        return cb();
                    }
                });
            }
        },
        // if the restore is successful, then destroy the backupdataset
        function _destroyBackupDatset(_, cb) {
            if (!_.isRestore) {
                return cb();
            } else {
                var cmd = 'zfs destroy -r ' +
                    backupSnapshot.split('@')[0];
                log.info({cmd: cmd}, 'PostgresMgr.standby: exec');
                exec(cmd, cb);
            }
        },
        // start health check irregardless of restore
        function _startHealthCheck(_, cb) {
            self._startHealthCheck(cb);
        }
    ], arg: {}}, function (err) {
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
 * @callback PostgresMgr-masterCb
 * @param {bool} canStart
 */

/**
 * Checks whether this PostgreSQL instance can be a master.
 *
 * @param {PostgresMgr-masterCb} cb
 */
PostgresMgr.prototype.canStartAsMaster = function canStartAsMaster(cb) {
    var self = this;
    var log = self._log;

    assert.func(cb, 'cb');

    log.info('PostgresMgr.canStartAsMaster: entering');

    self._syncStateChecker.canStartAsMaster(cb);
};

/**
 * Stop the sync state checker.
 *
 * @param {PostgresMgr-cb} callback
 */
PostgresMgr.prototype.stopSyncChecker = function stopSyncChecker(delCookie, cb)
{
    var self = this;
    var log = self._log;

    assert.func(cb, 'cb');

    log.info('PostgresMgr.stopSyncChecker: entering');
    self._syncStateChecker.stop(delCookie, cb);
};

/**
 * @callback PostgresMgr-lastRoleCb
 * @param {string} role
 */

/**
 * Get the last role of this PostgreSQL instance.
 * @param {PostgresMgr-lastRoleCb} cb
 */
PostgresMgr.prototype.getLastRole = function getLastRole(cb) {
    var self = this;
    self._syncStateChecker.getLastRole(cb);
};

/**
 * @return {string} The PostgreSQL URL, e.g. tcp://postgres@10.0.0.1:5324/
 */
PostgresMgr.prototype.getUrl = function getUrl() {
    var self = this;
    assert.object(self._url, 'this.url');
    return self._url;
};

/**
 * #@+
 * @private
 * @memberOf PostgresMgr
 */

/**
 * Stops the running postgres instance.
 *
 * Sends the following signals in order:
 * SIGTERM, SIGINT, SIGQUIT, SIGKILL
 * The first will wait for all clients to terminate before quitting, the second
 * will forcefully disconnect all clients, and the third will quit immediately
 * without proper shutdown, resulting in a recovery run during restart.
 *
 * @param {PostgresMgr-cb} callback
 */
PostgresMgr.prototype._stop = function (callback) {
    var self = this;
    var log = self._log;
    callback = once(callback);
    log.info('PostgresMgr.stop: entering');

    var successful;
    var postgres = self._postgres;
    if (!postgres) {
        log.info({
            postgresHandle: postgres,
            datadir: self._dataDir
        }, 'PostgresMgr.stop: exiting, postgres handle DNE, was pg started by' +
            ' another process?');

        return callback();
    }
    // MANATEE-81: unregister previous exit listener on the postgres handle.
    postgres.removeAllListeners('exit');

    postgres.once('exit', function (code, signal) {
        // always remove reference to postgres handle on exit.
        self._postgres = null;
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
    setTimeout(function () {
        if (!successful) {
            log.info('PostgresMgr.stop: trying SIGQUIT');
            postgres.kill('SIGQUIT');
        }
        // set another timeout and SIGKILL
        setTimeout(function () {
            if (!successful) {
                log.info('PostgresMgr.stop: trying SIGKILL');
                postgres.kill('SIGKILL');
            }
            // set another timeout and return error
            setTimeout(function () {
                if (!successful) {
                    log.error('PostgresMgr.stop: failed');
                    var err2 = new verror.VError('SIGKILL failed');
                    postgres.removeAllListeners('exit');
                    return callback(err2);
                }
            });
        }, self._opsTimeout);

    }, self._opsTimeout);
};

/**
 * Starts the periodic health checking of the pg instance.  emits error if
 * healthchk fails
 *
 * @param {PostgresMgr-cb} callback
 */
PostgresMgr.prototype._startHealthCheck = function (callback) {
    var self = this;
    var log = self._log;
    log.info('Postgresman.starthealthCheck: entering');

    if (self._healthChkIntervalId) {
        log.info('Postgresman.starthealthCheck: health check already running');
        return callback();
    } else {
        self._lastHealthChkTime = Date.now();
        self._healthChkIntervalId = setInterval(function () {
            // set a timeout in case _health() doesn't return in time
            var timeoutId = setTimeout(function () {
                /**
                 * Error event, emitted when there's an unrecoverable error
                 * with the PostgreSQL instance.
                 * Usually this is because of:
                 * - The healthcheck has failed.
                 * - PostgreSQL exited on its own.
                 * - The manager was unable to start PostgreSQL.
                 *
                 * @event PostgresMgr#error
                 * @type {verror.VError} error
                 */
                self.emit('error',
                    new verror.VError('PostgresMgr._startHealthCheck() timed ' +
                                      'out'));
            }, self._healthChkTimeout);

            self._health(healthHandler.bind(self, timeoutId));

        }, self._healthChkInterval);

        // return callback once healthcheck has been dispatched
        return callback();
    }

    /**
     * only error out when we've exceeded the timeout
     */
    function healthHandler(timeoutId, err) {
        log.trace({err: err}, 'postgresMgr.startHealthCheck.health: returned');
        clearTimeout(timeoutId);
        if (err) {
            var timeElapsed = Date.now() - self._lastHealthChkTime;
            log.debug({
                err: err,
                timeElapsed: timeElapsed,
                timeOut: self._healthChkTimeout
            }, 'PostgresMgr.health: failed');

            if (timeElapsed > self._healthChkTimeout) {
                var msg = 'PostgresMgr.health: health check timed out';
                log.error({
                    err: err,
                    timeElapsed: timeElapsed,
                    timeOut: self._healthChkTimeout
                }, msg);
                /**
                 * Error event, emitted when there's an unrecoverable error
                 * with the PostgreSQL instance.
                 * Usually this is because of:
                 * - The healthcheck has failed.
                 * - PostgreSQL exited on its own.
                 * - The manager was unable to start PostgreSQL.
                 *
                 * @event PostgresMgr#error
                 * @type {verror.VError} error
                 */
                self.emit('error', new verror.VError(err, msg));

            }
        } else {
            self._lastHealthChkTime = Date.now();
        }
    }
};

PostgresMgr.prototype._stopHealthCheck = function (callback) {
    var self = this;
    var log = self._log;
    log.info('Postgresman.stopHealthCheck: entering');

    if (self._healthChkIntervalId) {
        clearInterval(self._healthChkIntervalId);
        self._healthChkIntervalId = null;
    } else {
        log.info('Postgresman.stopHealthCheck: not running');
    }

    return callback();
};

/**
 * Start the postgres instance.
 * @param {function} callback The callback of the form f(err, process).
 */
PostgresMgr.prototype._start = function start(cb) {
    var self = this;
    var log = self._log;
    var msg = '';
    var intervalId = null;
    cb = once(cb);

    /**
     * Always reset and clear the healthcheck before callback.
     * This callback is invoked when the child PG process has started.
     */
    var callback = once(function (err, pg2) {
        clearInterval(intervalId);
        log.info('clearing healthcheck');

        cb(err, pg2);
    });

    log.info({
        postgresPath: self._postgresPath,
        dataDir: self._dataDir
    }, 'PostgresMgr.start: entering');

    // delete postmaster.pid if it exists.
    try {
        fs.unlinkSync(self._dataDir + '/postmaster.pid');
    } catch (e) {
        // ignore errors since postmaster might not exist in the first place
    }

    var postgres = spawn(self._postgresPath, ['-D', self._dataDir],
                         {uid: self._dbUserId});
    self._postgres = postgres;

    postgres.stdout.once('data', function (data) {
        log.trace('postgres stdout: ', data.toString());
    });

    postgres.stderr.once('data', function (data) {
        var dataStr = data.toString();
        log.trace('postgres stderr: ', dataStr);
        if (msg) {
            msg += dataStr;
        } else {
            msg = dataStr;
        }
        msg += data;
    });

    postgres.on('exit', function (code, signal) {
        // remove reference to postgres handle on exit.
        self._postgres = null;
        var err = new verror.VError(msg, code);
        log.info({
            postgresPath: self._postgresPath,
            dataDir: self._dataDir,
            code: code,
            signal: signal,
            err: err
        }, 'Postgresman.start: postgres -D exited with err');

        /*
         * fix for MANTA-997. This callback when invoked more than once
         * indicates that postgres has exited unexpectedly -- usually as a
         * result of unexpected pg crash.  Since postgres is started as a child
         * process, when it unexpectedly exits, start(), which has already
         * returned when postgres was first started, will return another
         * callback indicating postgres has exited.  If this callback is
         * invoked, it manifests itself by causing vasync to throw a pipeline
         * error.  What we really want is to indicate this as fatal and exit
         * manatee.
         */
        if (callback.called) {
            var errMsg = 'postgres exited unexpectedly, ' +
                'exiting manatee, please check for pg core dumps.';
            log.fatal(errMsg);
            /**
             * Error event, emitted when there's an unrecoverable error
             * with the PostgreSQL instance.
             * Usually this is because of:
             * - The healthcheck has failed.
             * - PostgreSQL exited on its own.
             * - The manager was unable to start PostgreSQL.
             *
             * @event PostgresMgr#error
             * @type {verror.VError} error
             */
            self.emit('error', new verror.VError(err, errMsg));
        }

        return callback(err);
    });

    // Wait for db to comeup via healthcheck
    var time = new Date().getTime();
    intervalId = setInterval(function () {
        self._health(function (err) {
            var timeSinceStart = new Date().getTime() - time;
            if (err) {
                log.info({
                    err: err,
                    timeSinceStart: timeSinceStart,
                    opsTimeout: self._opsTimeout,
                    postgresPath: self._postgresPath,
                    dataDir: self._dataDir
                }, 'Postgresman.start: db has not started');

                if (timeSinceStart > self._opsTimeout) {
                    log.info({
                        timeSinceStart: timeSinceStart,
                        opsTimeout: self._opsTimeout,
                        postgresPath: self._postgresPath,
                        dataDir: self._dataDir
                    }, 'Postgresman.start: start timeout');

                    self._stop(function () {
                        return callback(err, postgres);
                    });
                }
            } else {
                log.info({
                    timeSinceStart: timeSinceStart,
                    opsTimeout: self._opsTimeout,
                    postgresPath: self._postgresPath,
                    dataDir: self._dataDir
                }, 'Postgresman.start: db has started');
                return callback(null, postgres);
            }
        });
    }, 1000);
};

/**
 * Initializes the postgres data directory for a new DB. This can fail if the
 * db has already been initialized - this is okay, as startdb will fail if init
 * didn't finish succesfully.
 *
 * This function should only be called by the primary of the shard. Standbys
 * will not need to initialize but rather restore from a already running
 * primary.
 *
 * @param {function} callback The callback of the form f(err).
 */
PostgresMgr.prototype._initDb = function (callback) {
    var self = this;
    var log = self._log;
    log.info({
        dataDir: self._dataDir
    }, 'PostgresMgr.initDb: entering');

    vasync.pipeline({funcs: [
        // have to stop postgres here first such that we can assert the dataset,
        // other wise some actions will fail with EBUSY
        function stopPostgres(_, cb) {
            log.info('PostgresMgr.initDb: stop postgres');
            self._stop(cb);
        },
        // fix for MANATEE-90, always check that the dataset exists before
        // initializing postgres
        function assertDataset(_, cb) {
            log.info({dataset: self._zfsClientCfg.dataset},
                'PostgresMgr.initDb: assert dataset');
            var zfsClient = new ZfsClient(self._zfsClientCfg);
            zfsClient.assertDataset(cb);
        },
        function checkDataDirExists(_, cb) {
            log.info({datadir: self._dataDir},
                'PostgresMgr.initDb: check datadir exists');
            fs.stat(self._dataDir, function (err, stats) {
                if (err || !stats.isDirectory()) {
                    return cb(new verror.VError(err,
                        'postgres datadir ' + self._dataDir + ' DNE'));
                }

                return cb();
            });

        },
        function setDataDirOnwership(_, cb) {
            var cmd = 'chown -R ' + self._dbUser + ' '  + self._dataDir;
            log.info({cmd: cmd},
                'PostgresMgr.initDb: changing datadir ownership to postgres');
            exec(cmd, cb);
        },
        function setDataDirPerms(_, cb) {
            var cmd = 'chmod 700 ' + self._dataDir;
            log.info({cmd: cmd},
                'PostgresMgr.initDb: changing datadir perms to 700');
            exec(cmd, cb);

        },
        function _initDb(_, cb) {
            var cmd = 'sudo -u ' + self._dbUser + ' ' + self._pgInitDbPath +
                ' -D ' + self._dataDir;
            log.info({cmd: cmd}, 'PostgresMgr.initDb: initializing db');
            exec(cmd, function (err, stdout, stderr) {
                // ignore errors since the db could already be initialized
                log.info({
                    err: err,
                    stdout: stdout,
                    stderr: stderr,
                    dataDir: self._dataDir
                }, 'PostgresMgr.initDb: initdb returned');

                shelljs.cp('-f', self._hbaConf, self._dataDir + '/pg_hba.conf');
                log.info({
                    dataDir: self._dataDir,
                    postgresqlConf: self._postgresConf
                }, 'PostgresMgr.initDb: copying postgresql.conf to data dir');

                shelljs.cp('-f', self._postgresConf, self._dataDir +
                    '/postgresql.conf');

                return cb();
            });

        }
    ], arg: {}}, function (err) {
        log.info({err: err}, 'PostgresMgr.initDb: finished');
        return callback(err);
    });
};

PostgresMgr.prototype._queryDb = function (queryStr, callback) {
    var self = this;
    var log = self._log;
    callback = once(callback);
    log.trace({
        query: queryStr
    }, 'Postgresman.query: entering.');

    if (!self._pgClient) {
        self._pgClient = new Client(self._url.href);
        self._pgClient.once('error', function (err) {
            self._pgClient.removeAllListeners();
            log.trace({err: err}, 'got pg client error');
            // set the client to null on error so we can create a new client
            self._pgClient = null;
            return callback(new verror.VError(err,
                'error whilst querying postgres'));
        });
        self._pgClient.connect();
    }


    var query = self._pgClient.query(queryStr);
    var result = null;
    log.trace('querying', query);
    query.once('row', function (row) {
        log.trace({
            row: row
        }, 'got row');
        result = row;
    });

    query.once('error', function (err) {
        log.trace({
            err: err
        }, 'got err');
        var err2 = new verror.VError('error whilst querying postgres');
        // set the client to null on error so we can create a new client
        self._pgClient = null;
        return callback(err2);
    });

    query.once('end', function () {
        log.trace('query ended!');
        return callback(null, result);
    });
};

/**
 * Sends sighup to PostgreSQL.
 */
PostgresMgr.prototype._sighup = function (callback) {
    var self = this;
    var log = self._log;
    log.info('Postgresman.sighup: entering');

    var postgres = self._postgres;
    postgres.kill('SIGHUP');
    callback();
};

/**
 * Update keys in postgresql.conf. Note, keys in the current config not present
 * in the default config will be lost
 */
PostgresMgr.prototype._updatePgConf = function (options, cb) {
    var self = this;
    self._updateConf(options, self._postgresConf, self._postgresConfPath, cb);
};

/**
 * Update keys in recovery.conf. Note, keys in the current config not present
 * in the default config will be lost.
 */
PostgresMgr.prototype._updateRecoveryConf = function (options, cb) {
    var self = this;
    self._updateConf(options, self._recoveryConf, self._recoveryConfPath, cb);
};

PostgresMgr.prototype._updateConf = function (options, rpath, wpath, cb) {
    var self = this;
    var log = self._log;
    log.debug({
        options: options,
        rpath: rpath,
        wpath: wpath
    }, 'updating config');

    ConfParser.read(rpath, function (err, conf) {
        if (err) {
            log.info({
                err: err,
                options: options,
                postgresConf: rpath
            }, 'unable to read config');
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
    });
};

/**
 * Restarts the postgres instance. If no pg instance is running, this will just
 * start pg.
 * @param {function} callback The callback of the form f(err).
 */
PostgresMgr.prototype._restart = function (callback) {
    var self = this;
    var log = self._log;
    log.info('Postgresman.restart: entering');

    // check health first to see if db is running
    self._health(function (err) {
        if (err) {
            log.info('Postgresman.restart: db not running');
            return self._start(callback);
        } else {
            log.info('Postgresman.restart: db is running');
            self._stop(function (err2) {
                if (err2) {
                    return callback(err2);
                } else {
                    return self._start(callback);
                }
            });
        }
    });
};

/**
 * Check the health status of the running postgres db.
 * @param {function} callback The callback of the form f(err), where err
 * indicates an unhealthy db.
 */
PostgresMgr.prototype._health = function (callback) {
    var self = this;
    var log = self._log;
    log.trace('Postgresman.health: entering');
    self._queryDb('select current_time;', function (err) {
        if (err) {
            log.trace({err: err}, 'Postgresman.health: failed');
        }
        return callback(err);
    });
};

/**
 * check the replication status of the current pg node. returns error if
 * replication has failed.
 */
PostgresMgr.prototype._checkRepl = function (stdby) {
    var self = this;
    var log = self._log;
    var replReplayLoc = null;
    var replStartTime = Date.now();
    var timeoutId;
    log.info({standby: stdby}, 'PostgresMgr._checkRepl: entering');

    var checkReplEmitter = new EventEmitter();
    var cancel = false;
    checkReplEmitter.cancel = function () {
        log.info('PostgresMgr._checkRepl: cancelled, exiting');
        cancel = true;
        if (timeoutId) {
            clearTimeout(timeoutId);
        }
        checkReplEmitter.emit('done');
    };

    (function checkReplication() {
        self._checkReplStatus(stdby, function (err, _stop, replayLoc, myLoc) {
            if (cancel) {
                return;
            }
            if (err) {
                /*
                 * If we can't query the replication state, we just keep
                 * trying.  Importantly we do not count this as part of the
                 * replication timeout.  Generally this means the standby
                 * hasn't started or is unable to start.  This means that the
                 * standby will eventually time itself out and we will exit the
                 * loop since a new event will be emitted when the standby
                 * leaves the eleciton.
                 */
                log.info({err: err}, 'unable to query replication status');
                // reset the start time when we get error since we haven't
                // gotten any real replication information yet.
                replStartTime = Date.now();
                timeoutId = setTimeout(checkReplication, 1000);
                return;
            } else if (!replReplayLoc || replayLoc > replReplayLoc) {
                log.info({
                    oldReplayLoc: replReplayLoc,
                    currReplLoc: replayLoc
                }, 'replay row incremented, resetting startTime');
                replStartTime = Date.now();
                replReplayLoc = replayLoc;
            } else if (replReplayLoc > myLoc) {
                /*
                 * Fail fast if the remote replay location is > than our
                 * current location. This means that we (primary) is behind the
                 * sync. The sync will never catch up.
                 */
                checkReplEmitter.emit('error', new verror.VError(
                    'standby xlog location is greater than primary xlog ' +
                    'location.'));
                return;
            }

            var diffTime = Date.now() - replStartTime;
            // stop if caught up, return error if standby times out
            if (_stop) {
                log.info({
                    stop: _stop,
                    diffTime: diffTime,
                    oldReplayLoc: replReplayLoc,
                    currReplLoc: replayLoc
                }, 'PostgresMgr._checkRepl: done, stopping replication check');
                checkReplEmitter.emit('done');
                return;
            } else if (diffTime > self._replicationTimeout) {
                /*
                 * at this point, we've timed out trying to wait/query
                 * replication state, so we return error
                 */
                checkReplEmitter.emit('error',
                    new verror.VError('standby unable ot make forward ' +
                                      'progress'));
                return;
            } else {
                log.info({
                    stop: _stop,
                    diffTime: diffTime,
                    oldReplayLoc: replReplayLoc,
                    currReplLoc: replayLoc
                }, 'continuing replication check');
                timeoutId = setTimeout(checkReplication, 1000);
                return;
            }
        });
    })();

    return checkReplEmitter;
};

PostgresMgr.prototype._checkReplStatus = function (stdby, callback) {
    var self = this;
    var log = self._log;
    var query = sprintf(self.PG_STAT_REPLICATION, stdby);
    log.info({standby: stdby, query: query},
             'Postgresman.checkReplStatus: entering');
    self._queryDb(query, function (err, result) {
        log.debug({err: err, result: result}, 'returned from query');
        if (err) {
            return callback(new verror.VError(err,
                'unable to query replication stat'));
        }

        /*
         * empty result actually returns with the timez of request hence we
         * check whether sync_state exists as well
         */
        if (!result || !result.sync_state || !result.sent_location ||
            !result.flush_location) {
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
            log.info('exiting chekReplStatus: synchronous standby caught up');
            return callback(null, true, flushLocation, sentLocation);
        } else {
            log.info({
                row: result
            }, 'stil waiting for synchronous standby to catch up');
            return callback(null, null, flushLocation, sentLocation);
        }

    });
};

/** #@- */
