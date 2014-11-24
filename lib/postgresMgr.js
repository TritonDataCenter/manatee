/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2014, Joyent, Inc.
 */

/**
 * @overview The PostgreSQL wrapper. Handles all interactions with the
 * underlying PG process.
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

    // /manatee/pg/data
    assert.string(options.dataDir, 'options.dataDir');
    // postgres
    assert.string(options.dbUser, 'options.dbUser');
    // /opt/smartdc/manatee/etc/pg_hba.conf
    assert.string(options.hbaConf, 'options.hbaConf');
    // 10000
    assert.number(options.healthChkInterval, 'options.healthChkInterval');
    // 60000
    assert.number(options.healthChkTimeout, 'options.healthChkTimeout');
    // 300000
    assert.number(options.opsTimeout, 'options.opsTimeout');
    // /opt/local/bin/initdb
    assert.string(options.pgInitDbPath, 'options.pgInitDbPath');
    // /opt/smartdc/manatee/etc/postgresql.manta.coal.conf
    assert.string(options.postgresConf, 'options.postgresConf');
    // /opt/local/bin/postgres
    assert.string(options.postgresPath, 'options.postgresPath');
    // /opt/smartdc/manatee/etc/recovery.conf
    assert.string(options.recoveryConf, 'options.recoveryConf');
    // 60000
    assert.number(options.replicationTimeout, 'options.replicationTimeout');
    // [ Object object ]
    assert.object(options.snapShotterCfg, 'options.snapShotterCfg');
    // [ Object object ]
    assert.object(options.syncStateCheckerCfg, 'options.syncStateCheckerCfg');
    // tcp://postgres@10.77.77.8:5432/postgres
    assert.string(options.url, 'options.url');
    // [ Object object ]
    assert.object(options.zfsClientCfg, 'options.zfsClientCfg');
    // false
    assert.optionalBool(options.oneNodeWriteMode, 'options.oneNodeWriteMode');

    EventEmitter.call(this);

    /** @type {Bunyan} The bunyan log object */
    this._log = options.log.child({component: 'PostgresMgr'}, true);
    var log = this._log;
    var self = this;

    self._postgres = null; /* The child postgres process */

    /** @type {string} The dir on disk where the postgres instance is located */
    self._dataDir = options.dataDir;

    /*
     * paths to the postgres commands
     */
    /** @type {string} Path to the pg_init binary. */
    self._pgInitDbPath = options.pgInitDbPath;
    /** @type {string} Path to the postgres binary */
    self._postgresPath = options.postgresPath;

    /*
     * Paths to the pg configs
     */
    /** @type {string} Path to the master hba config file */
    self._hbaConf = options.hbaConf;
    /** @type {string} Path to the master postgres config file */
    self._postgresConf = options.postgresConf;
    /** @type {string} Path to the master recovery config file */
    self._recoveryConf = options.recoveryConf;
    /** @type {string} Path to the hba config file */
    self._hbaConfPath = self._dataDir + '/' + 'pg_hba.conf';
    /** @type {string} Path to the postgres config file */
    self._postgresConfPath = self._dataDir + '/' + 'postgresql.conf';
    /** @type {string} Path to the recovery config file */
    self._recoveryConfPath = self._dataDir + '/' + 'recovery.conf';

    /** @type {url} The URL of this postgres instance */
    self._url = url.parse(options.url);
    /** @type {string} The postgres user */
    self._dbUser = options.dbUser;
    /** @type {number} The postgres user uid */
    self._dbUserId = posix.getpwnam(self._dbUser).uid;

    /** @type {object} Configs for the backup client */
    self._zfsClientCfg = options.zfsClientCfg;
    /** @type {string} The postgres user */
    self._zfsClientCfg.dbUser = options.dbUser;
    /** @type {object} The handle to the zfs client */
    self._zfsClient = new ZfsClient(self._zfsClientCfg);

    /** @type {SnapShotter} The pg zfs snapshotter.  */
    self._snapShotter = new SnapShotter(options.snapShotterCfg);

    /*
     * The health check configs and state.
     */
    /** @type {number} health check timeout in ms */
    self._healthChkTimeout = options.healthChkTimeout;
    /** @type {number} health check interval in ms */
    self._healthChkInterval = options.healthChkInterval;
    /** @type {object} health check intervalId */
    self._healthChkIntervalId = null;
    /** @type {number} timestamp of the last healthcheck. */
    self._lastHealthChkTime = null;
    /** @type {boolean} whether postgres is known healthy. */
    self._healthy = null;
    /** @type {error} if !_healthy, the last health check error. */
    self._lastHealthChkErr = null;

    /**
     * @type {number}
     * Postgres operation timeout in ms. Any postgres operation e.g. init,
     * start, restart, will fail upon exceeding this timeout.
     */
    self._opsTimeout = options.opsTimeout;

    /**
     * @type {number}
     * Postgres replication timeout in ms. If a standby hasn't caught up with
     * the primary in this time frame, then this shard may have WAL corruption
     * and is put into a read only state.
     */
    self._replicationTimeout = options.replicationTimeout;

    /**
     * @type {boolean}
     * Enable writes when there's only one node in the shard? This is dangerous
     * and should be avoided as this will cause WAL corruption
     */
    self._oneNodeWriteMode = options.oneNodeWriteMode || false;

    /** @type {pg.Client} pg client used for health checks */
    self._pgClient = null;

    /**
     * Filled out on first reconfigure.
     */
    self._pgConfig = null;
    self._running = false;
    self._transitioning = false;
    self._appliedPgConfig = false;

    log.trace('new postgres manager', options);

    /**
     * Future-looking if this ends up decoupling the postgres process from the
     * manatee-sitter node process, there should be an init method that figures
     * out what the current state of postgres is and emit once the state is
     * known (self._pgConfig, etc).
     */
    setImmediate(function init() {
        self._log.info('PostgresMgr.startHealthCheck: entering');
        self._startHealthCheck(function () {
            self._log.info('PostgresMgr.startHealthCheck: exiting');
            //This emits 'false' indicating that postgres isn't online.
            self.emit('init', false);
        });
    });
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
 * Start up the PG instance.  Will return an error if postgres is already
 * running.  Postgres must have previously been reconfigured.
 *
 * @param {PostgresMgr-cb} callback
 */
PostgresMgr.prototype.start = function start(callback) {
    var self = this;
    var log = self._log;

    if (self._transitioning) {
        return (callback(new verror.VError('already transitioning')));
    }

    if (self._running) {
        return (callback(new verror.VError('postgres is already running')));
    }

    if (!self._pgConfig) {
        return (callback(new verror.VError(
            'postgres manager hasnt yet been configured')));
    }

    if (self._pgConfig.role === 'none') {
        return (callback(new verror.VError(
            'postgres manager role is none, not allowing start')));
    }

    log.info('PostgresMgr.start: entering');
    self._transitioning = true;
    self._reconfigure(function (err) {
        if (!err) {
            self._running = true;
        }
        log.info({err: err}, 'PostgresMgr.start: exiting');
        self._transitioning = false;
        return (callback(err));
    });
};


/**
 * Shut down the current PG instance.  Will return an error if postgres has
 * is not running.
 *
 * @param {PostgresMgr-cb} callback
 */
PostgresMgr.prototype.stop = function stop(callback) {
    var self = this;
    var log = self._log;

    if (self._transitioning) {
        return (callback(new verror.VError('already transitioning')));
    }

    if (!self._running) {
        return (callback(new verror.VError('postgres is not running')));
    }

    log.info('PostgresMgr.stop: entering');
    self._transitioning = true;
    vasync.pipeline({funcs: [
        function _stop(_, cb) {
            self._stop(cb);
        },
        function _setNotRunning(_, cb) {
            self._running = false;
            return (cb());
        }
    ], arg: {}}, function (err, results) {
        log.info({err: err, results: err ? results: null},
                 'PostgresMgr.stop: exiting');
        self._transitioning = false;
        return callback(err);
    });
};


/**
 * Reconfigures the Postgres instance.  Is a no-op if postgres is already
 * configured as specified.  May restart a running Postgres to pick up new
 * configuration.
 *
 * pgConfig is a non-null object representing postgres configuration.  It always
 * has this property:
 *
 * * role (string): one of 'primary', 'standby', or 'none'.
 *
 * If role is 'primary', then there's a 'downstream' property which contains the
 * pgUrl field and the backupUrl field for the corresponding manatee peer.
 *
 * If role is 'standby', then there's an 'upstream' property which contains the
 * pgUrl field and the backupUrl field for the corresponding manatee peer.
 *
 * If role is 'none', then replication is not configured at all, and 'upstream'
 * and 'downstream' are both null.
 *
 * The structures for "upstream" and "downstream" must have postgres connection
 * info as well as the backupUrl.  These structures may contain other fields
 * (which are ignored).  Examples:
 *
 *     {
 *         "role": "primary",
 *         "upstream": null,
 *         "downstream": {
 *             "pgUrl": "tcp://postgres@10.77.77.7:5432/postgres",
 *             "backupUrl": "http://10.77.77.7:12345"
 *         }
 *     }
 *
 *     {
 *         "role": "standby",
 *         "upstream": {
 *             "pgUrl": "tcp://postgres@10.77.77.7:5432/postgres",
 *             "backupUrl": "http://10.77.77.7:12345"
 *         },
 *         "downstream": null
 *     }
 *
 *     {
 *         "role": "none",
 *         "upstream": null,
 *         "downstream": null
 *     }
 *
 * @param {object} pgConfig As described above.
 * @param {PostgresMgr-cb} callback
 */
PostgresMgr.prototype.reconfigure = function reconfigure(pgConfig, callback) {
    var self = this;
    var log = self._log;

    function assertPeerIdentifier(peer) {
        assert.string(peer.pgUrl, 'peer.pgUrl');
        assert.string(peer.backupUrl, 'peer.backupUrl');
    }

    assert.object(pgConfig, 'pgConfig');
    assert.string(pgConfig.role, 'pgConfig.role');
    assert.ok(['primary', 'standby', 'none'].indexOf(pgConfig.role) !== -1);
    if (pgConfig.role === 'primary') {
        assert.ok(!pgConfig.upstream, 'pgConfig.upstream is not null');
        if (!self._oneNodeWriteMode) {
            assert.object(pgConfig.downstream, 'pgConfig.downstream');
            assertPeerIdentifier(pgConfig.downstream);
        } else {
            assert.ok(!pgConfig.downstream, 'pgConfig.downstream is not null');
        }
    }
    if (pgConfig.role === 'standby') {
        assert.object(pgConfig.upstream, 'pgConfig.upstream');
        assertPeerIdentifier(pgConfig.upstream);
        assert.ok(!pgConfig.downstream, 'pgConfig.downstream is not null');
    }
    if (pgConfig.role === 'none') {
        assert.ok(!pgConfig.upstream, 'pgConfig.upstream is not null');
        assert.ok(!pgConfig.downstream, 'pgConfig.downstream is not null');
    }
    assert.func(callback, 'callback');

    callback = once(callback);

    if (self._transitioning) {
        return (callback(new verror.VError('already transitioning')));
    }

    if (!self._running) {
        self._pgConfig = pgConfig;
        self._appliedPgConfig = false;
        return (setImmediate(callback));
    }

    log.info('PostgresMgr.reconfigure: entering');
    self._transitioning = true;
    self._reconfigure(pgConfig, function (err) {
        log.info({err: err}, 'PostgresMgr.reconfigure: exiting');
        self._transitioning = false;
        return (callback(err));
    });
};


/**
 * Gets the Postgres transaction log location.  Postgres must be running,
 * otherwise an error will be returned.
 *
 * @param {PostgresMgr-cb} callback
 * @return {string} A string indicating the current postgres transaction log
 * location.  For example: 0/17B7188
 */
PostgresMgr.prototype.getXLogLocation = function getXLogLocation(callback) {
    var self = this;

    if (!self._running) {
        return (callback(new verror.VError('postgres is not running')));
    }

    function onResponse(err, result) {
        if (err) {
            return (callback(err));
        }
        return (callback(null, result.loc));
    }

    if (self._pgConfig.role === 'primary') {
        self._queryDb('SELECT pg_current_xlog_location() as loc;',
                      onResponse);
    } else {
        self._queryDb('SELECT pg_last_xlog_replay_location() as loc;',
                      onResponse);
    }
};


/**
 * Gets the last know health status of postgres.  Returns a status object with
 * a few fields, described below:
 *
 * @return {boolean} result.healthy True if the last heath check was good.
 * @return {error} result.error The error if healthy === false.
 * @return {integer} result.lastCheck The timestamp the last time the heath
 * check was run.
 */
PostgresMgr.prototype.status = function status() {
    var self = this;
    return({
        'healthy': self._healthy,
        'error': self._lastHealthChkErr,
        'lastCheck': self._lastHealthChkTime
    });
};


/**
 * Closes down this PostgresMgr, stopping Postgres and the health check.  Once
 * called, you must create a new PostgresMgr rather than trying to reuse the
 * current one.
 *
 * @param {PostgresMgr-cb} callback
 */
PostgresMgr.prototype.close = function close(callback) {
    var self = this;
    var log = self._log;

    log.info('PostgresMgr.close: entering');
    vasync.pipeline({
        'funcs': [
            function _shutdownPostgres(_, _cb) {
                if (!self._running) {
                    return (_cb());
                }
                self.stop(_cb);
            },
            function _stopHealthCheck(_, _cb) {
                self._stopHealthCheck(_cb);
            }
        ]
    }, function (err) {
        log.info({err: err}, 'PostgresMgr.close: exiting');
        return (callback(err));
    });
};


/**
 * Reconfigures and either starts or restarts postgres.
 *
 * See the docs for .reconfigure for an explaination of pgConfig.
 */
PostgresMgr.prototype._reconfigure = function _reconfigure(pgConfig, callback) {
    if (typeof (pgConfig) === 'function') {
        callback = pgConfig;
        pgConfig = null;
    }

    var self = this;
    var emitter;

    function onReconfigure(err) {
        if (!err) {
            self._pgConfig = pgConfig;
            self._appliedPgConfig = true;
        }
        return (callback(err));
    }

    //Reconfigure to become nothing.
    if (pgConfig && pgConfig.role === 'none') {
        return (setImmediate(onReconfigure));
    }

    function comparePeers(a, b) {
        if (!a && !b) {
            return (true);
        }
        return (a.pgUrl === b.pgUrl &&
                a.backupUrl === b.backupUrl);
    }

    //If we already applied the same postgres config, there's nothing to do
    // but start postgres and return.
    if (pgConfig && self._pgconfig &&
        pgConfig.role === self._pgConfig.role &&
        comparePeers(pgConfig.upstream, self._pgConfig.upstream) &&
        comparePeers(pgConfig.downstream, self._pgConfig.downstream) &&
        self._appliedPgConfig) {
        if (self._postgres) {
            return (setImmediate(callback));
        } else {
            return (self._start(callback));
        }
    }

    //If we're already running and this is only a standby change, delegate
    // out to ._updateStandby
    if (self._postgres && self._pgConfig && pgConfig &&
        self._pgConfig.role === 'primary' &&
        pgConfig.role === 'primary') {
        emitter = self._updateStandby(pgConfig.downstream.pgUrl);
        emitter.on('error', onReconfigure);
        emitter.on('done', onReconfigure);
    }

    //For anything else after this point, we just do the whole thing since it is
    // either a start, promotion, or reconfiguration.
    if (!pgConfig) {
        pgConfig = self._pgConfig;
    }

    //Otherwise, we require a full reconfiguration.
    //Pick up a new standby if we're already running and this is
    if (pgConfig.role === 'primary') {
        emitter = self._primary(pgConfig.downstream.pgUrl);
        emitter.on('error', onReconfigure);
        emitter.on('done', onReconfigure);
    } else {
        self._standby(pgConfig.upstream.pgUrl,
                      pgConfig.upstream.backupUrl,
                      onReconfigure);
    }
};


/**
 * Transition the PostgreSQL instance to primary mode.
 *
 * @param {String} stdby The standby, like:
 *                       tcp://postgres@10.77.77.10:5432/postgres
 * @param {PostgresMgr-cb} callback
 */
PostgresMgr.prototype._primary = function _primary(stdby, callback) {
    var self = this;
    var log = self._log;

    log.info({
        url: self._url,
        standby: stdby
    }, 'PostgresMgr._primary: entering.');

    var replErrMsg = 'could not verify standby replication status, ' +
        'possible WAL corruption remaining in readonly mode';

    var checkReplEmitter;
    // we don't check for replication status if a standby doesn't exist.
    var finishedCheckRepl = stdby ? false : true;
    var primaryEmitter = new EventEmitter();
    primaryEmitter.cancel = function cancel() {
        log.info({
            finishedCheckRepl: finishedCheckRepl
        }, 'PostgresMgr._primary: cancelling operation');
        // We only try and cancel the replication check. But only after
        if (!finishedCheckRepl) {
            if (checkReplEmitter) {
                log.info('PostgresMgr._primary: replication check ' +
                         'started, cancelling check');
                checkReplEmitter.cancel();
            } else {
                log.info('PostgresMgr._primary: replication check not ' +
                         'started, trying again in 1s.');
                setTimeout(cancel, 1000);
            }
        }
    };

    vasync.pipeline({funcs: [
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
         *
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
        }
    ], arg: {}}, function (err) {
        if (err) {
            log.error({
                err: err,
                standby: stdby,
                url: self._url
            }, 'PostgresMgr._primary: error');
            primaryEmitter.emit('error', err);
        } else {
            log.info({
                standby: stdby,
                url: self._url
            }, 'PostgresMgr._primary: complete');
            primaryEmitter.emit('done');
        }
    });

    return primaryEmitter;
};


/**
 * Updates the standby of the current node. This assumes the current node is
 * already a primary. This does only sends SIGHUP to postgres, not SIGINT.
 *
 * @param {String} stdby The standby, like:
 *                       tcp://postgres@10.77.77.8:5432/postgres)
 * @param {PostgresMgr-cb} callback
 */
PostgresMgr.prototype._updateStandby = function _updateStandby(stdby) {
    var self = this;
    var log = self._log;

    log.info({
        url: self._url,
        standby: stdby
    }, 'PostgresMgr._updateStandby: entering.');

    var replErrMsg = 'could not verify standby replication status, ' +
        'possible WAL corruption remaining in readonly mode';

    var checkReplEmitter;
    // we don't check for replication status if a standby doesn't exist.
    var finishedCheckRepl = stdby ? false : true;
    var updateStandbyEmitter = new EventEmitter();
    updateStandbyEmitter.cancel = function cancel() {
        log.info({
            finishedCheckRepl: finishedCheckRepl
        }, 'PostgresMgr._updateStandby: cancelling operation');
        // We only try and cancel the replication check.
        if (!finishedCheckRepl) {
            if (checkReplEmitter) {
                log.info('PostgresMgr._updateStandby: replication check ' +
                         'started, cancelling check');
                checkReplEmitter.cancel();
            } else {
                log.info('PostgresMgr._updateStandby: replication check not ' +
                         'started, trying again in 1s.');
                setTimeout(cancel, 1000);
            }
        }
    };

    vasync.pipeline({funcs: [
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
        }
    ], arg: {}}, function (err) {
        if (err) {
            log.info({
                standby: stdby,
                url: self._url
            }, 'PostgresMgr._updateStandby: error');
            updateStandbyEmitter.emit('error', err);
        } else {
            log.info({
                standby: stdby,
                url: self._url
            }, 'PostgresMgr._updateStandby: complete');
            updateStandbyEmitter.emit('done');
        }
    });

    return updateStandbyEmitter;
};


/**
 * Transitions a postgres instance to standby state.
 *
 * @param {string} primUrl The postgres url of the primary, like:
 *                         tcp://postgres@10.77.77.10:5432/postgres
 * @param {string} backupUrl The http url of the primary's backup service, like:
 *                           http://10.77.77.10:12345
 * @param {PostgresMgr-cb} callback
 */
PostgresMgr.prototype._standby = function _standby(primUrl,
                                                   backupUrl,
                                                   callback) {
    var self = this;
    var log = self._log;
    var primaryUrl = url.parse(primUrl);
    var backupSnapshot;

    log.info({
        primaryUrl: primaryUrl.href,
        backupUrl: backupUrl
    }, 'PostgresMgr._standby: entering');

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
        }, 'PostgresMgr._standby: restoring db from primary');

        self._zfsClient.restore(backupUrl, function (err2, snapshot) {
            backupSnapshot = snapshot;
            if (err2) {
                log.info({
                    err: err2,
                    backupUrl: backupUrl
                }, 'PostgresMgr._standby: could not restore from primary');
                return cb(err2);
            }
            var cmd = 'chown -R ' + self._dbUser + ' ' + self._dataDir;
            log.info({
                cmd: cmd
            }, 'PostgresMgr._standby: finished backup, chowning datadir');
            exec(cmd, cb);
        });
    }

    /**
     * If steps 1-4 error out, then a restore of the database is taken from the
     * primary. This is controlled by the _.isRestore flag attached to the
     * vasync args.
     */
    vasync.pipeline({funcs: [
        // have to stop postgres here first such that we can assert the dataset,
        // other wise some actions will fail with EBUSY
        function _stopPostgres(_, cb) {
            log.info('PostgresMgr.initDb: stop postgres');
            self._stop(cb);
        },
        // fix for MANATEE-90, always check that the dataset exists before
        // starting postgres
        function _assertDataset(_, cb) {
            self._zfsClient.assertDataset(cb);
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
                        self._zfsClient.restoreDataset(backupSnapshot,
                                                       function () {
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
                        self._zfsClient.restoreDataset(backupSnapshot,
                                                       function () {
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
                log.info({cmd: cmd}, 'PostgresMgr._standby: exec');
                exec(cmd, cb);
            }
        }
    ], arg: {}}, function (err) {
        if (err) {
            log.info({
                err:err,
                primaryUrl: primaryUrl.href,
                backupUrl: backupUrl
            }, 'PostgresMgr._standby: error');
            return callback(err);
        } else {
            log.info({
                primaryUrl: primaryUrl.href,
                backupUrl: backupUrl
            }, 'PostgresMgr._standby: complete');
            return callback();
        }
    });
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
    log.info('PostgresMgr.starthealthCheck: entering');

    if (self._healthChkIntervalId) {
        log.info('PostgresMgr.starthealthCheck: health check already running');
        return callback();
    } else {
        self._healthy = null;
        self._lastHealthChkErr = null;
        self._lastHealthChkTime = Date.now();
        self._healthChkIntervalId = setInterval(function () {
            // set a timeout in case _health() doesn't return in time
            var timeoutId = setTimeout(function () {
                /**
                 * Unhealthy event, emitted when there's an unrecoverable error
                 * with the PostgreSQL instance. Usually this is because of:
                 * - The healthcheck has failed.
                 * - PostgreSQL exited on its own.
                 * - The manager was unable to start PostgreSQL.
                 *
                 * @event PostgresMgr#error
                 * @type {verror.VError} error
                 */
                self._healthy = false;
                self._lastHealthChkErr = new verror.VError(
                    'PostgresMgr._startHealthCheck() timed out');
                self.emit('unhealthy', self._lastHealthChkErr);
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
                self._healthy = false;
                self._lastHealthChkErr =  new verror.VError(err, msg);
                self.emit('unhealthy', self._lastHealthChkErr);
            }
        } else {
            self._lastHealthChkTime = Date.now();
            self._healthy = true;
            self._lastHealthChkErr = null;
            self.emit('healthy');
        }
    }
};


/**
 * Stop the postgres health check.
 * @param {function} callback The callback of the form f(err).
 */
PostgresMgr.prototype._stopHealthCheck = function (callback) {
    var self = this;
    var log = self._log;
    log.info('PostgresMgr.stopHealthCheck: entering');

    if (self._healthChkIntervalId) {
        clearInterval(self._healthChkIntervalId);
        self._healthChkIntervalId = null;
    } else {
        log.info('PostgresMgr.stopHealthCheck: not running');
    }

    return (setImmediate(callback));
};


/**
 * Start the postgres instance.
 * @param {function} callback The callback of the form f(err, process).
 */
PostgresMgr.prototype._start = function _start(cb) {
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
        }, 'PostgresMgr.start: postgres -D exited with err');

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
            //TODO: Do we want to crash at this point?
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
                }, 'PostgresMgr.start: db has not started');

                if (timeSinceStart > self._opsTimeout) {
                    log.info({
                        timeSinceStart: timeSinceStart,
                        opsTimeout: self._opsTimeout,
                        postgresPath: self._postgresPath,
                        dataDir: self._dataDir
                    }, 'PostgresMgr.start: start timeout');

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
                }, 'PostgresMgr.start: db has started');
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
            self._zfsClient.assertDataset(cb);
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

                //TODO: Is this necessary?  Shouldn't we copy this over
                // in other places and not here?  I'd rather this file doesn't
                // exist than it exist and is wrong.
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
    }, 'PostgresMgr.query: entering.');

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
    log.info('PostgresMgr.sighup: entering');

    var postgres = self._postgres;
    postgres.kill('SIGHUP');
    callback();
};


/**
 * Update keys in postgresql.conf. Starts from the default conf that ships with
 * Manatee, meaning that keys in the current config not present in the default
 * config will be lost.
 */
PostgresMgr.prototype._updatePgConf = function (options, cb) {
    var self = this;
    self._updateConf(options, self._postgresConf, self._postgresConfPath, cb);
};


/**
 * Update keys in recovery.conf. Starts from the default conf that ships with
 * Manatee, meaning that keys in the current config not present in the default
 * config will be lost.
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
    log.info('PostgresMgr.restart: entering');

    // check health first to see if db is running
    self._health(function (err) {
        if (err) {
            log.info('PostgresMgr.restart: db not running');
            return self._start(callback);
        } else {
            log.info('PostgresMgr.restart: db is running');
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
    log.trace('PostgresMgr.health: entering');
    self._queryDb('select current_time;', function (err) {
        if (err) {
            log.trace({err: err}, 'PostgresMgr.health: failed');
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
                 * leaves the election.
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
             'PostgresMgr.checkReplStatus: entering');
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
