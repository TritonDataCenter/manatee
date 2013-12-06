/**
 * @overview The PostgreSQL sync state checker. Checks whether the current
 * replication state is async or synchronous.
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
var pg = require('pg');
var Client = pg.Client;
var fs = require('fs');
var sprintf = require('util').format;
var vasync = require('vasync');
var verror = require('verror');

/**
 * Checks and persists the current replication state. We do this by querying the
 * leader about our replication state -- since Postgresql does not present a
 * method of finding a slave's current replication status on the slave itself.
 *
 * @constructor
 *
 * @param {object} options Options object.
 * @param {Bunyan} options.log Bunyan logger.
 * @param {string} options.cookieLocation File on disk where the state is stored
 * @param {number} options.interval Period in ms to check the sync state.
 */
function SyncStateChecker(options) {
    assert.object(options, 'options');
    assert.number(options.interval, 'options.interval');
    assert.string(options.cookieLocation, 'options.cookieLocation');
    assert.object(options.log, 'options.log');

    /** @type {Bunyan} The bunyan log object */
    this._log = options.log.child({component: 'SyncStateChecker'}, true);
    /** @type {number} sync state check interval in ms */
    this._interval = options.interval;
    /** @type {string} location of the stored sync state cookie */
    this._cookieLocation = options.cookieLocation;
    /** @type {pg.Client} postgres client */
    this._pgClient = null;
    /** @type {string} URL of the this instance's primary */
    this._primaryUrl = null;
    /** @type {object} intervalId of the sync check */
    this._intervalId = null;
}

module.exports = SyncStateChecker;

/**
 * @const
 * @type {string} PostgreSQL sync state query string.
 * @default
 */
SyncStateChecker.prototype.SYNC_STATE_PG_QUERY =
    'select sync_state from pg_stat_replication where client_addr = \'%s\'';

/**
 * @constant
 * @type {string} sync state checker async flag
 * @default
 */
SyncStateChecker.prototype.ASYNC = 'async';

/**
 * @callback SyncStateChecker-cb
 * @param {Error} error
 */

/**
 * Start the sync checker.
 *
 * @param {string} primaryUrl looks like:
 * tcp://postgres@10.77.77.34:5432/postgres
 * @param {Url} myUrl looks like: tcp://postgres@10.77.77.34:5432/postgres
 * @param {SyncStateChecker-cb} cb
 */
SyncStateChecker.prototype.start = function start(primaryUrl, myUrl, cb) {
    var self = this;
    var log = self._log;
    log.info({
        primaryUrl: primaryUrl,
        currPrimaryUrl: self._primaryUrl,
        myUrl: myUrl
    }, 'SyncStateChecker.start: entering');

    assert.string(primaryUrl, 'primaryUrl');
    assert.object(myUrl, 'myUrl');
    assert.func(cb, 'cb');

    self._primaryUrl = primaryUrl;
    var queryString = sprintf(self.SYNC_STATE_PG_QUERY, myUrl.hostname);

    function createPgClient(pgUrl, _cb) {
        self._pgClient = new pg.Client(pgUrl);

        /*
         * do nothing on errors since this is querying state from a remote pg,
         * many things can cause this to fail, and we don't want the client
         * error to barf and emit error all the way up the stack, causing the
         * manatee process to fail.
         */
        self._pgClient.on('error', function () {});

        self._pgClient.connect(function (err) {
            // try again next time
            if (err) {
                self._pgClient.end();
                self._pgClient = null;
                err = new verror.VError(err);
            }
            return _cb(err);
        });

    }

    function querySyncState() {
        if (!self._pgClient) {
            log.info({primaryUrl: self._primaryUrl},
                'SyncStateChecker.start.querySyncState: ' +
                'pgClient not created yet, creating pgClient');

            createPgClient(self._primaryUrl, function (err) {
                log.debug({err: err},
                    'SyncStateChecker.start.querySyncState: ' +
                    'returned from createPgClient');
                return;
            });
        }
        self._pgClient.query(queryString, function (err, result) {
            if (err) {
                log.warn({err: err}, 'unable to query replication status');
            } else {
                log.debug({
                    file: self._cookieLocation,
                    state: result
                }, 'writing sync_state cookie');
                if (result.rows[0] && result.rows[0]['sync_state']) {
                    var state = {
                        role: result.rows[0]['sync_state'],
                        leader: self._primaryUrl
                    };
                    fs.writeFile(self._cookieLocation, JSON.stringify(state),
                        'utf8', function (_err)
                    {
                        if (_err) {
                            log.warn({
                                err: _err,
                                file: self._cookieLocation,
                                state: result
                            }, 'unable to write replication status');
                        }
                    });
                } else {
                    log.warn({
                        state: result
                    }, 'replication result does not have valid sync state');
                }
            }
        });
    }

    function checkWriteSyncState() {
        log.debug({url: self._primaryUrl, queryString: queryString},
                  'querying sync state from pg');

        querySyncState();
    }

    vasync.pipeline({
        funcs: [
            function stopInterval(_, _cb) {
                if (self._intervalId) {
                    clearInterval(self._intervalId);
                    self._intervalId = null;
                }
                return _cb();
            },
            function dcOldPgClient(_, _cb) {
                if (self._pgClient) {
                    try {
                        self._pgClient.end();
                    } catch (e) {}
                    self._pgClient = null;
                }
                return _cb();
            },
            function createNewPgClient(_, _cb) {
                /*
                 * ignore any errors whilst creating pg client, since we always
                 * try to create a new one if this client doesn't exist.
                 */
                createPgClient(self._primaryUrl, function () {
                    return _cb();
                });
            },
            function startInterval(_, _cb) {
                self._intervalId =
                    setInterval(checkWriteSyncState, self._interval);
                // manually invoke the function the first time.
                checkWriteSyncState();
                return _cb();
            }
        ]
    }, function (err, results) {
        return cb(err);
    });
};

/**
 * Stop checking the sync state of my primary. We only want to delete the
 * cookie if we are becoming the primary, otherwise we would like to preserve
 * the cookie information.
 *
 * @param {bool} delCookie Whether to delete the cookie.
 * @param {SyncStateChecker-cb} cb
 */
SyncStateChecker.prototype.stop = function (delCookie, cb) {
    assert.func(cb, 'cb');

    var self = this;
    self._log.info({
        currPrimaryUrl: self._primaryUrl,
        delCookie: delCookie
    }, 'SyncStateChecker.stop: entering');

    assert.func(cb, 'cb');

    vasync.pipeline({
        funcs: [
            function stopInterval(_, _cb) {
                if (self._intervalId) {
                    clearInterval(self._intervalId);
                    self._intervalId = null;
                }
                return _cb();
            },
            function dcOldPgClient(_, _cb) {
                if (self._pgClient) {
                    try {
                        self._pgClient.end();
                    } catch (e) {}
                    self._pgClient = null;
                }
                return _cb();
            },
            function deleteCookie(_, _cb) {
                if (delCookie) {
                    fs.unlink(self._cookieLocation, function (err) {
                        self._log.warn({err: err},
                            'SyncStateChecker.stop: unable to del cookie');
                        return _cb();
                    });
                } else {
                    return _cb();
                }
            }
        ]
    }, function (err, results) {
        self._log.info({err: err}, 'SyncStateChecker.stop: exiting');
        return cb(err);
    });
};

/**
 * @callback SyncStateChecker-lastRoleCb
 * @param {Error} error
 * @param {string} state
 */

/**
 * Get the last role of this instance.
 *
 * @param {SyncStateChecker-lastRoleCb} cb
 */
SyncStateChecker.prototype.getLastRole = function (cb) {
    var self = this;
    self._log.info({
        cookieLocaltion: self._cookieLocation
    }, 'SyncStateChecker.getLastRole: entering');

    assert.func(cb, 'cb');
    fs.readFile(self._cookieLocation, 'utf8', function (err, data) {
        if (err) {
            // do nothing if we can't get the cookie
            self._log.warn({err: err}, 'unable to get last role');
            return cb();
        }
        var state;
        if (data) {
            state = JSON.parse(data);
        }

        self._log.info({
            role: state
        }, 'SyncStateChecker.getLastRole: exiting');
        return cb(null, state);
    });
};

/**
 * Write the primary cookie.
 *
 * @param {SyncStateChecker-cb} cb
 */
SyncStateChecker.prototype.writePrimaryCookie = function writePrimaryCookie(cb)
{
    var self = this;
    self._log.info({myUrl: self._url},
        'SyncStateChecker.writePrimaryCookie: entering');

    var state = {
        role: 'primary'
    };

    fs.writeFile(self._cookieLocation, JSON.stringify(state), function (err) {
        if (err) {
            err = new verror.VError(err);
        }

        self._log.info({err: err, cookie: state},
            'SyncStateChecker.writePrimaryCookie: exiting');
        return cb(err);
    });
};

/**
 * @callback SyncStateChecker-masterCb
 * @param {boolean} canStart
 */

/**
 * Check whether this instance can start as a master.
 *
 * @param {SyncStateChecker-masterCb} cb
 */
SyncStateChecker.prototype.canStartAsMaster = function (cb) {
    var self = this;
    self._log.info({
        cookieLocaltion: self._cookieLocation
    }, 'SyncStateChecker.canStartAsMaster: entering');

    assert.func(cb, 'cb');

    fs.readFile(self._cookieLocation, 'utf8', function (err, data) {
        var can = true;
        var state;
        if (data) {
            state = JSON.parse(data);
        }
        if (state && state.role === self.ASYNC) {
            can = false;
        }

        self._log.info({
            cookieLocaltion: self._cookieLocation,
            data: data,
            canStart: can
        }, 'SyncStateChecker.canStartAsMaster: exiting');
        return cb(can);
    });
};
