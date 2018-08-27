/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2018, Joyent, Inc.
 */

/**
 * @overview Daemon that takes periodic zfs snapshots of the postgres data dir.
 *
 *                   _.---.._
 *      _        _.-' \  \    ''-.
 *    .'  '-,_.-'   /  /  /       '''.
 *   (       _                     o  :
 *    '._ .-'  '-._         \  \-  ---]
 *                  '-.___.-')  )..-'
 *                           (_/
 *
 */
var assert = require('assert-plus');
var EventEmitter = require('events').EventEmitter;
var forkexec = require('forkexec');
var once = require('once');
var restify = require('restify');
var util = require('util');
var vasync = require('vasync');
var verror = require('verror');

var lib_common = require('../lib/common');

var VE = verror.VError;


/*
 * For determining if a ZFS snapshot name is at least somewhat well-formed.
 * Can also be used to break a snapshot, e.g. "data/set@snapname" into
 * "data/set" and "snapname" components.
 */
var RE_SNAPSHOT = new RegExp('^([^@]+)@([^@]+)$');

/**
 * Takes periodic zfs snapshots of a pg data dir.
 *
 * As of this writing, there are other places that snapshots are taken
 * independently:
 *    1. Snapshots for backups that are uploaded offsite.
 *    2. When sync is promoted (see postgresMgr)
 *
 * @constructor
 * @augments EventEmitter
 *
 * @fires SnapShotter#error
 *
 * @param {object} options Options object.
 * @param {Bunyan} options.log Bunyan logger.
 * @param {string} options.dataset The ZFS dataset to snapshot.
 * @param {number} options.pollInterval How often to take snapshots.
 * dataset. i.e.  the .zfs dir.
 * @param {number} options.snapshotNumber Number of snapshots to keep.
 * @param {string} options.healthUrl The url to check manatee health.
 *
 * @throws {Error} If the options object is malformed.
 */
function SnapShotter(options) {
    assert.object(options, 'options');
    assert.object(options.log, 'options.log');

    assert.string(options.dataset, 'options.dataset');
    assert.optionalNumber(options.pollInterval, 'options.pollInterval');
    assert.optionalNumber(options.snapshotNumber, 'options.snapshotNumber');
    assert.optionalString(options.healthUrl, 'options.healthUrl');

    EventEmitter.call(this);

    var self = this;

    this._log = options.log;

    /*
     * This number is used to correlate the "begin" and "end" messages for
     * particular invocations of zfs(1M) in trace log messages.
     */
    this._zfsRuns = 0;

    /** @type {number} The snapshot period in ms */
    this._pollInterval = options.pollInterval || 1 * 1000;

    /** @type {string} The ZFS dataset to snapshot */
    this._dataset = options.dataset;

    /** @type {number} The number of snapshots to retain */
    this._snapshotNumber = options.snapshotNumber || 10;

    /** @type {string} The health endpoint for this manatee */
    this._healthUrl = options.healthUrl;

    self._log.info('initialized snapshotter with options', options);
}

module.exports = SnapShotter;
util.inherits(SnapShotter, EventEmitter);

/**
 * @callback SnapsShotter-Cb
 * @param {Error} error
 */

/**
 * Start the snapshotter.
 *
 * @param {Snapshotter-Cb} callback
 */
SnapShotter.prototype.start = function start(callback) {
    var self = this;
    var log = self._log;

    log.info('starting snapshotter daemon');

    function create() {
        var skipSnapshot = false;
        vasync.pipeline({funcs: [
            function checkHealth(_, cb) {
                if (!self._healthUrl) {
                    return (cb());
                }
                var client = restify.createJsonClient({
                    url: self._healthUrl,
                    version: '*'
                });
                client.get('/ping', function (err, req, res, obj) {
                    log.info({
                        err: err,
                        obj: obj
                    }, 'got manatee sitter status');
                    if (err || (obj && !obj.healthy)) {
                        log.warn({err: err}, 'error fetching manatee status, ' +
                                 'not taking snapshot.');
                        skipSnapshot = true;
                    }
                    return (cb());
                });
            },
            function takeSnapshot(_, cb) {
                if (skipSnapshot) {
                    return (cb());
                }
                self.createSnapshot(String(Date.now()), cb);
            }
        ], 'arg': {}}, function (err) {
            if (err) {
                log.error({err: err}, 'unable to create snapshot');
                /**
                 * SnapShotter error event.
                 *
                 * @event SnapShotter#error
                 * @type {Error}
                 */
                self.emit('error', err);
            }
        });
    }

    /*
     * Call create() before setting the interval so that the function fires
     * immediately rather than waiting before its first execution.
     */
    create();
    setInterval(create, self._pollInterval);


    /*
     * Call cleanup() immediately, it will be scheduled to repeat later.
     */
    (function cleanup() {
        log.info('cleaning up snapshots');
        vasync.pipeline({funcs: [
            function _checkForDataset(_, cb) {
                assert.string(self._dataset, 'self._dataset');

                lib_common.zfsExists({ log: log, dataset: self._dataset },
                  function (err, exists) {
                    if (err) {
                        cb(err);
                        return;
                    }

                    assert.bool(exists, 'exists');
                    if (exists) {
                        cb();
                        return;
                    }

                    /*
                     * If the dataset does not yet exist we have nothing to do.
                     * Fail early with the "no_dataset" sentinel to prevent
                     * firing an error event.
                     */
                    log.info('dataset "%s" does not yet exist; no cleanup',
                      self._dataset);
                    cb(new VE({ info: { no_dataset: true }}, 'no dataset'));
                });
            },
            function _getSnapshots(_, cb) {
                assert.string(self._dataset, 'self._dataset');
                assert.ok(!RE_SNAPSHOT.test(self._dataset), self._dataset +
                    ' should not be a snapshot');

                /*
                 * List snapshots sorted by creation time in ascending order.
                 * This makes the oldest snapshot appear first in the list.
                 */
                self._execZfs({
                    label: 'list snapshots for cleanup',
                    args: [
                        'list',
                        '-t', 'snapshot',
                        '-H',
                        '-d', '1',
                        '-s', 'creation',
                        '-o', 'name',
                        self._dataset
                    ]
                }, function (err, info) {
                    if (err) {
                        log.error({
                            err: err,
                            duration_ms: info.duration_ms
                        }, 'failure to list snapshots for cleanup');
                        cb(err);
                        return;
                    }

                    /*
                     * Snapshots created by Manatee are named for their
                     * creation time, expressed as the number of milliseconds
                     * since the UNIX epoch.  Filter out any snapshots that do
                     * not match this pattern, as they were likely created by
                     * the operator or another tool.
                     */
                    var ignored = 0;
                    _.snapshots = info.stdout.split('\n').filter(function (l) {
                        var t = RE_SNAPSHOT.exec(l);

                        if (!t) {
                            return (false);
                        }

                        if (!t[2].match(/^\d{13}$/)) {
                            /*
                             * This line describes a well-formed snapshot,
                             * but the snapshot name does not match our
                             * expected format.
                             */
                            ignored++;
                            return (false);
                        }

                        return (true);
                    });

                    log.info({
                        duration_ms: info.duration_ms,
                        snapshots_count: _.snapshots.length,
                        snapshots_ignored: ignored
                    }, 'cleanup: found %d snapshots, ignored %d snapshots',
                        _.snapshots.length, ignored);

                    cb();
                });
            },
            function _deleteSnapshots(_, cb) {
                cb = once(cb);
                var snapshots = _.snapshots;

                if (snapshots.length < self._snapshotNumber) {
                    setImmediate(cb);
                    return;
                }

                log.info({
                    numberOfSnapshots: snapshots.length,
                    threshHold: self._snapshotNumber
                }, 'deleting snapshots as number exceeds threshold');

                var excessSnapshots =
                    snapshots.length - self._snapshotNumber;
                if (excessSnapshots <= 0) {
                    setImmediate(cb);
                    return;
                }

                var deletedCount = 0;
                /*
                 * stuckSnapshots is an array of objects containing a
                 * snapshot and the error messages that occured when that
                 * snapshot could not be deleted in the following loop.
                 */
                var stuckSnapshots = [];

                vasync.forEachPipeline({
                    inputs: snapshots,
                    func: function (s, next) {

                        /*
                         * If enough snapshots have been deleted to bring the
                         * total number under the configured threshold, move on.
                         */
                        if (deletedCount >= excessSnapshots) {
                            setImmediate(next);
                            return;
                        }

                        self._deleteSnapshot(s, function (err, snapshot) {
                            if (err) {
                                /*
                                 * Though the callback has passed an error, we
                                 * do not want to fire an amon alert on a single
                                 * delete failure, since the reason for it may
                                 * resolve with a retry.  Collect the failed
                                 * deletions with information about which
                                 * snapshot could not be deleted and why in case
                                 * intervention is required.
                                 */
                                var deleteFailure = {
                                    err: err,
                                    snapshot: snapshot
                                };
                                stuckSnapshots.push(deleteFailure);

                                /*
                                 * If the number of stuck snapshots has reached
                                 * or exceeded the total number of snapshots, we
                                 * have tried them all.  If the situation does
                                 * not resolve itself on subsequent cleanup
                                 * attempts, the snapshotter service will be
                                 * essentially unusable.
                                 */
                                if (stuckSnapshots.length >= snapshots.length) {
                                    /*
                                     * We justify the use of a fatal error here,
                                     * which is needed to fire an amon alert,
                                     * because if this condition is hit the
                                     * service may become "unusable" as per the
                                     * bunyan log level documentation.
                                     */
                                    log.fatal({
                                        numberOfStuckSnapshots:
                                            stuckSnapshots.length,
                                        numberofTotalSnapshots:
                                            snapshots.length,
                                        threshold: self._snapshotNumber
                                    }, 'unable to delete any snapshots, if we' +
                                    ' are over the threshold of snapshots to' +
                                    ' keep we can\'t create new ones!');

                                    next(err);
                                    return;
                                }

                                /*
                                 * Quit the delete operation and raise an error
                                 * once we have tried to delete each snapshot in
                                 * the array except the most recent, which is
                                 * currently the last snapshot in the array.
                                 */
                                if (stuckSnapshots.length >= excessSnapshots) {
                                    log.error({
                                        numberOfStuckSnapshots:
                                            stuckSnapshots.length,
                                        stuckSnapshots: stuckSnapshots
                                    }, 'unable to delete snapshots, exceeding' +
                                        ' the threshold of snapshots to keep!');

                                    next(err);
                                    return;
                                }

                                /*
                                 * Continue to iterate through snapshots.
                                 */
                                next();
                                return;
                            }

                            /*
                             * Successful removal of a snapshot.
                             */
                            deletedCount++;
                            next();

                        });
                    }
                }, function snapshotterPipelineDone(err) {
                    if (err) {
                        log.info('could not complete snapshot deletion');
                        cb(err);
                        return;
                    }
                    log.info('completed snapshot deletion');
                    cb();
                });
            }
        ], arg: {}}, function onSnapshotterCleanupEnd(err, results) {
            /*
             * The cleanup operation will retry regardless of prior failure if
             * too few snapshots were deleted to bring the total number of
             * snapshots for this dataset under the configured threshold. This
             * is done in case these errors were transient. Even if they were
             * not, it is better to have more alarms fire than to allow the
             * system to fill up with old snapshots when retrying continuously
             * could have, in many cases, easily prevented that.  Though its
             * true this may produce more logging, which also takes up space,
             * error messages are likely much smaller than a snapshot of a
             * production dataset.
             */
            setTimeout(cleanup, self._pollInterval);
            if (err && VE.info(err).no_dataset !== true) {
                log.error({err: err, results: results},
                          'unable to maintain snapshots');
                /**
                 * SnapShotter error event.
                 *
                 * @event SnapShotter#error
                 * @type {Error}
                 */
                self.emit('error', err);
            }
            log.info('done with snapshot cleanup');
        });
    })();

    log.info('started snapshotter daemon');
    return callback();
};

/**
 * Creates a zfs snapshot of the current postgres data directory.
 *
 * @param {string} name The name of the snapshot.
 * @param {Snapshotter-Cb} callback
 */
SnapShotter.prototype.createSnapshot = function createSnapshot(name, callback) {
    var self = this;

    assert.string(name, 'name');
    assert.string(self._dataset, 'self._dataset');
    assert.func(callback, 'callback');

    var snapshot = self._dataset + '@' + name;
    var log = self._log.child({
        snapshot: snapshot
    });

    log.info('creating ZFS snapshot');

    self._writeSnapshot(snapshot, function (err) {
        if (err) {
            log.warn(err, 'error while creating ZFS snapshot');
        } else {
            log.info('ZFS snapshot created');
        }

        /*
         * These errors are only logged, not passed up to the caller, since the
         * caller's responsibility is to retry periodically regardless of
         * failure.
         */
        callback();
    });
};

/**
 * #@+
 * @private
 * @memberOf SnapShotter
 */

/**
 * Write a zfs snapshot to disk.
 * @param String snapshot The name of the snapshot.
 */
SnapShotter.prototype._writeSnapshot = function (snapshot, callback) {
    var self = this;

    assert.string(snapshot, 'snapshot');
    assert.ok(RE_SNAPSHOT.test(snapshot), 'invalid snapshot: ' + snapshot);
    assert.func(callback, 'callback');

    var log = self._log.child({
        snapshot: snapshot
    });

    log.info('SnapShotter.writeSnapshot: entering');

    self._execZfs({
        label: 'write snapshot',
        args: [
            'snapshot',
            snapshot
        ]
    }, function (err, info) {
        log.info({
            err: err,
            duration_ms: info.duration_ms
        }, 'SnapShotter.writeSnapshot: exiting');

        callback(err);
    });
};

/**
 * Delete a zfs snapshot from disk
 * @param String snapshot The name of the snapshot.
 */
SnapShotter.prototype._deleteSnapshot = function (snapshot, callback) {
    var self = this;

    assert.string(snapshot, 'snapshot');
    assert.ok(RE_SNAPSHOT.test(snapshot), 'invalid snapshot: ' + snapshot);
    assert.func(callback, 'callback');

    var log = self._log.child({
        snapshot: snapshot
    });

    log.info('SnapShotter._deleteSnapshot: entering');

    self._execZfs({
        label: 'delete snapshot',
        args: [
            'destroy',
            snapshot
        ]
    }, function (err, info) {
        log.info({
            err: err,
            duration_ms: info.duration_ms
        }, 'SnapShotter._deleteSnapshot: exiting');

        callback(err, snapshot);
    });
};

/*
 * Run a "zfs" command, reporting the start and end, and the resultant output,
 * at the TRACE level in the log.
 *
 * The "label" property of the options object should be a string that reflects
 * the reason we are running this command, and the "args" array should be the
 * arguments to "zfs".
 *
 * The callback will be called with the following two arguments:
 *
 *   error:
 *     A VError object if execution was not successful, or null
 *     for successful executions.
 *
 *   info:
 *     The "info" object returned by forkExecWait(), augmented with
 *     the property "duration_ms": the total execution time as a number
 *     of milliseconds.  The most useful property of this object is
 *     the string-valued property "stdout", containing the result of
 *     a successful process execution.
 *
 */
SnapShotter.prototype._execZfs = function (opts, callback) {
    var self = this;

    assert.object(opts, 'opts');
    assert.string(opts.label, 'opts.label');
    assert.arrayOfString(opts.args, 'opts.args');

    assert.number(self._zfsRuns);
    var log = self._log.child({
        zfs_run: ++self._zfsRuns,
        zfs_label: opts.label,
        zfs_args: opts.args
    });

    log.trace('_execZfs: start zfs command');
    var begin = process.hrtime();

    forkexec.forkExecWait({
        argv: [
            '/sbin/zfs'
        ].concat(opts.args),
        includeStderr: true
    }, function (err, info) {
        var dur = process.hrtime(begin);

        if (!info) {
            info = {};
        }
        info.duration_ms = Math.round(dur[0] * 1000 + dur[1] / 1000000);

        log.trace({
            err: err,
            zfs_info: info
        }, '_execZfs: end zfs command');

        /*
         * Note that "forkexec" already adorns the error object with a
         * reasonably complete message about the exact failure, including the
         * stderr output if there was any.
         */
        callback(err, info);
    });
};

/** #@- */
