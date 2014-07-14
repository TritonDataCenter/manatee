/**
 * @overview Daemon that takes periodic zfs snapshots of the postgres data dir.
 * @copyright Copyright (c) 2013, Joyent, Inc. All rights reserved.
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
var exec = require('child_process').exec;
var once = require('once');
var util = require('util');
var vasync = require('vasync');
var verror = require('verror');

/**
 * Takes periodic zfs snapshots of a pg data dir.
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
 *
 * @throws {Error} If the options object is malformed.
 */
function SnapShotter(options) {
    assert.object(options, 'options');
    assert.object(options.log, 'options.log');

    assert.string(options.dataset, 'options.dataset');
    assert.optionalNumber(options.pollInterval, 'options.pollInterval');
    assert.optionalNumber(options.snapshotNumber, 'options.snapshotNumber');

    EventEmitter.call(this);

    var self = this;

    this._log = options.log;

    /** @type {number} The snapshot period in ms */
    this._pollInterval = options.pollInterval || 1 * 1000;

    /** @type {string} The ZFS dataset to snapshot */
    this._dataset = options.dataset;

    /** @type {number} The number of snapshots to retain */
    this._snapshotNumber = options.snapshotNumber || 10;

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
        self.createSnapshot(Date.now(), function (err) {
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
    // manually start the first time as setInterval waits the interval before
    // starting
    create();

    setInterval(create, self._pollInterval);

    (function cleanup() {
        log.info('cleaning up snapshots');
        vasync.pipeline({funcs: [
            function _getSnapshots(_, cb) {
                /*
                 * get the snapshot and sort ascending by name. This guarantees
                 * the earliest snapshot is on top.
                 */
                var cmd = 'zfs list -t snapshot -H -d 1 -s name -o name ' +
                    self._dataset;
                exec(cmd, function (err, stdout, stderr) {
                    log.debug({snapshots: stdout}, 'got snapshots');
                    _.snapshots = stdout.split('\n');
                    return cb(err);
                });
            },
            function _stripSnapshots(_, cb) {
                /*
                 * MANATEE-214 A snapshot name is just time since epoch in ms.
                 * So it's a 13 digit number like 1405378955344. We only want
                 * snapshots that look like this to avoid using other snapshots
                 * as they may have been created by an operator.
                 */
                var regex = /^\d{13}$/;
                var snaps = [];
                for (var i = 0; i < _.snapshots.length; i++) {
                    var snapshot = _.snapshots[i].split('@')[1];
                    // only push snapshots that we created.
                    if (regex.test(snapshot) === true) {
                        snaps.push(_.snapshots[i]);
                    }
                }

                _.snapshots = snaps;

                return cb();
            },
            function _deleteSnapshots(_, cb) {
                cb = once(cb);
                var snapshots = _.snapshots;
                if (snapshots.length > self._snapshotNumber) {
                    log.info({
                        numberOfSnapshots: snapshots.length,
                        threshHold: self._snapshotNumber
                    }, 'deleting snapshots as number exceeds threshold');

                    var barrier = vasync.barrier();

                    barrier.on('drain', cb);
                    for (var i = 0;
                         i < snapshots.length - self._snapshotNumber;
                         i++) {

                        var delSnapshot = snapshots[i];
                        barrier.start(delSnapshot);

                        /* jshint loopfunc: true */
                        self._deleteSnapshot(delSnapshot,
                                             function (err, snapshot) {
                            if (err) {
                                log.error({
                                    err: err,
                                    snapshot: delSnapshot
                                }, 'unable to delete snapshot');
                                return cb(err);
                            }
                            barrier.done(snapshot);
                        });
                    }
                } else {
                    return cb();
                }
            }
        ], arg: {}}, function (err, results) {
            if (err) {
                log.fatal({err: err, results: results},
                          'unable to maintain snapshots');
                /**
                 * SnapShotter error event.
                 *
                 * @event SnapShotter#error
                 * @type {Error}
                 */
                self.emit('error', err);
            } else {
                setTimeout(cleanup, self._pollInterval);
            }
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
    var snapshot = self._dataset + '@' + name;
    var log = self._log;
    log.info({
        snapshot: snapshot
    }, 'creating snapshot');
    self._writeSnapshot(snapshot, function (err) {
        if (err) {
            log.warn({err: err}, 'error while creating snapshot');
        }

        // ignore all errors and try again later.
        return callback();
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
    var log = self._log;
    var cmd = 'pfexec zfs snapshot ' + snapshot;
    log.info({
        snapshot: snapshot,
        cmd: cmd
    }, 'SnapShotter.writeSnapshot: entering');

    exec(cmd, function (err) {
        if (err) {
            err = new verror.VError(err);
        }

        log.info({
            err: err,
            snapshot: snapshot
        }, 'SnapShotter.writeSnapshot: exiting');

        return callback(err);
    });
};

/**
 * Delete a zfs snapshot from disk
 * @param String snapshot The name of the snapshot.
 */
SnapShotter.prototype._deleteSnapshot = function (snapshot, callback) {
    var self = this;
    var log = self._log;
    var cmd = 'pfexec zfs destroy ' + snapshot;
    log.info({
        snapshot: snapshot,
        cmd: cmd
    }, 'SnapShotter._deleteSnapshot: entering');

    exec(cmd, function (err) {
        if (err) {
            err = new verror.VError(err);
        }

        log.info({
            err: err,
            snapshot: snapshot
        }, 'SnapShotter._deleteSnapshot: exiting');

        return callback(err, snapshot);
    });
};

/** #@- */
