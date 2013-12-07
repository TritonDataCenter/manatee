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
var shelljs = require('shelljs');
var exec = require('child_process').exec;
var util = require('util');
var pg = require('pg');
var Client = pg.Client;
var verror = require('verror');

/**
 * Takes periodic zfs snapshots of a pg data dir.
 *
 * @constructor
 *
 * @param {object} options Options object.
 * @param {Bunyan} options.log Bunyan logger.
 * @param {string} options.dataset The ZFS dataset to snapshot.
 * @param {number} options.pollInterval How often to take snapshots.
 * @param {string} options.snapshotDir  Path to the snapshot directory of the
 * dataset. i.e.  the .zfs dir.
 * @param {number} options.snapshotNumber Number of snapshots to keep.
 * @param {string} options.pgUrl URL of the PostgreSQL instance.
 */
function SnapShotter(options) {
    assert.object(options, 'options');
    assert.object(options.log, 'options.log');
    assert.string(options.dataset, 'options.dataset');
    assert.number(options.pollInterval, 'options.pollInterval');
    assert.string(options.snapshotDir, 'options.snapshotDir');
    assert.number(options.snapshotNumber, 'options.snapshotNumber');
    assert.string(options.pgUrl, 'options.pgUrl');

    EventEmitter.call(this);

    this.log = options.log;

    /** @type {number} The snapshot period in ms */
    this.pollInterval = options.pollInterval;

    /** @type {string} The ZFS dataset to snapshot */
    this.dataset = options.dataset;

    /** @type {string} The .zfs dir where snapshots are stored */
    this.snapshotDir = options.snapshotDir;

    /** @type {number} The number of snapshots to retain */
    this.snapshotNumber = options.snapshotNumber;

    /**
     * @type {string}
     * The URL of the postgres instance.  Used to start and stop backups of pg.
     */
    this.pgUrl = options.pgUrl;

    this.log.info('initialized snapshotter with options', options);
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
    var log = self.log;

    log.info('starting snapshotter daemon');

    // manually start the first time as setInterval waits the interval before
    // starting
    self.createSnapshot(Date.now(), function (err) {
        if (err) {
            log.error({err: err}, 'unable to create snapshot');
            self.emit('error', err);
        }
    });

    setInterval(function () {
        self.createSnapshot(Date.now(), function (err) {
            if (err) {
                log.error({err: err},
                'unable to create snapshot');
                self.emit('error', err);
            }
        });

        var snapshots = shelljs.ls(self.snapshotDir);
        // sort snapshots by earliest ones first
        snapshots.sort(function (a, b) {
            return a - b;
        });

        log.debug('got snapshots', snapshots);
        // delete snapshots
        if (snapshots.length > self.snapshotNumber) {
            log.info({
                numberOfSnapshots: snapshots.length,
                threshHold: self.snapshotNumber
            }, 'deleting snapsshots as number of snapshots exceeds threshold');

            for (var i = 0; i < snapshots.length - self.snapshotNumber; i++) {
                var delSnapshot =
                self.dataset + '@' + snapshots[i];

                self._deleteSnapshot(delSnapshot, function (err) {
                    if (err) {
                        log.error({
                            err: err,
                            snapshot: delSnapshot
                        }, 'unable to delete snapshot');
                        self.emit('error', err);
                    }
                });
            }
        }
    }, self.pollInterval);

    log.info('started snapshotter daemon');
    return callback();
};

/**
 * Creates a zfs snapshot of the current postgres data directory.
 * 3 steps to creating a snapshot.
 * 1) pg_start_backup.
 * 2) write snapshot to zfs.
 * 3) pg_stop_backup.
 *
 * @param {string} name The name of the snapshot.
 * @param {Snapshotter-Cb} callback
 */
SnapShotter.prototype.createSnapshot = function createSnapshot(name, callback) {
    var self = this;
    var snapshot = self.dataset + '@' + name;
    var log = self.log;
    log.info({
        snapshot: snapshot
    }, 'creating snapshot');
    self._pgStartBackup(function (backupErr) {
        self._writeSnapshot(snapshot, function (snapshotErr) {
            self._pgStopBackup(function (stopErr) {
                if (stopErr || snapshotErr || backupErr) {
                    log.warn({
                        backupErr: backupErr,
                        snapshotErr: snapshotErr,
                        stopErr: stopErr
                    }, 'error while creating snapshot');
                }

                return callback(snapshotErr);
            });
        });
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
    var log = self.log;
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
}

/**
 * Delete a zfs snapshot from disk
 * @param String snapshot The name of the snapshot.
 */
SnapShotter.prototype._deleteSnapshot = function (snapshot, callback) {
    var self = this;
    var log = self.log;
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

        return callback(err);
    });
}

SnapShotter.prototype._pgStopBackup = function (callback) {
    var self = this;
    self.log.info('stopping pg_start_backup');
    var queryString = 'SELECT pg_stop_backup();';
    self._queryDb(queryString, function (err, result) {
        return callback(err, result);
    });
}

SnapShotter.prototype._pgStartBackup = function (callback) {
    var self = this;
    self.log.info('starting pg_start_backup');
    var label = new Date().getTime();
    var queryString = 'SELECT pg_start_backup(\'' + label + '\', true);';
    self._queryDb(queryString, function (err, result) {
        return callback(err, result);
    });
}

SnapShotter.prototype._queryDb = function (query, callback) {
    var self = this;
    var log = self.log;
    log.debug('entering querydb %s', query);

    var client = new Client(self.pgUrl);
    client.connect(function (err) {
        if (err) {
            log.info({err: err},
            'can\'t connect to pg with err');
            client.end();
            return callback(err);
        }
        log.debug('connected to pg, running query %s', query);
        client.query(query, function (err2, result) {
            client.end();
            if (err2) {
                log.info({err: err2},
                'error whilst querying pg ');
            }
            return callback(err2, result);
        });
        return true;
    });
}

/** #@- */
