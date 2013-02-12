// Copyright (c) 2012, Joyent, Inc. All rights reserved.
var assert = require('assert-plus');
var EventEmitter = require('events').EventEmitter;
var shelljs = require('shelljs');
var shellSpawner = require('./shellSpawner').spawn;
var util = require('util');
var pg = require('pg');
var Client = pg.Client;
var verror = require('verror');

/**
* Takes periodic zfs snapshots of a pg data dir.
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

        /**
        * The snapshot period in ms
        */
        this.pollInterval = options.pollInterval;

        /**
        * The ZFS dataset to snapshot
        */
        this.dataset = options.dataset;

        /**
        * The .zfs dir where snapshots are stored
        */
        this.snapshotDir = options.snapshotDir;

        /**
        * The number of snapshots to retain
        */
        this.snapshotNumber = options.snapshotNumber;

        /**
        * The URL of the postgres instance.
        * Used to start and stop backups of pg.
        */
        this.pgUrl = options.pgUrl;

        this.log.trace('initialized snapshotter with options', options);
}

module.exports = SnapShotter;
util.inherits(SnapShotter, EventEmitter);

SnapShotter.prototype.start = function start(callback) {
        var self = this;
        var log = self.log;

        log.info('starting snapshotter daemon');

        // manually start the first time as setInterval waits the interval
        // before starting
        self.createSnapshot(Date.now(), function(err) {
                if (err) {
                        log.error({err: err}, 'unable to create snapshot');
                        self.emit('error', err);
                }
        });

        setInterval(function() {
                self.createSnapshot(Date.now(), function(err) {
                        if (err) {
                                log.error({err: err},
                                          'unable to create snapshot');
                                self.emit('error', err);
                        }
                });

                var snapshots = shelljs.ls(self.snapshotDir);
                // sort snapshots by earliest ones first
                snapshots.sort(function(a, b) {
                        return a - b;
                });

                log.debug('got snapshots', snapshots);
                // delete snapshots
                if (snapshots.length > self.snapshotNumber) {
                        log.info({
                                numberOfSnapshots: snapshots.length,
                                threshHold: self.snapshotNumber
                        }, 'deleting snapsshots as number of snapshots' +
                        ' exceeds threshold');

                        for (var i = 0;
                             i < snapshots.length - self.snapshotNumber;
                             i++)
                        {
                                var delSnapshot =
                                        self.dataset + '@' + snapshots[i];

                                deleteSnapshot(self,
                                               delSnapshot,
                                               function(err)
                                {
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
*/
SnapShotter.prototype.createSnapshot = function createSnapshot(name, callback) {
        var self = this;
        var snapshot = self.dataset + '@' + name;
        var log = self.log;
        log.info({
                snapshot: snapshot
        }, 'creating snapshot');
        pgStartBackup(self, function(backupErr) {
                writeSnapshot(self, snapshot, function(snapshotErr) {
                        pgStopBackup(self, function(stopErr) {
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
* Write a zfs snapshot to disk.
* @param String snapshot The name of the snapshot.
*/
function writeSnapshot(self, snapshot, callback) {
        var log = self.log;
        log.info({
                snapshot: snapshot
        }, 'SnapShotter.writeSnapshot: entering');

        shellSpawner('pfexec zfs snapshot ' + snapshot, log, funtion(err) {
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
function deleteSnapshot(self, snapshot, callback) {
        var log = self.log;
        log.info({
                snapshot: snapshot
        }, 'SnapShotter.deleteSnapshot: entering');

        shellSpawner('pfexec zfs destroy ' + snapshot, log,
                           function(err) {
                if (err) {
                        err = new verror.VError(err);
                }

                log.info({
                        err: err,
                        snapshot: snapshot
                }, 'SnapShotter.deleteSnapshot: exiting');

                return callback(err);
        });
}

function pgStopBackup(self, callback) {
        self.log.info('stopping pg_start_backup');
        var queryString = 'SELECT pg_stop_backup();';
        queryDb(self, queryString, function(err, result) {
                return callback(err, result);
        });
}

function pgStartBackup(self, callback) {
        self.log.info('starting pg_start_backup');
        var label = new Date().getTime();
        var queryString = 'SELECT pg_start_backup(\'' + label + '\', true);';
        queryDb(self, queryString, function(err, result) {
                return callback(err, result);
        });
}

function queryDb(self, query, callback) {
        var log = self.log;
        log.debug('entering querydb %s', query);

        var client = new Client(self.pgUrl);
        client.connect(function(err) {
                if (err) {
                        log.info({err: err},
                                'can\'t connect to pg with err');
                        client.end();
                        return callback(err);
                }
                log.debug('connected to pg, running query %s', query);
                client.query(query, function(err2, result) {
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
