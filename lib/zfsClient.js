// Copyright (c) 2012, Joyent, Inc. All rights reserved.
var assert = require('assert-plus');
var EventEmitter = require('events').EventEmitter;
var restify = require('restify');
var shelljs = require('shelljs');
var spawn = require('./shellSpawner').spawn;
var util = require('util');
var uuid = require('node-uuid');
var vasync = require('vasync');
var verror = require('verror');

/**
 * Zfs client -- interactions with zfs from node
 */
function ZfsClient(options) {
        assert.object(options, 'options');
        assert.object(options.log, 'options.log');
        assert.string(options.dataset, 'options.parentDataset');
        assert.string(options.dataset, 'options.dataset');
        assert.string(options.snapshotDir, 'options.snapshotDir');
        assert.string(options.mountpoint, 'options.mountpoint');
        assert.string(options.serverUrl, 'options.serverUrl');
        assert.string(options.zfsHost, 'options.zfsHost');
        assert.number(options.zfsPort, 'options.zfsPort');
        assert.number(options.pollInterval, 'options.pollInterval');
        assert.string(options.zfsRecvPath, 'options.zfsRecvPath');
        assert.object(options.snapShotter, 'options.snapShotter');

        EventEmitter.call(this);

        var self = this;

        this.log = options.log;

        /**
        * The zfs dataset of this peer.
        */
        this.dataset = options.dataset;
        this.parentDataset = options.parentDataset;

        /**
         * The zfs mountpoint of the dataset
         */
        this.mountpoint = options.mountpoint;

        /**
        * The peer's .zfs dir which stores a list of the current snapshots
        */
        this.snapshotDir = options.snapshotDir;

        /**
        * The backup server's url. http://10.0.0.0:1234
        */
        this.serverUrl= options.serverUrl;

        /**
        * The ip addr used for zfs recv
        */
        this.zfsHost = options.zfsHost;

        /**
        * The port used for zfs recv
        */
        this.zfsPort = options.zfsPort;

        /**
        * Path to the zfs_recv binary
        */
        this.zfs_recv = options.zfsRecvPath;

        /**
         * The zfs snapshotter
         */
        this.snapShotter = options.snapShotter;

        this.pollInterval = options.pollInterval;

        this.intervalId = null;

        this.client = restify.createJsonClient({
                url: self.serverUrl,
                version: '*'
        });

        this.log.info('initializing ZfsClient with options', options);
}

module.exports = ZfsClient;
util.inherits(ZfsClient, EventEmitter);

ZfsClient.prototype.restore = function restore(cb) {
        var self = this;
        var log = self.log;

        var backupDataset;

        log.info({
                dataset: self.dataset,
                snapshotDir: self.snapshotDir,
                serverUrl: self.serverUrl,
                pollInterval: self.pollInterval
        }, 'ZFSClient.restore: entering');
        var tasks = [
                function _receive(_, cb) {
                        self.receive(function(err, dataset) {
                                backupDataset = dataset;
                                return cb(err);
                        });
                },
                function _setMountPoint(_, cb) {
                        spawn('pfexec zfs set mountpoint=' +
                                self.mountpoint + ' ' + self.dataset,
                        log, cb);
                },
                function _setNoAutoMount(_, cb) {
                        spawn('pfexec zfs set canmount=noauto ' +
                        self.dataset, log, cb);
                },
                function _mount(_, cb) {
                        spawn('pfexec zfs mount ' + self.dataset, log, cb);
                }
        ];
        vasync.pipeline({funcs: tasks, arg: {}}, function(err) {
                if (err) {
                        log.info({
                                err: err,
                                dataset: self.dataset,
                                snapshotDir: self.snapshotDir,
                                serverUrl: self.serverUrl,
                                pollInterval: self.pollInterval
                        }, 'unable to restore snapshot');
                        return cb(err);
                } else {
                        log.info({
                                dataset: self.dataset,
                                backupDataset: backupDataset,
                                snapshotDir: self.snapshotDir,
                                serverUrl: self.serverUrl,
                                pollInterval: self.pollInterval
                        }, 'ZFSClient.restore: success');
                        return cb(null, backupDataset);
                }
        });
};


/**
* Restore the pg data dir using zfs recv. First takes a snapshot and clones it
* to a backup dataset in case the recv fails.
*
* @param {Function} callback The callback of the form f(err, backupDataset)
*/
ZfsClient.prototype.receive = function receive(callback) {
        var self = this;
        var log = self.log;

        var backupDataset;

        log.info({
                dataset: self.dataset,
                snapshotDir: self.snapshotDir,
                serverUrl: self.serverUrl,
                pollInterval: self.pollInterval
        }, 'ZfsClient.restore: entering');

        var emitter = new EventEmitter();
        emitter.once('zfs-err', function(err) {
                emitter.removeAllListeners('zfs-done');
                log.info({
                        err: err,
                        dataset: self.dataset,
                        snapshotDir: self.snapshotDir,
                        serverUrl: self.serverUrl,
                        pollInterval: self.pollInterval
                }, 'zfs receive failed');
                return callback(err);
        });

        emitter.once('zfs-done', function() {
                emitter.removeAllListeners('zfs-err');
                clearInterval(self.intervalId);
                log.info({
                        dataset: self.dataset,
                        backupDataset: backupDataset,
                        snapshotDir: self.snapshotDir,
                        serverUrl: self.serverUrl,
                        pollInterval: self.pollInterval
                }, 'sucessfully received backup image', self.intervalId);
                self.intervalId = null;

                return callback(null, backupDataset);
        });

        var tasks = [
                function _backupCurrentDataset(_, cb) {
                        self.backupCurrentDataset(function(err, dataset) {
                                backupDataset = dataset;
                                return cb(err);
                        });
                },
                function destroyCurrentDataset(_, cb) {
                        spawn('pfexec zfs destroy -r ' + self.dataset, log, cb);
                },
                function _startZfsRecv(_, cb) {
                        var cmd = self.zfs_recv + ' 0 ' + self.zfsPort +
                                  ' -u ' + self.dataset;
                        spawn(cmd, log, function(err) {
                                log.info('ZfsClient.receive: zfs recv has ' +
                                         'finished');
                                 if (err) {
                                         log.info({err: err},
                                         'zfs recv failed');
                                         emitter.emit('zfs-err', err);
                                 } else {
                                         emitter.emit('zfs-done');
                                 }
                        });
                        return cb();
                },
                function _postRestoreRequest(_, cb) {
                        postRestoreRequest(self, function(err, jobPath) {
                                _.jobPath = jobPath;
                                return cb(err);
                        });
                },
                // make sure we're always making forward progress -- because
                // zfs recv can potentially sit and wait forever.
                function _pollRestoreCompletion(_, cb) {
                        pollRestoreCompletion(self, _.jobPath, emitter, cb);
                }
        ];

        vasync.pipeline({funcs: tasks, arg: {}}, function(err) {
                if (err) {
                        log.info({
                                err: err,
                                dataset: self.dataset,
                                snapshotDir: self.snapshotDir,
                                serverUrl: self.serverUrl,
                                pollInterval: self.pollInterval
                        }, 'unable to receive snapshot');
                        emitter.removeAllListeners('zfs-done');
                        emitter.removeAllListeners('zfs-err');
                        return callback(err);
                } else {
                        log.info({
                                dataset: self.dataset,
                                snapshotDir: self.snapshotDir,
                                serverUrl: self.serverUrl,
                                pollInterval: self.pollInterval
                        }, 'enqueued receive request');
                        return true;
                }
        });
};

/**
 * Restores a dataset from a snapshot
 */
ZfsClient.prototype.restoreDataset = function restoreDataset(snapshot,
                                                             dataset, cb) {
        var self = this;
        var log = self.log;

        log.info({
                backupDataset: dataset,
                snapshot: snapshot
        }, 'ZfsClient.restoreDataset: entering');

        var tasks = [
                function _destroyDataset(_, cb) {
                        spawn('pfexec zfs destroy -r ' + dataset, log, cb);
                },
                function _cloneDataset(_, cb) {
                        // TODO: we want to set canmount=noauto such that we
                        // don't auto mount the snapshot. This doesn't
                        // currently work , blocked on OS-1870
                        spawn('pfexec zfs clone -o canmount=off ' + snapshot +
                              ' ' + dataset, log, cb);
                },
                function _promoteDataset(_, cb) {
                        spawn('pfexec zfs promote ' + dataset, log, cb);
                },
                function _destroyBackupDataset(_, cb) {
                        spawn('pfexec zfs destroy -r ' + snapshot.split('@')[0],
                              log, cb);
                },
                function _setCanmountOn(_, cb) {
                        spawn('pfexec zfs set canmount=on ' + dataset,
                              log, cb);
                },
                function _setMountpoint(_, cb) {
                        spawn('pfexec zfs set mountpoint=' + self.mountpoint +
                              ' ' + dataset, log, cb);
                },
                function _zfsMount(_, cb) {
                        spawn('pfexec zfs mount ' + dataset, log, cb);
                }
        ];

        vasync.pipeline({funcs: tasks, arg: {}}, function(err) {
                if (err) {
                        log.info({
                                err: err,
                                dataset: dataset,
                                snapshot: snapshot
                        }, 'unable to restore dataset');
                        return cb(err);
                } else {
                        log.info({
                                dataset: dataset,
                                snapshot: snapshot
                        }, 'restored dataset');
                        return cb();
                }
        });
}

/**
* Posts a restore request to the primary peer in the shard
*/
function postRestoreRequest(self, callback) {
        var log = self.log;
        var request = {
                host: self.zfsHost,
                port: self.zfsPort,
                dataset: self.dataset
        };

        log.info({
                zfsHost: request.host,
                zfsPort: request.port,
                snapshotDir: self.snapshotDir,
                serverUrl: self.serverUrl,
                pollInterval: self.pollInterval
        }, 'Sending restore request');
        self.client.post('/backup', request, function(err, req, res, obj) {
                if (err) {
                        log.info({
                                err: err,
                                dataset: self.dataset,
                                snapshotDir: self.snapshotDir,
                                serverUrl: self.serverUrl,
                                pollInterval: self.pollInterval
                        }, 'posting restore request failed');
                        return callback(err);
                }

                self.log.info({
                        dataset: self.dataset,
                        snapshotDir: self.snapshotDir,
                        serverUrl: self.serverUrl,
                        pollInterval: self.pollInterval,
                        response: obj
                }, 'successfully posted restore request');
                return callback(null, obj.jobPath);
        });
}

/**
*  Polls the restore service for the status of the backup job.
*
*  @param {string} jobPath The REST path of the backup job.
*  @param {function} callback The callback of the form f(err, job) where job
*  is the job object returned from the server, and err indicates an error
*  either polling for the job or in the job itself.
*
*/
function pollRestoreCompletion(self, jobPath, emitter, callback) {
        var log = self.log;
        log.info({jobpath: jobPath},
                'zfsClient.pollRestoreCompletion: entering');
        self.intervalId = setInterval(function() {
                // clearInterval() may not stop any already enqueued
                // tasks, so we only return callback if intervalId is
                // not null
                if (!self.intervalId) {
                        log.info('aborting already cancelled pollRestore');
                        return;
                }
                log.debug({
                        dataset: self.dataset,
                        snapshotDir: self.snapshotDir,
                        serverUrl: self.serverUrl,
                        pollInterval: self.pollInterval,
                        jobPath: jobPath
                }, 'getting restore job status', self.intervalId);

                self.client.get(jobPath, function(err, req, res, obj) {
                        if (err) {
                                log.info({
                                        err: err,
                                        dataset: self.dataset,
                                        snapshotDir: self.snapshotDir,
                                        serverUrl: self.serverUrl,
                                        jobPath: jobPath,
                                        pollInterval: self.pollInterval,
                                        intervalId: self.intervalId
                                }, 'error getting restore job status');
                                clearInterval(self.intervalId);
                                self.intervalId = null;
                                return emitter.emit('zfs-err', err);
                        }

                        log.debug({
                                backupJob: obj
                        }, 'got restore job status');

                        if (obj.done === true) {
                                log.info('restore job is done',
                                         self.intervalId);
                                clearInterval(self.intervalId);
                                self.intervalId = null;
                                return true;
                        } else if (obj.done === 'failed') {
                                var msg = 'restore job failed';
                                var err2 = new verror.VError(msg, err);
                                log.info({
                                        err: err2,
                                        dataset: self.dataset,
                                        snapshotDir: self.snapshotDir,
                                        serverUrl: self.serverUrl,
                                        jobPath: jobPath,
                                        pollInterval: self.pollInterval
                                }, 'restore job failed', self.intervalId);

                                clearInterval(self.intervalId);
                                self.intervalId = null;
                                return emitter.emit('zfs-err', err2);
                        } else {
                                log.info({
                                        dataset: self.dataset,
                                        snapshotDir: self.snapshotDir,
                                        serverUrl: self.serverUrl,
                                        jobPath: jobPath,
                                        pollInterval: self.pollInterval
                                }, 'restore job not complete', self.intervalId);
                                return true;
                        }
                });
        }, self.pollInterval);

        return callback();
}


/**
 * @return callback Function f(err, backupDataset)
 */
ZfsClient.prototype.backupCurrentDataset =
        function backupCurrentDataset(callback)
{
        var self = this;
        var log = self.log;
        log.debug({
                dataset: self.dataset,
                snapshotDir: self.snapshotDir
        }, 'entering ZfsClient.backupCurrentDataset');

        var backupDataset;
        var snapshotId = Date.now();

        var tasks = [
                // first take a snapshot of the current dataset
                function _takeSnapshot(_, cb) {
                        _.srcSnapshot = self.dataset + '@' + snapshotId;
                        spawn('pfexec zfs snapshot ' + _.srcSnapshot, log, cb);
                },
                function _cloneDataset(_, cb) {
                        backupDataset = self.parentDataset + '/' + uuid();
                        // TODO: we want to set canmount=noauto such that we
                        // don't auto mount the snapshot. This doesn't
                        // currently work , blocked on OS-1870
                        spawn('pfexec zfs clone -o canmount=off ' +
                               _.srcSnapshot + ' ' + backupDataset, log, cb);
                },
                function _setCanmountOn(_, cb) {
                        spawn('pfexec zfs set canmount=on ' + backupDataset,
                              log, cb);
                },
                function _setMountpoint(_, cb) {
                        spawn('pfexec zfs set mountpoint=' + self.mountpoint +
                              ' ' + backupDataset, log, cb);
                },
                function _promoteDataset(_, cb) {
                        spawn('pfexec zfs promote ' + backupDataset, log, cb);
                }
        ];

        vasync.pipeline({funcs: tasks, arg: {}}, function(err) {
                if (err) {
                        log.error({
                                err: err,
                                dataset: self.dataset,
                                snapshotDir: self.snapshotDir
                        }, 'ZfsClient.backupCurrentDataset: error');
                        return callback(err);
                } else {
                        log.info({
                                backupDataset: backupDataset,
                                dataset: self.dataset,
                                snapshotDir: self.snapshotDir
                        }, 'ZfsClient.backupCurrentDataset: complete');
                        return callback(null, backupDataset + '@' + snapshotId);
                }
        });
}
