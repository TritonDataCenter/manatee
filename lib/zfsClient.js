// Copyright (c) 2012, Joyent, Inc. All rights reserved.
var assert = require('assert-plus');
var EventEmitter = require('events').EventEmitter;
var once = require('once');
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

        this.client = restify.createJsonClient({
                url: self.serverUrl,
                version: '*'
        });

        this.log.info('initializing ZfsClient with options', options);
}

module.exports = ZfsClient;
util.inherits(ZfsClient, EventEmitter);

/**
 * @param {Function} cb The callback of the form f(err, backupSnapshot)
 */
ZfsClient.prototype.restore = function restore(cb) {
        var self = this;
        var log = self.log;

        var backupSnapshot;

        log.info({
                dataset: self.dataset,
                serverUrl: self.serverUrl,
                pollInterval: self.pollInterval
        }, 'ZFSClient.restore: entering');
        var tasks = [
                //function _backupCurrentDataset(_, cb) {
                        //backupDataset(self, self.dataset,
                                      //self.parentDataset + '/' + uuid(),
                                      //self.mountpoint, function(err, snap)
                        //{
                                //backupSnapshot = snap;
                                //return cb(err);
                        //});
                //},
                function _receive(_, cb) {
                        receive(self, self.dataset, self.serverUrl,
                                self.pollInterval, cb);
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
                                serverUrl: self.serverUrl,
                                pollInterval: self.pollInterval
                        }, 'unable to restore snapshot');
                        return cb(err);
                } else {
                        log.info({
                                dataset: self.dataset,
                                backupDataset: backupDataset,
                                serverUrl: self.serverUrl,
                                pollInterval: self.pollInterval
                        }, 'ZFSClient.restore: success');
                        return cb(null, backupDataset);
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
 * @return callback Function f(err, backupSnapshot)
 */
function backupDataset(self, dataset, backupDataset, mountpoint, callback)
{
        var log = self.log;
        log.debug({
                dataset: dataset,
                backupDataset: backupDataset,
                mountpoint: mountpoint
        }, 'entering ZfsClient.backupCurrentDataset');

        var snapshotId = Date.now();

        var tasks = [
                // first take a snapshot of the current dataset
                function _takeSnapshot(_, cb) {
                        _.srcSnapshot = dataset + '@' + snapshotId;
                        spawn('pfexec zfs snapshot ' + _.srcSnapshot, log, cb);
                },
                function _cloneDataset(_, cb) {
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
                        spawn('pfexec zfs set mountpoint=' + mountpoint +
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
                                dataset: dataset,
                                backupDataset: backupDataset,
                                mountpoint: mountpoint
                        }, 'ZfsClient.backupCurrentDataset: error');
                        return callback(err);
                } else {
                        var backupSnapshot = backupDataset + '@' + snapshotId;
                        log.info({
                                dataset: dataset,
                                backupDataset: backupDataset,
                                backupSnapshot: backupSnapshot,
                                mountpoint: mountpoint
                        }, 'ZfsClient.backupCurrentDataset: complete');
                        return callback(null, backupSnapshot);
                }
        });
};

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
                serverUrl: self.serverUrl,
                pollInterval: self.pollInterval
        }, 'Sending restore request');
        self.client.post('/backup', request, function(err, req, res, obj) {
                if (err) {
                        log.info({
                                err: err,
                                dataset: self.dataset,
                                serverUrl: self.serverUrl,
                                pollInterval: self.pollInterval
                        }, 'posting restore request failed');
                        return callback(err);
                }

                self.log.info({
                        dataset: self.dataset,
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
function pollRestoreCompletion(self, serverUrl, pollInterval, restoreIntervalId,
                               jobPath, callback)
{
        var log = self.log;
        log.info({
                serverUrl: serverUrl,
                jobPath: jobPath,
                pollInterval: pollInterval,
                restoreIntervalId: restoreIntervalId
        }, 'zfsClient.pollRestoreCompletion: entering');
        callback = once(callback);
        restoreIntervalId = setInterval(function() {
                log.debug({
                        serverUrl: serverUrl,
                        jobPath: jobPath,
                        pollInterval: pollInterval,
                        restoreIntervalId: restoreIntervalId
                }, 'getting restore job status');

                self.client.get(jobPath, function(err, req, res, obj) {
                        if (err) {
                                log.info({
                                        err: err,
                                        serverUrl: serverUrl,
                                        jobPath: jobPath,
                                        pollInterval: pollInterval,
                                        restoreIntervalId:
                                                restoreIntervalId
                                }, 'error getting restore job status');
                                clearInterval(restoreIntervalId);
                                return callback(err);
                        }

                        log.debug({
                                backupJob: obj
                        }, 'got restore job status');

                        if (obj.done === true) {
                                log.info('restore job is done',
                                         restoreIntervalId);
                                clearInterval(restoreIntervalId);
                                return callback();
                        } else if (obj.done === 'failed') {
                                var msg = 'restore job failed';
                                var err2 = new verror.VError(msg, err);
                                log.info({
                                        err: err2,
                                        serverUrl: serverUrl,
                                        jobPath: jobPath,
                                        pollInterval: pollInterval,
                                        restoreIntervalId:
                                                restoreIntervalId
                                }, 'restore job failed');

                                clearInterval(restoreIntervalId);
                                return callback(err2);
                        } else {
                                log.info({
                                        serverUrl: serverUrl,
                                        jobPath: jobPath,
                                        pollInterval: pollInterval,
                                        restoreIntervalId:
                                                restoreIntervalId
                                }, 'restore job not complete');
                                return true;
                        }
                });
        }, pollInterval);

        return (undefined);
};

/**
* Restore the pg data dir using zfs recv.
*
* @param {Function} callback The callback of the form f(err)
*/
function receive(self, dataset, serverUrl, pollInterval, callback) {
        var log = self.log;
        callback = once(callback);

        var restoreIntervalId;

        log.info({
                dataset: dataset,
                serverUrl: serverUrl,
                pollInterval: pollInterval,
                restoreIntervalId: restoreIntervalId
        }, 'ZfsClient.restore: entering');

        var tasks = [
                function destroyCurrentDataset(_, cb) {
                        spawn('pfexec zfs destroy -r ' + dataset, log, cb);
                },
                // start the zfs receive first
                function _startZfsRecv(_, cb) {
                        var cmd = self.zfs_recv + ' 0 ' + self.zfsPort +
                                  ' -u ' + self.dataset;
                        spawn(cmd, log, function(err) {
                                clearInterval(restoreIntervalId);
                                log.info({
                                        err: err,
                                        dataset: dataset,
                                        serverUrl: serverUrl,
                                        pollInterval: pollInterval,
                                        restoreIntervalId: restoreIntervalId
                                }, 'ZfsClient.receive: finished');

                                return callback(err);
                        });
                        return cb();
                },
                // then start the send
                function _postRestoreRequest(_, cb) {
                        postRestoreRequest(self, function(err, jobPath) {
                                _.jobPath = jobPath;
                                return cb(err);
                        });
                },
                // make sure we're always making forward progress -- because
                // zfs recv can potentially sit and wait forever.
                function _pollRestoreCompletion(_, cb) {
                        pollRestoreCompletion(self, self.serverUrl,
                                self.pollInterval, restoreIntervalId,
                                _.jobPath, cb);
                }
        ];

        vasync.pipeline({funcs: tasks, arg: {}}, function(err) {
                if (err) {
                        log.info({
                                err: err,
                                dataset: dataset,
                                serverUrl: serverUrl,
                                pollInterval: pollInterval,
                                restoreIntervalId: restoreIntervalId
                        }, 'unable to receive snapshot');
                        // clear the interval incase one of the other funcs
                        // failed
                        clearInterval(restoreIntervalId);
                        return callback(err);
                } else {
                        log.info({
                                dataset: dataset,
                                serverUrl: serverUrl,
                                pollInterval: pollInterval,
                                restoreIntervalId: restoreIntervalId
                        }, 'enqueued receive request');
                        return true;
                }
        });
};
