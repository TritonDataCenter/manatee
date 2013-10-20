/*
 * Copyright (c) 2013, Joyent, Inc. All rights reserved.
 *
 *                   _.---.._
 *      _        _.-' \  \    ''-.
 *    .'  '-,_.-'   /  /  /       '''.
 *   (       _                     o  :
 *    '._ .-'  '-._         \  \-  ---]
 *                  '-.___.-')  )..-'
 *                           (_/
 */
var EventEmitter = require('events').EventEmitter;

var assert = require('assert-plus');
var exec = require('child_process').exec;
var once = require('once');
var restify = require('restify');
var shelljs = require('shelljs');
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
    assert.string(options.parentDataset, 'options.parentDataset');
    assert.string(options.dataset, 'options.dataset');
    assert.string(options.dbUser, 'options.dbUser');
    assert.string(options.mountpoint, 'options.mountpoint');
    assert.optionalString(options.serverUrl, 'options.serverUrl');
    assert.string(options.zfsHost, 'options.zfsHost');
    assert.number(options.zfsPort, 'options.zfsPort');
    assert.number(options.pollInterval, 'options.pollInterval');
    assert.string(options.zfsRecvPath, 'options.zfsRecvPath');

    EventEmitter.call(this);

    var self = this;

    this.log = options.log;
    this.log.info('initializing ZfsClient with options', options);

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
     * the dbuser
     */
    this.dbUser = options.dbUser;

    /**
     * The backup server's url. http://10.0.0.0:1234
     */
    this.serverUrl = options.serverUrl;

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

    this.pollInterval = options.pollInterval;

    if (self.serverUrl) {
        this.client = restify.createJsonClient({
            url: self.serverUrl,
            version: '*'
        });
    }

    this.log.info({
        dataset: self.dataset,
        parentDataset: self.parentDataset,
        mountpoint: self.mountpoint,
        serverUrl: self.serverUrl,
        zfsHost: self.zfsHost,
        zfsPort: self.zfsPort,
        zfs_recv: self.zfs_recv,
        pollInterval: self.pollInterval,
        client: self.client
    }, 'initalized ZfsClient');
}

module.exports = ZfsClient;
util.inherits(ZfsClient, EventEmitter);

/**
 * Restores the current dataset from the remote.
 * @param {Function} cb The callback of the form f(err, backupSnapshot)
 */
ZfsClient.prototype.restore = function restore(callback) {
    var self = this;
    var log = self.log;

    var backupSnapshot;

    log.info({
        dataset: self.dataset,
        serverUrl: self.serverUrl,
        pollInterval: self.pollInterval
    }, 'ZFSClient.restore: entering');
    var tasks = [
        function _backupCurrentDataset(_, cb) {
            backupDataset(self, self.dataset, self.parentDataset + '/' + uuid(),
                self.mountpoint, function (err, snap)
                {
                    backupSnapshot = snap;
                    return cb(err);
                });
        },
        function _receive(_, cb) {
            receive(self, self.dataset, self.serverUrl, self.pollInterval, cb);
        },
        function _setMountPoint(_, cb) {
            var cmd = 'zfs set mountpoint=' +
                self.mountpoint + ' ' + self.dataset;
            log.info({ cmd: cmd }, 'ZfsClient.restore: exec');
            exec(cmd, cb);
        },
        function _setNoAutoMount(_, cb) {
            var cmd = 'zfs set canmount=noauto ' + self.dataset;
            log.info({ cmd: cmd }, 'ZfsClient.restore: exec');
            exec(cmd, cb);
        },
        function _mount(_, cb) {
            var cmd = 'zfs mount ' + self.dataset;
            log.info({ cmd: cmd }, 'ZfsClient.restore: exec');
            exec(cmd, cb);
        },
        // MANATEE-119 - snapshot after restoring the dataset from a primary
        function _snapshot(_, cb) {
            var cmd = 'zfs snapshot ' + self.dataset + '@' +
                new Date().getTime();
            log.info({ cmd: cmd }, 'ZfsClient.restore: exec');
            exec(cmd, cb);
        }
    ];
    vasync.pipeline({funcs: tasks, arg: {}}, function (err) {
        if (err) {
            log.info({
                err: err,
                dataset: self.dataset,
                backupSnapshot: backupSnapshot,
                serverUrl: self.serverUrl,
                pollInterval: self.pollInterval
            }, 'unable to restore snapshot');
            return callback(err, backupSnapshot);
        } else {
            log.info({
                dataset: self.dataset,
                backupSnapshot: backupSnapshot,
                serverUrl: self.serverUrl,
                pollInterval: self.pollInterval
            }, 'ZFSClient.restore: success');
            return callback(null, backupSnapshot);
        }
    });
};

/**
 * Restores a dataset from a snapshot
 * @param callback Function f(err)
 */
ZfsClient.prototype.restoreDataset = function restoreDataset(snapshot, callback)
{
    var self = this;
    var log = self.log;
    var dataset = self.dataset;

    log.info({
        dataset: dataset,
        snapshot: snapshot
    }, 'ZfsClient.restoreDataset: entering');

    var tasks = [
        function _destroyDataset(_, cb) {
            var cmd = 'zfs destroy -r ' + dataset;
            log.info({cmd: cmd}, 'ZfsClient.restoreDataset: exec');
            exec(cmd, function (err) {
                log.info({err: new verror.VError(
                    'zfs destroy failed', err)});
                    // don't care if the destroy fails
                    return cb();
            });
        },
        function _cloneDataset(_, cb) {
            // TODO: we want to set canmount=noauto such that we don't auto
            // mount the snapshot. This doesn't currently work , blocked on
            // OS-1870
            var cmd = 'zfs clone -o canmount=off ' + snapshot + ' ' + dataset;
            log.info({cmd: cmd}, 'ZfsClient.restoreDataset: exec');
            exec(cmd, cb);
        },
        function _promoteDataset(_, cb) {
            var cmd = 'zfs promote ' + dataset;
            log.info({cmd: cmd}, 'ZfsClient.restoreDataset: exec');
            exec(cmd, cb);
        },
        function _destroyBackupDataset(_, cb) {
            var cmd = 'zfs destroy -r ' +
                snapshot.split('@')[0];
            log.info({cmd: cmd}, 'ZfsClient.restoreDataset: exec');
            exec(cmd, cb);
        },
        function _setCanmountNoauto(_, cb) {
            var cmd = 'zfs set canmount=noauto ' + dataset;
            log.info({cmd: cmd}, 'ZfsClient.restoreDataset: exec');
            exec(cmd, cb);
        },
        function _setMountpoint(_, cb) {
            var cmd = 'zfs set mountpoint=' +
                self.mountpoint + ' ' + dataset;
            log.info({cmd: cmd}, 'ZfsClient.restoreDataset: exec');
            exec(cmd, cb);
        },
        function _mkdirpMountpoint(_, cb) {
            var cmd = 'mkdir -p ' + self.mountpoint;
            log.info({cmd: cmd}, 'ZfsClient.restoreDataset: exec');
            exec(cmd, cb);
        },
        function _chownMountPoint(_, cb) {
            var cmd = 'sudo chown -R ' + self.dbUser + ' ' + self.mountpoint;
            log.info({cmd: cmd}, 'ZfsClient.restoreDataset: exec');
            exec(cmd, cb);
        },
        function _chmodMountpoint(_, cb) {
            var cmd = 'chmod 755 ' + self.mountpoint;
            log.debug({
                cmd: cmd
            }, 'ZfsClient.restoreDataset: chmod mountpoint to 755');
            exec(cmd, cb);
        },
        function _zfsMount(_, cb) {
            var cmd = 'zfs mount ' + dataset;
            log.info({cmd: cmd}, 'ZfsClient.restoreDataset: exec');
            exec(cmd, cb);
        }
    ];

    vasync.pipeline({funcs: tasks, arg: {}}, function (err) {
        if (err) {
            log.info({
                err: err,
                dataset: dataset,
                snapshot: snapshot
            }, 'unable to restore dataset');
            return callback(err);
        } else {
            log.info({
                dataset: dataset,
                snapshot: snapshot
            }, 'restored dataset');
            return callback();
        }
    });
};

/**
 * Asserts whether the zfs dataset is ready to used.
 * This checks:
 * 1) whether the dataset exists.
 * 2) whether the mountpoint is correct.
 * 3) whether the dataset is mounted.
 * 4) if the dataset is not mounted, mount the dataset.
 */
ZfsClient.prototype.assertDataset = function assertDataset(callback) {
    var self = this;
    var log = self.log;
    log.debug({
        dataset: self.dataset
    }, 'entering ZfsClient.assertDataset');

    var tasks = [
        function _checkDatasetExists(_, cb) {
            _.exists = true;
            var cmd = 'zfs list ' + self.dataset;
            log.debug({
                cmd: cmd
            }, 'ZfsClient.assertDataset: checking whether dataset exists');

            exec(cmd, function (err) {
                if (err) {
                    _.exists = false;
                    log.info({err: err, dataset: self.dataset},
                        'dataset does not exist');
                }
                // doesn't matter if the dataset doesn't exist, we will create
                // it
                return cb();
            });
        },
        // fix for MANATEE-95, if the box restarts during a restore operation,
        // then the backup dataset never gets renamed back to the manatee
        // dataset.
        function _createDataset(_, cb) {
            if (_.exists) {
                return cb();
            }
            var cmd = 'zfs create -o canmount=noauto ' + self.dataset;
            log.debug({
                cmd: cmd
            }, 'ZfsClient.assertDataset: dataset DNE, creating dataset');
            exec(cmd, cb);

            return (undefined);
        },
        // dataset should exist here, unmount it so we can perform setup, such
        // as setting the mountpoint.
        function _unmountDataset(_, cb) {
            var cmd = 'zfs unmount ' + self.dataset;
            log.debug({
                cmd: cmd
            }, 'ZfsClient.assertDataset: unmounting dataset for setup');
            exec(cmd, function (err) {
                log.info({err: err},
                    'ZfsClient.assertDataset: unmount finished');
                // the unmount will fail if the dataset is already unmounted so
                // ignore it.
                return cb();
            });
        },
        function _setMountpoint(_, cb) {
            var cmd = 'zfs set mountpoint=' + self.mountpoint + ' ' +
                self.dataset;
            log.debug({
                cmd: cmd
            }, 'ZfsClient.assertDataset: setting mountpoint');
            exec(cmd, cb);
        },
        function _chownMountpoint(_, cb) {
            var cmd = 'sudo chown -R ' + self.dbUser + ' ' + self.mountpoint;
            log.debug({
                cmd: cmd
            }, 'ZfsClient.assertDataset: chown mountpoint to dbuser');
            exec(cmd, cb);
        },
        function _chmodMountpoint(_, cb) {
            var cmd = 'chmod 755 ' + self.mountpoint;
            log.debug({
                cmd: cmd
            }, 'ZfsClient.assertDataset: chmod mountpoint to 755');
            exec(cmd, cb);
        },
        // MANATEE-94 -- if the dataset isn't mounted, and for some reason
        // there are already files under the mountpath, we need to remove them
        // otherwise the mount dataset operation will fail.
        function rmrMountpoint(_, cb) {
            var cmd = 'rm -rf ' + self.mountpoint + '/*';
            log.debug({
                cmd: cmd
            }, 'ZfsClient.assertDataset: removing files under mountpoint');
            exec(cmd, cb);
        },
        function _mountDataset(_, cb) {
            var cmd = 'zfs mount ' + self.dataset;
            log.debug({
                cmd: cmd
            }, 'ZfsClient.assertDataset: mounting dataset');
            exec(cmd, cb);
        }
    ];

    vasync.pipeline({funcs: tasks, arg: {}}, function (err) {
        log.info({
            err: err
        }, 'ZfsClient.assertDataset: exiting');

        return callback(err);
    });
};

/**
 * Backup the current dataset, and promote associated datasets to the backup
 * dataset -- leaving the current dataset free to receive another snapshot.
 * @param callback Function f(err, backupSnapshot)
 */
function backupDataset(self, dataset, backup, mountpoint, callback) {
    var log = self.log;
    log.debug({
        dataset: dataset,
        backupDataset: backup,
        mountpoint: mountpoint
    }, 'entering ZfsClient.backupDataset');

    var snapshotId = Date.now();

    var tasks = [
        // first take a snapshot of the current dataset
        function _takeSnapshot(_, cb) {
            _.srcSnapshot = dataset + '@' + snapshotId;
            var cmd = 'zfs snapshot ' + _.srcSnapshot;
            log.info({cmd: cmd}, 'ZfsClient.backupDataset: exec');
            exec(cmd, cb);
        },
        function _cloneDataset(_, cb) {
            // TODO: we want to set canmount=noauto such that we don't auto
            // mount the snapshot. This doesn't currently work , blocked on
            // OS-1870
            var cmd = 'zfs clone -o canmount=off ' +
                _.srcSnapshot + ' ' + backup;
            log.info({cmd: cmd}, 'ZfsClient.backupDataset: exec');
            exec(cmd, cb);
        },
        function _setCanmountOn(_, cb) {
            var cmd = 'zfs set canmount=noauto ' + backup;
            log.info({cmd: cmd}, 'ZfsClient.backupDataset: exec');
            exec(cmd, cb);
        },
        function _setMountpoint(_, cb) {
            var cmd = 'zfs set mountpoint=' + mountpoint +
                ' ' + backup;
            log.info({cmd: cmd}, 'ZfsClient.backupDataset: exec');
            exec(cmd, cb);
        },
        function _promoteDataset(_, cb) {
            var cmd = 'zfs promote ' + backup;
            log.info({cmd: cmd}, 'ZfsClient.backupDataset: exec');
            exec(cmd, cb);
        }
    ];

    vasync.pipeline({funcs: tasks, arg: {}}, function (err) {
        if (err) {
            log.error({
                err: err,
                dataset: dataset,
                backupDataset: backup,
                mountpoint: mountpoint
            }, 'ZfsClient.backupDataset: error');
            return callback(err);
        } else {
            var backupSnapshot = backup+ '@' + snapshotId;
            log.info({
                dataset: dataset,
                backupDataset: backup,
                backupSnapshot: backupSnapshot,
                mountpoint: mountpoint
            }, 'ZfsClient.backupDataset: complete');
            return callback(null, backupSnapshot);
        }
    });
}

/**
 * Posts a restore request to the primary peer in the shard.
 * @param callback Function f(err, jobpath)
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
    self.client.post('/backup', request, function (err, req, res, obj) {
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
    restoreIntervalId = setInterval(function () {
        log.debug({
            serverUrl: serverUrl,
            jobPath: jobPath,
            pollInterval: pollInterval,
            restoreIntervalId: restoreIntervalId
        }, 'getting restore job status');

        self.client.get(jobPath, function (err, req, res, obj) {
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
}

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
            var cmd = 'zfs destroy -r ' + dataset;
            log.info({cmd: cmd}, 'ZfsClient.receive: exec');
            exec(cmd, cb);
        },
        /*
         * MANATEE-94 destroyCurrentDataset() should already have removed
         * everything under the mountpoint, however, to be safe, we rmr
         * everything under $mountpoint/
         */
        function rmrMountpoint(_, cb) {
            var cmd = 'rm -rf ' + self.mountpoint + '/*';
            log.info({cmd: cmd}, 'ZfsClient.receive: exec');
            exec(cmd, cb);
        },
        // start the zfs receive first
        function _startZfsRecv(_, cb) {
            var cmd = self.zfs_recv + ' 0 ' + self.zfsPort +
                ' -u ' + self.dataset;
            log.info({cmd: cmd}, 'ZfsClient.receive: exec');
            exec(cmd, function (err) {
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
            postRestoreRequest(self, function (err, jobPath) {
                _.jobPath = jobPath;
                return cb(err);
            });
        },
        // make sure we're always making forward progress -- because zfs recv
        // can potentially sit and wait forever.
        function _pollRestoreCompletion(_, cb) {
            pollRestoreCompletion(self, self.serverUrl,
                self.pollInterval, restoreIntervalId,
            _.jobPath, cb);
        }
    ];

    vasync.pipeline({funcs: tasks, arg: {}}, function (err) {
        if (err) {
            log.info({
                err: err,
                dataset: dataset,
                serverUrl: serverUrl,
                pollInterval: pollInterval,
                restoreIntervalId: restoreIntervalId
            }, 'unable to receive snapshot');
            // clear the interval incase one of the other funcs failed
            clearInterval(restoreIntervalId);
            return callback(err);
        } else {
            log.info({
                dataset: dataset,
                serverUrl: serverUrl,
                pollInterval: pollInterval,
                restoreIntervalId: restoreIntervalId
            }, 'enqueued receive request');
            return (undefined);
        }
    });
}
