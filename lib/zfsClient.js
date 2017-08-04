/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2014, Joyent, Inc.
 */

/**
 * @overview The ZFS client.
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
var exec = require('child_process').exec;
var path = require('path');
var once = require('once');
var net = require('net');
var restify = require('restify');
var spawn = require('child_process').spawn;
var util = require('util');
var uuid = require('node-uuid');
var vasync = require('vasync');
var verror = require('verror');

/**
 * ZFS client. Interactions with the underlying ZFS dataset -- which is used by
 * PostgreSQL -- are managed through this client.
 *
 * This class is responsible for restoring the dataset from a remote instance
 * of manatee.
 *
 * @constructor
 *
 * @param {object} options Options object.
 * @param {Bunyan} options.log Bunyan logger.
 * @param {string} options.dataset ZFS datset.
 * @param {string} options.dbUser PostgreSQL DB user.
 * @param {string} options.mountpoint Mountpoint for the ZFS dataset.
 * @param {string} options.zfsHost IP address used for ZFS recv.
 * @param {number} options.zfsPort Port used for ZFS recv.
 * @param {number} options.pollInterval How often to poll the backup server in
 * ms.
 * @param {string} options.zfsPath Path of the ZFS binary
 *
 * @throws {Error} If the options object is malformed.
 */
function ZfsClient(options) {
    assert.object(options, 'options');
    assert.object(options.log, 'options.log');

    assert.string(options.dataset, 'options.dataset');
    assert.string(options.dbUser, 'options.dbUser');
    assert.string(options.mountpoint, 'options.mountpoint');
    assert.number(options.pollInterval, 'options.pollInterval');
    assert.string(options.zfsHost, 'options.zfsHost');
    assert.string(options.zfsPath, 'options.zfsPath');
    assert.number(options.zfsPort, 'options.zfsPort');

    var self = this;

    /** @type {Bunyan} The bunyan log object */
    this._log = options.log.child({component: 'ZfsClient'}, true);
    self._log.info('initializing ZfsClient with options', options);

    /** @type {string} My ZFS dataset */
    this._dataset = options.dataset;
    /** @type {string} My parent ZFS dataset */
    this._parentDataset = path.dirname(self._dataset);

    /** @type {string} ZFS mountpoint of the datset */
    this._mountpoint = options.mountpoint;

    /** @type {string} PostgreSQL user */
    this._dbUser = options.dbUser;

    /** @type {number} The backup server poll interval */
    this._pollInterval = options.pollInterval;
    /** @type {Object} The current/last restore object */
    this._restoreObject = null;

    /** @type {string} IP addr used for zfs recv */
    this._zfsHost = options.zfsHost;
    /** @type {number} Port used for zfs recv */
    this._zfsPort = options.zfsPort;
    /** @type {string} Path to the zfs binary */
    this._zfsPath = options.zfsPath;

    self._log.info({
        dataset: self._dataset,
        parentDataset: self._parentDataset,
        mountpoint: self._mountpoint,
        zfsHost: self._zfsHost,
        zfsPort: self._zfsPort,
        zfsPath: self._zfsPath,
        pollInterval: self._pollInterval
    }, 'initalized ZfsClient');
}

module.exports = ZfsClient;

/**
 * @callback ZfsClient-restoreCb
 * @param {Error} error
 * @param {string} backupSnapshot The previous dataset's snapshot.
 */

/**
 * Restores the current dataset from the remote host.
 * @param {string} serverUrl server's url. e.g. http://10.0.0.0:1234
 * @param {ZfsClient-restoreCb} callback
 */
ZfsClient.prototype.restore = function restore(serverUrl, callback) {
    var self = this;
    var log = self._log;

    var backupSnapshot;

    log.info({
        dataset: self._dataset,
        serverUrl: serverUrl,
        pollInterval: self._pollInterval
    }, 'ZFSClient.restore: entering');

    vasync.pipeline({funcs: [
        function _createRestClient(_, cb) {
            self._client = restify.createJsonClient({
                url: serverUrl,
                version: '*'
            });
            return cb();
        },
        function _backupCurrentDataset(_, cb) {
            self._backupDataset(self._dataset,
                                self._parentDataset + '/' + uuid.v4(),
                                self._mountpoint,
                                function (err, snap)
            {
                backupSnapshot = snap;
                return cb(err);
            });
        },
        function _receive(_, cb) {
            self._receive(self._dataset,
                          serverUrl,
                          self._pollInterval,
                          cb);
        },
        function _setMountPoint(_, cb) {
            var cmd = 'zfs set mountpoint=' + self._mountpoint + ' ' +
                      self._dataset;
            log.info({ cmd: cmd }, 'ZfsClient.restore: exec');
            exec(cmd, cb);
        },
        function _setNoAutoMount(_, cb) {
            var cmd = 'zfs set canmount=noauto ' + self._dataset;
            log.info({ cmd: cmd }, 'ZfsClient.restore: exec');
            exec(cmd, cb);
        },
        function _mount(_, cb) {
            var cmd = 'zfs mount ' + self._dataset;
            log.info({ cmd: cmd }, 'ZfsClient.restore: exec');
            exec(cmd, cb);
        },
        // MANATEE-119 - snapshot after restoring the dataset from a primary
        function _snapshot(_, cb) {
            var cmd = 'zfs snapshot ' + self._dataset + '@' +
                new Date().getTime();
            log.info({ cmd: cmd }, 'ZfsClient.restore: exec');
            exec(cmd, cb);
        }
    ], arg: {}}, function (err) {
        if (err) {
            log.info({
                err: err,
                dataset: self._dataset,
                backupSnapshot: backupSnapshot,
                serverUrl: serverUrl,
                pollInterval: self._pollInterval
            }, 'unable to restore snapshot');
            return callback(err, backupSnapshot);
        } else {
            log.info({
                dataset: self._dataset,
                backupSnapshot: backupSnapshot,
                serverUrl: serverUrl,
                pollInterval: self._pollInterval
            }, 'ZFSClient.restore: success');
            return callback(null, backupSnapshot);
        }
    });
};

/**
 * @callback ZfsClient-cb
 * @param {Error} err
 */

/**
 * Restores a dataset from a snapshot.
 * @param {string} snapshot The snapshot to restore from.
 * @param {ZfsClient-cb} callback
 */
ZfsClient.prototype.restoreDataset = function restoreDataset(snapshot, callback)
{
    var self = this;
    var log = self._log;
    var dataset = self._dataset;

    log.info({
        dataset: dataset,
        snapshot: snapshot
    }, 'ZfsClient.restoreDataset: entering');

    vasync.pipeline({funcs: [
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
            // mount the snapshot. This doesn't currently work, blocked on
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
                self._mountpoint + ' ' + dataset;
            log.info({cmd: cmd}, 'ZfsClient.restoreDataset: exec');
            exec(cmd, cb);
        },
        function _mkdirpMountpoint(_, cb) {
            var cmd = 'mkdir -p ' + self._mountpoint;
            log.info({cmd: cmd}, 'ZfsClient.restoreDataset: exec');
            exec(cmd, cb);
        },
        function _chownMountPoint(_, cb) {
            var cmd = 'chown -R ' + self._dbUser + ' ' + self._mountpoint;
            log.info({cmd: cmd}, 'ZfsClient.restoreDataset: exec');
            exec(cmd, cb);
        },
        function _chmodMountpoint(_, cb) {
            var cmd = 'chmod 755 ' + self._mountpoint;
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
    ], arg: {}}, function (err) {
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
 * -  whether the dataset exists.
 * -  whether the mountpoint is correct.
 * -  whether the dataset is mounted.
 * -  if the dataset is not mounted, mount the dataset.
 * @param {ZfsClient-cb} callback
 */
ZfsClient.prototype.assertDataset = function assertDataset(callback) {
    var self = this;
    var log = self._log;
    log.debug({
        dataset: self._dataset
    }, 'entering ZfsClient.assertDataset');

    vasync.pipeline({funcs: [
        function _checkDatasetExists(_, cb) {
            _.exists = true;
            var cmd = 'zfs list ' + self._dataset;
            log.debug({
                cmd: cmd
            }, 'ZfsClient.assertDataset: checking whether dataset exists');

            exec(cmd, function (err) {
                if (err) {
                    _.exists = false;
                    log.info({err: err, dataset: self._dataset},
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
            var cmd = 'zfs create -o canmount=noauto ' + self._dataset;
            log.debug({
                cmd: cmd
            }, 'ZfsClient.assertDataset: dataset DNE, creating dataset');
            exec(cmd, cb);

            return (undefined);
        },
        // dataset should exist here, unmount it so we can perform setup, such
        // as setting the mountpoint.
        function _unmountDataset(_, cb) {
            var cmd = 'zfs unmount ' + self._dataset;
            log.debug({
                cmd: cmd
            }, 'ZfsClient.assertDataset: unmounting dataset for setup');
            exec(cmd, function (err, stdout, stderr) {
                log.info({
                    err: err,
                    stdout: stdout,
                    stderr: stderr
                }, 'ZfsClient.assertDataset: unmount finished');

                // the unmount will fail if the dataset is already unmounted so
                // ignore it.
                return cb();
            });
        },
        function _setMountpoint(_, cb) {
            var cmd = 'zfs set mountpoint=' + self._mountpoint + ' ' +
                self._dataset;
            log.debug({
                cmd: cmd
            }, 'ZfsClient.assertDataset: setting mountpoint');
            exec(cmd, cb);
        },
        function _chownMountpoint(_, cb) {
            var cmd = 'chown -R ' + self._dbUser + ' ' + self._mountpoint;
            log.debug({
                cmd: cmd
            }, 'ZfsClient.assertDataset: chown mountpoint to dbuser');
            exec(cmd, cb);
        },
        function _chmodMountpoint(_, cb) {
            var cmd = 'chmod 755 ' + self._mountpoint;
            log.debug({
                cmd: cmd
            }, 'ZfsClient.assertDataset: chmod mountpoint to 755');
            exec(cmd, cb);
        },
        // MANATEE-94 -- if the dataset isn't mounted, and for some reason
        // there are already files under the mountpath, we need to remove them
        // otherwise the mount dataset operation will fail.
        function rmrMountpoint(_, cb) {
            var cmd = 'rm -rf ' + self._mountpoint + '/*';
            log.debug({
                cmd: cmd
            }, 'ZfsClient.assertDataset: removing files under mountpoint');
            exec(cmd, cb);
        },
        function _mountDataset(_, cb) {
            var cmd = 'zfs mount ' + self._dataset;
            log.debug({
                cmd: cmd
            }, 'ZfsClient.assertDataset: mounting dataset');
            exec(cmd, cb);
        }
    ], arg: {}}, function (err) {
        log.info({
            err: err
        }, 'ZfsClient.assertDataset: exiting');

        return callback(err);
    });
};

/**
 * #@+
 * @private
 * @memberOf ZfsClient
 */

/**
 * @callback ZfsClient-backupDatasetCb
 * @param {Error} err
 * @param {string} backupSnapshot The snapshot of the backedup dataset.
 */

/**
 * Backup the current dataset such that another dataset can be replaced in its
 * place.
 *
 * @param {object} self
 * @param {string} dataset The dataset to backup.
 * @param {string} backup The dataset to back up to.
 * @param {string} mountpoint The mountpoint of the dataset.
 * @param {ZfsClient-backupdatasetCb} callback
 */
ZfsClient.prototype._backupDataset = function (dataset,
                                              backup,
                                              mountpoint,
                                              callback)
{
    var self = this;
    var log = self._log;
    log.debug({
        dataset: dataset,
        backupDataset: backup,
        mountpoint: mountpoint
    }, 'entering ZfsClient.backupDataset');

    var snapshotId = Date.now();

    vasync.pipeline({funcs: [
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
    ], arg: {}}, function (err) {
        if (err) {
            err = new verror.VError(err);
        } else {
            var backupSnapshot = backup+ '@' + snapshotId;
            log.info({
                err: err,
                dataset: dataset,
                backupDataset: backup,
                backupSnapshot: backupSnapshot,
                mountpoint: mountpoint
            }, 'ZfsClient.backupDataset: exiting');
            return callback(err, backupSnapshot);
        }
    });
};

/**
 * @callback ZfsClient-postRestoreRequestCb
 * @param {Error} err
 * @param {string} jobPath The restore job's REST path.
 */

/**
 * Posts a restore request to the primary peer in the shard.
 *
 * @param {object} self
 * @param {ZfsClient-postRestoreRequestCb} callback
 */
ZfsClient.prototype._postRestoreRequest = function (serverUrl, callback) {
    var self = this;
    var log = self._log;
    var request = {
        host: self._zfsHost,
        port: self._zfsPort,
        dataset: self._dataset
    };

    log.info({
        zfsHost: request.host,
        zfsPort: request.port,
        serverUrl: serverUrl,
        pollInterval: self._pollInterval
    }, 'ZfsClient.postRestoreRequest: entering');

    self._client.post('/backup', request, function (err, req, res, obj) {
        if (err) {
            err = new verror.VError(err, 'Posting restore request failed');
        }

        self._log.info({
            err: err,
            dataset: self._dataset,
            serverUrl: serverUrl,
            pollInterval: self._pollInterval,
            response: obj
        }, 'ZfsClient.postRestoreRequest: exiting');
        return callback(err, obj ? obj.jobPath : null);
    });
};

/**
 * @callback ZfsClient-pollRestoreCompletionCb
 * @param {Error} err
 * @param {Object} job The job object returned from the server.
 */

/**
 *  Polls the restore service for the status of the backup job.
 *
 *  @param {object} self
 *  @param {string} serverUrl The url of the restore server.
 *  @param {string} restoreintervalid The restore intervalId from setInterva().
 *  @param {string} jobPath The REST path of the backup job.
 *  @param {ZfsClient-pollrestorecompletioncb} callback
 */
ZfsClient.prototype._pollRestoreCompletion = function (serverUrl,
                                                      pollInterval,
                                                      restoreIntervalId,
                                                      jobPath,
                                                      callback)
{
    var self = this;
    var log = self._log;
    log.info({
        serverUrl: serverUrl,
        jobPath: jobPath,
        pollInterval: pollInterval
    }, 'zfsClient.pollRestoreCompletion: entering');

    callback = once(callback);
    restoreIntervalId = setInterval(function () {
        log.debug({
            serverUrl: serverUrl,
            jobPath: jobPath,
            pollInterval: pollInterval
        }, 'getting restore job status');

        self._client.get(jobPath, function (err, req, res, obj) {
            if (err) {
                log.info({
                    err: err,
                    serverUrl: serverUrl,
                    jobPath: jobPath,
                    pollInterval: pollInterval
                }, 'error getting restore job status');
                clearInterval(restoreIntervalId);
                return callback(err);
            }

            log.debug({
                backupJob: obj
            }, 'got restore job status');
            self._restoreObject = obj;

            if (obj.done === true) {
                log.info('restore job is done');
                clearInterval(restoreIntervalId);
                return callback();
            } else if (obj.done === 'failed') {
                var msg = 'restore job failed';
                var err2 = new verror.VError(msg, err);
                log.info({
                    err: err2,
                    serverUrl: serverUrl,
                    jobPath: jobPath,
                    pollInterval: pollInterval
                }, 'restore job failed');

                clearInterval(restoreIntervalId);
                return callback(err2);
            } else {
                log.info({
                    serverUrl: serverUrl,
                    jobPath: jobPath,
                    jobSize: obj.size,
                    jobCompleted: obj.completed,
                    pollInterval: pollInterval
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
 * @param {object} self
 * @param {string} dataset The dataset to receive the snapshot to.
 * @param {string} serverUrl
 * @param {number} pollInterval
 * @param {ZfsClient-cb} callback
 */
ZfsClient.prototype._receive = function (dataset,
                                        serverUrl,
                                        pollInterval,
                                        callback)
{
    var self = this;
    var log = self._log;
    callback = once(callback);

    var restoreIntervalId;
    var server;

    log.info({
        dataset: dataset,
        serverUrl: serverUrl,
        pollInterval: pollInterval
    }, 'ZfsClient.restore: entering');

    vasync.pipeline({funcs: [
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
            var cmd = 'rm -rf ' + self._mountpoint + '/*';
            log.info({cmd: cmd}, 'ZfsClient.receive: exec');
            exec(cmd, cb);
        },
        function _startZfsRecv(_, cb) {
            cb = once(cb);
            log.info('zfsClient._receive: starting zfs recv ' + self._dataset);
            /*
             * -u so we don't mount the dataset since the mountpoint hasn't
             *  been changed yet. Otherwise the recv will fail trying to create
             *  the mountpoint
             */
            _.zfsRecv = spawn(self._zfsPath, ['recv', '-v', '-u',
                                              self._dataset]);

            _.zfsRecv.stdout.on('data', function (data) {
                var dataStr = data.toString();
                log.info('zfs recv stdout: ', dataStr);
            });

            var msg = '';
            _.zfsRecv.stderr.on('data', function (data) {
                var dataStr = data.toString();
                log.info('zfs recv stderr: ', dataStr);
                msg += data;
            });

            _.zfsRecv.on('exit', function (code) {
                if (code !== 0) {
                    var err2 = new verror.VError('zfs recv: ' + msg + ' ' +
                                                 code);
                    log.error({err: err2},
                              'zfsClient._receive: zfs recv failed');
                    cb(err2);
                }

                log.info('zfsClient._receive: completed zfs recv');
            });
            return cb();
        },
        function createServer(_, cb) {
            cb = once(cb);
            server = net.createServer();
            server.on('connection', function (socket) {
                log.info('ZFSClient._receive: got socket, piping to zfs recv');
                socket.pipe(_.zfsRecv.stdin);
                return cb();
            });

            log.info({port: self._zfsPort, host: self._zfsHost},
                     'listening for zfs send on');
            server.listen(self._zfsPort, self._zfsHost, 1, function (err) {
                if (err) {
                    log.warn({err: err},
                             'ZfsClient._receive: could not start server');
                    err = new verror.VError(err);
                }
                return cb(err);
            });

            server.on('error', function (err) {
                log.warn({err: err}, 'ZfsClient._receive: got socket error');
                return cb(new verror.VError(err));
            });
        },
        function _postRestoreRequest(_, cb) {
            self._postRestoreRequest(serverUrl, function (err, jobPath) {
                _.jobPath = jobPath;
                return cb(err);
            });
        },
        /*
         * make sure we're always making forward progress -- because zfs recv
         * can potentially sit and wait forever if there is no response from
         * the server.
         */
        function _pollRestoreCompletion(_, cb) {
            self._pollRestoreCompletion(serverUrl, self._pollInterval,
                                        restoreIntervalId, _.jobPath, cb);
        }
    ], arg: {}}, function (err) {
        // always close the server when done.
        try {
            server.close();
        } catch (e) {}

        if (err) {
            log.info({
                err: err,
                dataset: dataset,
                serverUrl: serverUrl,
                pollInterval: pollInterval
            }, 'unable to receive snapshot');
            // clear the interval incase one of the other funcs failed
            clearInterval(restoreIntervalId);
            return callback(err);
        } else {
            log.info({
                dataset: dataset,
                serverUrl: serverUrl,
                pollInterval: pollInterval
            }, 'successfully received zfs dataset');
            return callback();
        }
    });
};

/** #@- */
