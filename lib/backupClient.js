var assert = require('assert-plus');
var EventEmitter = require('events').EventEmitter;
var restify = require('restify');
var shelljs = require('shelljs');
var spawn = require('child_process').spawn;
var util = require('util');
var vasync = require('vasync');
var verror = require('verror');

/**
* Client used to grab a zfs snapshot from an existing postgres peer and
* restore the pg data dir on another host.
*/
function BackupClient(options) {
        assert.object(options, 'options');
        assert.object(options.log, 'options.log');
        assert.string(options.dataset, 'options.dataset');
        assert.string(options.snapshotDir, 'options.snapshotDir');
        assert.string(options.serverUrl, 'options.serverUrl');
        assert.string(options.zfsHost, 'options.zfsHost');
        assert.number(options.zfsPort, 'options.zfsPort');
        assert.number(options.pollInterval, 'options.pollInterval');
        assert.string(options.zfsRecvPath, 'options.zfsRecvPath');

        EventEmitter.call(this);

        var self = this;

        this.log = options.log;

        /**
        * The zfs dataset of this peer.
        */
        this.dataset = options.dataset;

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

        this.pollInterval = options.pollInterval;

        this.intervalId = null;

        this.client = restify.createJsonClient({
                url: self.serverUrl,
                version: '*'
        });

        this.log.info('initializing BackupClient with options', options);
}

module.exports = BackupClient;
util.inherits(BackupClient, EventEmitter);

/**
* Restore the pg data dir using zfs recv
*/
BackupClient.prototype.restore = function restore(callback) {
        var self = this;
        var log = self.log;

        log.info({
                dataset: self.dataset,
                snapshotDir: self.snapshotDir,
                serverUrl: self.serverUrl,
                pollInterval: self.pollInterval
        }, 'BackupClient.restore: entering');

        self.on('zfs-err', function(err) {
                log.info({
                        err: err,
                        dataset: self.dataset,
                        snapshotDir: self.snapshotDir,
                        serverUrl: self.serverUrl,
                        pollInterval: self.pollInterval
                }, 'zfs receive failed');
                return callback(err);
        });

        self.on('zfs-done', function() {
                clearInterval(self.intervalId);
                log.info({
                        dataset: self.dataset,
                        snapshotDir: self.snapshotDir,
                        serverUrl: self.serverUrl,
                        pollInterval: self.pollInterval
                }, 'sucessfully received backup image', self.intervalId);
                self.intervalId = null;
                return callback();
        });

        var tasks = [
                function _deleteSnapshots(_, cb) {
                        deleteSnapshots(self, cb);
                },
                function _startZfsRecv(_, cb) {
                        startZfsRecv(self, cb);
                },
                function _postRestoreRequest(_, cb) {
                        postRestoreRequest(self, function(err, jobPath) {
                                _.jobPath = jobPath;
                                return cb(err);
                        });
                },
                function _pollRestoreCompletion(_, cb) {
                        pollRestoreCompletion(self, _.jobPath, cb);
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
                        }, 'unable to restore database');
                        return callback(err);
                } else {
                        log.info({
                                dataset: self.dataset,
                                snapshotDir: self.snapshotDir,
                                serverUrl: self.serverUrl,
                                pollInterval: self.pollInterval
                        }, 'enqueued restore request');
                        return true;
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
function pollRestoreCompletion(self, jobPath, callback) {
        var log = self.log;
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
                                return self.emit('zfs-err', err);
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
                                return self.emit('zfs-err', err2);
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

function startZfsRecv(self, callback) {
        var log = self.log;

        log.info('receiving latest snapshot to ', self.dataset);
        log.info('running cmd %s %s %s -F  %s',
                 self.zfs_recv,
                 0,
                 self.zfsPort,
                 self.dataset);
        var zfsRecv = spawn('pfexec', [
                             self.zfs_recv,
                             0,
                             self.zfsPort,
                             '-F',
                             self.dataset
                            ]);

        var msg;

        zfsRecv.stdout.on('data', function(data) {
                log.debug('zfsRecv stdout: ', data.toString());
        });

        zfsRecv.stderr.on('data', function(data) {
                var dataStr = data.toString();
                log.error('zfsRecv stderr: ', dataStr);
                if (msg) {
                        msg += dataStr;
                } else {
                        msg = dataStr;
                }
                msg += data;
        });

        zfsRecv.on('exit', function(code) {
                if (code !== 0) {
                        var err = new verror.VError(msg, code);
                        log.info({
                                err: err,
                                dataset: self.dataset,
                                snapshotDir: self.snapshotDir,
                                serverUrl: self.serverUrl
                        }, 'unable to compete zfs_recv');
                        self.emit('zfs-err', err);
                } else {
                        self.emit('zfs-done');
                }
        });

        // Callback to indicate zfs_recv has started
        return callback();
}

/**
* Deletes any snapshots on the system before attempting a restore
*/
function deleteSnapshots(self, callback) {
        var log = self.log;
        log.debug({
                dataset: self.dataset,
                snapshotDir: self.snapshotDir,
                serverUrl: self.serverUrl
        }, 'getting snapshots');

        var snapshots = null;
        if (shelljs.test('-d', self.snapshotDir)) {
                snapshots = shelljs.ls(self.snapshotDir);
        } else {
                var err = new verror.VError('snapshot dir ' +
                                            self.snapshotDir +
                                            ' not found');
                log.info({
                        err: err,
                        dataset: self.dataset,
                        snapshotDir: self.snapshotDir,
                        serverUrl: self.serverUrl
                }, 'snapshot dir not found');
                return callback(err);
        }
        log.debug({
                snapshots: snapshots,
                dataset: self.dataset,
                snapshotDir: self.snapshotDir,
                serverUrl: self.serverUrl
        }, 'got snapshots');

        var deleted = 0;
        if (snapshots.length === 0) {
                log.info('no snapshots to delete, returning');
                return callback();
        }
        snapshots.forEach(function(snapshot) {
                snapshot = self.dataset + '@' + snapshot;
                log.info({
                        dataset: self.dataset,
                        snapshotDir: self.snapshotDir,
                        serverUrl: self.serverUrl,
                        snapshot: snapshot
                }, 'deleting snapshot');
                var delSnapshot = spawn('pfexec', ['zfs', 'destroy', snapshot]);

                delSnapshot.stdout.on('data', function(data) {
                        log.debug('delSnapshot stdout: ', data.toString());
                });

                var msg;
                delSnapshot.stderr.on('data', function(data) {
                        var dataStr = data.toString();
                        log.debug('delSnapshot stderr: ', dataStr);
                        if (msg) {
                                msg += dataStr;
                        } else {
                                msg = dataStr;
                        }
                        msg += data;
                });

                delSnapshot.on('exit', function(code) {
                        if (code !== 0) {
                                var err2 = new verror.VError(msg, code);
                                log.info({
                                        err: err2,
                                        dataset: self.dataset,
                                        snapshotDir: self.snapshotDir,
                                        serverUrl: self.serverUrl,
                                        pollInterval: self.pollInterval
                                }, 'unable to delete snapshots');
                                return callback(err2);
                        } else {
                                deleted++;
                                log.info({
                                        dataset: self.dataset,
                                        snapshotDir: self.snapshotDir,
                                        serverUrl: self.serverUrl,
                                        snapshot: snapshot
                                }, 'deleted snapshot');
                                if (deleted === snapshots.length) {
                                        return callback();
                                } else {
                                        return true;
                                }
                        }
                });
        });

        return true;
}
