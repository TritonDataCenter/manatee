/**
 * @overview Postgres backup sender.
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
var EventEmitter = require('events').EventEmitter;
var shelljs = require('shelljs');
var spawn = require('child_process').spawn;
var net = require('net');
var once = require('once');
var util = require('util');
var vasync = require('vasync');
var verror = require('verror');

/**
 * Responsible for getting the latest snapshot and zfs sending it to a remote
 * host.
 *
 * @constructor
 * @augments EventEmitter
 *
 * @fires BackupSender#err If a backupjob has failed.
 * @fires BackupSender#done A backupjob has successfully completed.
 *
 * @param {object} options Options object.
 * @param {Bunyan} options.log Bunyan logger.
 * @param {string} zfsPath Path to the zfs_send binary.
 * @param {string} dataset ZFS dataset to send to clients.
 * @param {string} snapshotDir ZFS directory of snapshots, i.e. the .zfs dir.
 * @param {BackupQueue} queue BackupQueue used to hold client requests.
 *
 * @throws {Error} If the options object is malformed.
 */
function BackupSender(options) {
    assert.object(options, 'options');
    assert.object(options.log, 'options.log');
    assert.string(options.zfsPath, 'options.zfsPath');
    assert.string(options.dataset, 'options.dataset');
    assert.string(options.snapshotDir, 'options.snapshotDir');
    assert.object(options.queue, 'options.queue');

    EventEmitter.call(this);

    /** @type {Bunyan} The bunyan log object */
    this._log = options.log.child({component: 'BackupSender'}, true);
    /** @type {string} Path to the zfs_send binary */
    this._zfsPath = options.zfsPath;
    /** @type {string} ZFS dataset used to send to clients */
    this._dataset = options.dataset;
    /** @type {string} ZFS directory of snapshots */
    this._snapshotDir = options.snapshotDir;
    /** @type {BackupQueue} Backup queue used to hold client requests */
    this._queue = options.queue;

    var self = this;

    self._queue.on('push', function (backupJob) {
        self._send(backupJob, function (err) {
            if (err) {
                self._log.error({
                    backupJob: backupJob,
                    err: err
                }, 'unable to send backup');
                /**
                 * Err event, emitted when unable to send a backup to a client.
                 * We don't use the conventional 'error' event here because we
                 * don't want to blow up node if this error isn't caught.
                 *
                 * @event BackupSender#err
                 * @type {Error}
                 */
                self.emit('err', err);
                backupJob.err = err;
            } else {
                /**
                 * Done event, emitted when a backup job has finished.
                 *
                 * @event BackupSender#done
                 * @type {object} The completed backup job.
                 */
                self._log.info({
                    backupJob: backupJob
                }, 'successfully sent backup');
                self.emit('done', backupJob);
            }
        });
    });

    self._log.debug('Initializing BackupSender with options', options);
}

module.exports = BackupSender;
util.inherits(BackupSender, EventEmitter);

/**
 * #@+
 * @private
 * @memberOf BackupSender
 */

/**
 * @callback BackupSender-cb
 * @param {Error} error
 */

/**
 * Send the latest snapshot to a remote host.
 * @param {object} backupJob The host and port mapping of the remote host.
 * @param {BackupSender-cb} callback
 */
BackupSender.prototype._send = function (backupJob, callback) {
    var self = this;
    var log = self._log;
    callback = once(callback);

    var socket;
    vasync.pipeline({funcs: [
        function _getLatestSnapshot(_, _cb) {
            self._getLatestSnapshot(function (err, snapshot) {
                _.snapshot = snapshot;
                return _cb(err);
            });
        },
        function _createSocket(_, _cb) {
            _cb = once(_cb);

            log.info({port: backupJob.port, host: backupJob.host},
                     'BackupSender._send: creating socket for zfs send');
            socket = net.connect(backupJob.port, backupJob.host);

            socket.on('connect', function () {
                log.info('BackupSender._send: spawning: %s %s %s', self._zfs,
                         'send', _.snapshot);
                var zfsSend = spawn(self._zfsPath, ['send', '-P', _.snapshot]);
                zfsSend.stdout.pipe(socket);

                var msg = '';
                zfsSend.stderr.on('data', function (data) {
                    var dataStr = data.toString();
                    log.info('zfs send stderr: ', dataStr);
                    msg += data;
                });

                zfsSend.on('exit', function (code) {
                    if (code !== 0) {
                        var err2 = new verror.VError('zfs send: ' + msg + ' ' +
                                                     code);
                        backupJob.done = 'failed';
                        log.error({err: err2, backupJob: backupJob},
                                  'unable to complete zfs send');
                        return _cb(err2);
                    }

                    backupJob.done = true;
                    log.info({backupJob: backupJob}, 'completed backup job');
                    return _cb();
                });

            });

            socket.on('error', function (err) {
                log.error({err: err}, 'BackupSender._send: socket error.');
                backupJob.done = 'failed';
                return _cb(err);
            });

        }
    ], arg: {}}, function (err, results) {
        log.info({err: err, results: err ? results : null},
                 'BackupSender._send: exit');
        return callback(err);
    });
};

BackupSender.prototype._getLatestSnapshot = function (callback) {
    var self = this;
    var log = self._log;
    log.debug({snapshotDir: self._snapshotDir}, 'getting snapshots from dir');
    var snapshots = shelljs.ls(self._snapshotDir);
    log.debug({snapshots: snapshots}, 'got snapshots');

    // fix for MANATEE-90, send an error if there are no snapshots
    if (snapshots.length === 0) {
        log.error('no snapshots found');

        return callback(new verror.VError('no snapshots found'));
    }

    snapshots.sort(function (a, b) { return a - b; });

    var latestSnapshot = snapshots[snapshots.length - 1];
    latestSnapshot = self._dataset + '@' + latestSnapshot;
    log.debug('latest snapshot is %s', latestSnapshot);
    return callback(null, latestSnapshot);
};

/** #@- */
