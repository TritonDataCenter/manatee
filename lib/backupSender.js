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
var util = require('util');
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
 * @param {string} zfsSendPath Path to the zfs_send binary.
 * @param {string} dataset ZFS dataset to send to clients.
 * @param {string} snapshotDir ZFS directory of snapshots, i.e. the .zfs dir.
 * @param {BackupQueue} queue BackupQueue used to hold client requests.
 */
function BackupSender(options) {
    assert.object(options, 'options');
    assert.object(options.log, 'options.log');
    assert.string(options.zfsSendPath, 'options.zfsSendPath');
    assert.string(options.dataset, 'options.dataset');
    assert.string(options.snapshotDir, 'options.snapshotDir');
    assert.object(options.queue, 'options.queue');

    EventEmitter.call(this);

    /** @type {Bunyan} The bunyan log object */
    this._log = options.log.child({component: 'BackupSender'}, true);
    /** @type {string} Path to the zfs_send binary */
    this._zfs_send = options.zfsSendPath;
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

    self._getLatestSnapshot(function (err, snapshot) {
        if (err) {
            return callback(err);
        }
        log.info('sending latest snapshot %s to ', snapshot, backupJob);

        log.info('running cmd: %s %s %s %s %s',
            self._zfs_send,
            backupJob.host,
            backupJob.port,
            '-P',
            snapshot);
        var zfsSend = spawn(self._zfs_send,
            [backupJob.host, backupJob.port, '-P', snapshot]);

        zfsSend.stdout.on('data', function (data) {
            log.debug('zfsSend stdout: ', data.toString());
        });

        var msg = '';
        zfsSend.stderr.on('data', function (data) {
            var dataStr = data.toString();
            log.info('zfsSend stderr: ', dataStr);
            if (msg) {
                msg += dataStr;
            } else {
                msg = dataStr;
            }
            msg += data;
        });

        zfsSend.on('exit', function (code) {
            if (code !== 0) {
                var err2 = new verror.VError(msg, code);
                backupJob.done = 'failed';
                log.error({
                    err: err2,
                    backupJob: backupJob
                }, 'unable to complete zfs_send');
                return callback(err2);
            }

            backupJob.done = true;
            log.info({
                backupJob: backupJob
            }, 'completed backup job');
            return callback();
        });
    });
};

BackupSender.prototype._getLatestSnapshot = function (callback) {
    var self = this;
    var log = self._log;
    log.debug({
        snapshotDir: self._snapshotDir
    }, 'getting snapshots from dir');
    var snapshots = shelljs.ls(self._snapshotDir);
    log.debug({
        snapshots: snapshots
    }, 'got snapshots');

    // fix for MANATEE-90, send an error if there are no snapshots
    if (snapshots.length === 0) {
        log.error('no snapshots found');

        return callback(new verror.VError('no snapshots found'));
    }
    snapshots.sort(function (a, b) {
        return a - b;
    });

    var latestSnapshot = snapshots[snapshots.length - 1];
    latestSnapshot = self._dataset + '@' + latestSnapshot;
    log.debug('latest snapshot is %s', latestSnapshot);
    return callback(null, latestSnapshot);
};

/** #@- */
