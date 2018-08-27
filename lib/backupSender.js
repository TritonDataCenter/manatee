/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2018, Joyent, Inc.
 */

/**
 * @overview Postgres backup sender.
 *
 *                   _.---.._
 *      _        _.-' \  \    ''-.
 *    .'  '-,_.-'   /  /  /       '''.
 *   (       _                     o  :
 *    '._ .-'  '-._         \  \-  ---]
 *                  '-.___.-')  )..-'
 *                           (_/
 */
var exec = require('child_process').exec;
var EventEmitter = require('events').EventEmitter;
var spawn = require('child_process').spawn;
var net = require('net');
var util = require('util');

var assert = require('assert-plus');
var once = require('once');
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
 * @param {BackupQueue} queue BackupQueue used to hold client requests.
 *
 * @throws {Error} If the options object is malformed.
 */
function BackupSender(options) {
    assert.object(options, 'options');
    assert.object(options.log, 'options.log');

    assert.string(options.dataset, 'options.dataset');
    assert.object(options.queue, 'options.queue');
    assert.string(options.zfsPath, 'options.zfsPath');

    EventEmitter.call(this);

    /** @type {Bunyan} The bunyan log object */
    this._log = options.log.child({component: 'BackupSender'}, true);
    /** @type {string} Path to the zfs_send binary */
    this._zfsPath = options.zfsPath;
    /** @type {string} ZFS dataset used to send to clients */
    this._dataset = options.dataset;
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

module.exports = {
    start: function (cfg) {
        return new BackupSender(cfg);
    }
};
util.inherits(BackupSender, EventEmitter);

/**
 * @constant
 * @type {regex}
 * @default
 * Matches the output of zfs send -v. Sometimes the output is both lines,
 * sometimes it is only one. Hence the additon of .*\n* at the end of the
 * regex.
 * e.g.
 * full    dataset   4261260088
 * size    4261260088
 */
var ZFS_PROGRESS_HEADER = /^full\s+\S+\s+(\d+)\n.*\n*$/;

/**
 * @constant
 * @type {regex}
 * @default
 * Matches the output of zfs send -v
 * e.g.
 *
 * 20:14:41        103507768       dataset
 */
var ZFS_PROGRESS_REGEX = /^\d\d\:\d\d\:\d\d\t(\d+)\t\S+\n$/;

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
            var zfsSend;
            socket.on('connect', function () {
                log.info('BackupSender._send: spawning: %s %s %s',
                         self._zfsPath, 'send', _.snapshot);
                zfsSend = spawn(self._zfsPath, ['send', '-v', '-P',
                                                     _.snapshot]);
                zfsSend.stdout.pipe(socket);

                var msg = '';
                /*
                 * The output of zfs send -v results in this:
                 * full    <dataset>   4261506216
                 * size    4261506216
                 * 18:44:00        86419656        <dataset>
                 * 18:44:01        168928808       <dataset>
                 * 18:44:02        270109368       <dataset>
                 * 18:44:03        357610800       <dataset>
                 * 18:44:04        449201120       <dataset>
                 * 18:44:05        549578496       <dataset>
                 * 18:44:06        632414272       <dataset>
                 * 18:44:07        727119168       <dataset>
                 */
                backupJob.size = null;
                backupJob.done = 0;
                zfsSend.stderr.on('data', function (data) {
                    var dataStr = data.toString();
                    log.info('zfs send stderr: ', dataStr);
                    // parse zfs verbose output
                    if (ZFS_PROGRESS_HEADER.test(dataStr)) {
                        backupJob.size = ZFS_PROGRESS_HEADER.exec(dataStr)[1];
                        log.info({size: backupJob.size},
                                 'zfs send updating snapshot size');
                    } else if (ZFS_PROGRESS_REGEX.test(dataStr)) {
                        backupJob.completed =
                            ZFS_PROGRESS_REGEX.exec(dataStr)[1];
                        log.info({completed: backupJob.completed},
                                 'zfs send updating completed bytes');
                    }
                    msg = dataStr;
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
                zfsSend.kill('SIGTERM');
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
    log.debug({dataset: self._dataset}, 'getting snapshots.');

    /*
     * get the snapshot and sort descending by name. This guarantees the
     * earliest snapshot is on top.
     */
    var cmd = 'zfs list -t snapshot -H -d 1 -S name -o name ' + self._dataset;
    exec(cmd, function (err, stdout, stderr) {
        log.debug({snapshots: stdout}, 'got snapshots');
        if (err) {
            return callback(err);
        }
        var snapshots = stdout.split('\n');
        log.debug({snapshots: snapshots}, 'got snapshots');
        /*
         * MANATEE-214
         * A snapshot name is just time since epoch in ms. So it's a 13 digit
         * number like 1405378955344. We only want snapshots that look like
         * this to avoid using other snapshots as they may have been created by
         * an operator.
         */
        var regex = /^\d{13}$/;
        var snapshotIndex = -1;
        for (var i = 0; i < snapshots.length; i++) {
            var snapshot = snapshots[i].split('@')[1];
            log.debug({snapshot: snapshot}, 'testing snapshot against regex');
            if (regex.test(snapshot) === true) {
                snapshotIndex = i;
                break;
            }
        }
        // fix for MANATEE-90, send an error if there are no snapshots
        if (snapshots.length === 0 || snapshotIndex === -1) {
            log.error('no snapshots found');

            return callback(new verror.VError('no snapshots found'));
        }

        log.debug('latest snapshot is %s', snapshots[snapshotIndex]);
        return callback(null, snapshots[snapshotIndex]);
    });
};
