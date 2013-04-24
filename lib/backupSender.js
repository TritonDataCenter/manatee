var assert = require('assert-plus');
var EventEmitter = require('events').EventEmitter;
var shelljs = require('shelljs');
var spawn = require('child_process').spawn;
var util = require('util');
var verror = require('verror');

/**
 * Responsible for getting the latest snapshot and zfs sending it to a remote
 * host.
 */
function BackupSender(options) {
    assert.object(options, 'options');
    assert.object(options.log, 'options.log');
    assert.string(options.zfsSendPath, 'options.zfsSendPath');
    assert.string(options.dataset, 'options.dataset');
    assert.string(options.snapshotDir, 'options.snapshotDir');
    assert.object(options.queue, 'options.queue');

    EventEmitter.call(this);

    this.log = options.log;
    this.zfs_send = options.zfsSendPath;
    this.dataset = options.dataset;
    this.snapshotDir = options.snapshotDir;
    this.queue = options.queue;

    var self = this;

    this.queue.on('push', function(backupJob) {
        send(self, backupJob, function(err) {
            if (err) {
                self.log.error({
                    backupJob: backupJob,
                    err: err
                }, 'unable to send backup');
                self.emit('err', err);
                backupJob.err = err;
            } else {
                self.log.info({
                    backupJob: backupJob
                }, 'successfully sent backup');
                self.emit('done', backupJob);
            }
        });
    });

    this.log.debug('Initializing BackupSender with options', options);
}

module.exports = BackupSender;
util.inherits(BackupSender, EventEmitter);

/**
 * Send the latest snapshot to a remote host.
 * @param Object backupJob The host and port mapping of the remote host.
 * @param Function callback The callback of the form f(err);
 */
function send(self, backupJob, callback) {
    var log = self.log;

    getLatestSnapshot(self, function(err, snapshot) {
        if (err) {
            return callback(err);
        }
        log.info('sending latest snapshot %s to ', snapshot, backupJob);

        log.info('running cmd: %s %s %s %s %s',
            self.zfs_send,
            backupJob.host,
            backupJob.port,
            '-P',
            snapshot);
        var zfsSend = spawn(self.zfs_send,
            [backupJob.host, backupJob.port, '-P', snapshot]);

        zfsSend.stdout.on('data', function(data) {
            log.debug('zfsSend stdout: ', data.toString());
        });

        var msg = '';
        zfsSend.stderr.on('data', function(data) {
            var dataStr = data.toString();
            log.info('zfsSend stderr: ', dataStr);
            if (msg) {
                msg += dataStr;
            } else {
                msg = dataStr;
            }
            msg += data;
        });

        zfsSend.on('exit', function(code) {
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

        return (undefined);
    });
}

function getLatestSnapshot(self, callback) {
    var log = self.log;
    log.debug({
        snapshotDir: self.snapshotDir
    }, 'getting snapshots from dir');
    var snapshots = shelljs.ls(self.snapshotDir);
    log.debug({
        snapshots: snapshots
    }, 'got snapshots');

    // fix for MANATEE-90, send an error if there are no snapshots
    if (snapshots.length === 0) {
        log.error('no snapshots found');

        return callback(new verror.VError('no snapshots found'));
    }
    snapshots.sort(compareNumbers);

    var latestSnapshot = snapshots[snapshots.length - 1];
    latestSnapshot = self.dataset + '@' + latestSnapshot;
    log.debug('latest snapshot is %s', latestSnapshot);
    return callback(null, latestSnapshot);
}

function compareNumbers(a, b) {
    return a - b;
}
