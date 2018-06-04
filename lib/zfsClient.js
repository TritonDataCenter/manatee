/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2018, Joyent, Inc.
 */


var assert = require('assert-plus');
var path = require('path');
var fs = require('fs');
var once = require('once');
var net = require('net');
var restify = require('restify');
var spawn = require('child_process').spawn;
var vasync = require('vasync');
var verror = require('verror');
var forkexec = require('forkexec');
var jsprim = require('jsprim');

var lib_common = require('../lib/common');

var VE = verror.VError;

var CMD_ZFS = '/sbin/zfs';

var ZFS_LOG = null;


function zfsExecCommon(log, argv, callback) {
    assert.object(log, 'log');
    assert.arrayOfString(argv, 'args');
    assert.func(callback, 'callback');

    /*
     * Note that we do not pass our environment on to the "zfs" command, in
     * order to avoid environment-dependent behaviour; e.g., locale-specific
     * error messages or output formatting.  Buffer up to 2MB of output from
     * the ZFS command.
     */
    var opts = {
        argv: [ CMD_ZFS ].concat(argv),
        env: {},
        maxBuffer: 2 * 1024 * 1024,
        includeStderr: true
    };

    log.debug({ argv: argv }, 'exec zfs start');
    forkexec.forkExecWait(opts, function (err, info) {
        log.debug({ argv: argv, err: err, info: info }, 'exec zfs end');

        callback(err, info);
    });
}

/*
 * Set a ZFS dataset property.
 */
function zfsSet(opts, callback) {
    assert.object(opts, 'opts');
    assert.string(opts.dataset, 'opts.dataset');
    assert.string(opts.property, 'opts.property');
    assert.string(opts.value, 'opts.value');
    assert.func(callback, 'callback');

    var log = ZFS_LOG;

    log.info('set ZFS property "%s" to "%s" on dataset "%s"', opts.property,
      opts.value, opts.dataset);

    zfsExecCommon(log, [ 'set', opts.property + '=' + opts.value,
      opts.dataset ], function (err, info) {
        if (err) {
            callback(new VE(err, 'set property "%s" to "%s" on dataset "%s"',
              opts.property, opts.value, opts.dataset));
            return;
        }

        callback();
    });
}

/*
 * Resets a ZFS dataset property so that a value is inherited from the parent
 * dataset.
 */
function zfsInherit(opts, callback) {
    assert.object(opts, 'opts');
    assert.string(opts.dataset, 'opts.dataset');
    assert.string(opts.property, 'opts.property');
    assert.func(callback, 'callback');

    var log = ZFS_LOG;

    log.info('clear ZFS property "%s" on dataset "%s"', opts.property,
      opts.dataset);

    zfsExecCommon(log, [ 'inherit', opts.property, opts.dataset ],
      function (err, info) {
        if (err) {
            callback(new VE(err, 'clear property "%s" on dataset "%s"',
              opts.property, opts.value, opts.dataset));
            return;
        }

        callback();
    });
}

/*
 * Gets the value of a ZFS dataset property.
 */
function zfsGet(opts, callback) {
    assert.object(opts, 'opts');
    assert.string(opts.dataset, 'opts.dataset');
    assert.string(opts.property, 'opts.property');
    assert.func(callback, 'callback');

    var log = ZFS_LOG;

    log.info('get ZFS property "%s" on dataset "%s"', opts.property,
      opts.dataset);

    zfsExecCommon(log, [ 'get', '-Hp', opts.property, opts.dataset ],
      function (err, info) {
        if (err) {
            callback(new VE(err, 'get property "%s" from dataset "%s"',
              opts.property, opts.dataset));
            return;
        }

        var t = info.stdout.split('\t');
        if (t.length !== 4 || t[0] !== opts.dataset || t[1] !== opts.property) {
            callback(new VE('zfs get "%s" "%s": invalid line: %s',
              opts.property, opts.dataset, info.stdout.trim()));
            return;
        }

        log.info('ZFS property "%s" on dataset "%s" has value "%s"',
          opts.property, opts.dataset, t[2]);

        callback(null, t[2]);
    });
}

/*
 * Create a snapshot of a ZFS dataset.
 */
function zfsSnapshot(opts, callback) {
    assert.object(opts, 'opts');
    assert.string(opts.dataset, 'opts.dataset');
    assert.string(opts.snapshot, 'opts.snapshot');
    assert.func(callback, 'callback');

    var log = ZFS_LOG;

    log.info('create ZFS snapshot "%s" on dataset "%s"', opts.snapshot,
      opts.dataset);

    zfsExecCommon(log, [ 'snapshot', opts.dataset + '@' + opts.snapshot ],
      function (err, info) {
        if (err) {
            callback(new VE(err, 'create snapshot "%s" of dataset "%s"',
              opts.snapshot, opts.dataset));
            return;
        }

        callback();
    });
}

/*
 * Create a ZFS dataset.
 */
function zfsCreate(opts, callback) {
    assert.object(opts, 'opts');
    assert.string(opts.dataset, 'opts.dataset');
    assert.optionalObject(opts.props, 'opts.props');
    assert.func(callback, 'callback');

    var log = ZFS_LOG;

    var args = [ 'create' ];
    if (opts.props) {
        jsprim.forEachKey(opts.props, function (prop, val) {
            args.push('-o', prop + '=' + val);
        });
    }
    args.push(opts.dataset);

    log.info('create ZFS dataset "%s"', opts.dataset);

    zfsExecCommon(log, args, function (err, info) {
        if (err) {
            callback(new VE(err, 'create dataset "%s"', opts.dataset));
            return;
        }

        callback();
    });
}

/*
 * Rename a ZFS dataset.
 */
function zfsRename(opts, callback) {
    assert.object(opts, 'opts');
    assert.string(opts.dataset, 'opts.dataset');
    assert.string(opts.target, 'opts.target');
    assert.bool(opts.parents, 'opts.parents');
    assert.func(callback, 'callback');

    var log = ZFS_LOG;

    var args = [ 'rename' ];
    if (opts.parents) {
        args.push('-p');
    }
    args.push(opts.dataset, opts.target);

    log.info('rename ZFS dataset from "%s" to "%s"', opts.dataset, opts.target);

    zfsExecCommon(log, args, function (err, info) {
        if (err) {
            callback(new VE(err, 'rename dataset "%s" to "%s"', opts.dataset,
              opts.target));
            return;
        }

        callback();
    });
}


/*
 * Mount a ZFS dataset.
 */
function zfsMount(opts, callback) {
    assert.object(opts, 'opts');
    assert.string(opts.dataset, 'opts.dataset');
    assert.func(callback, 'callback');

    var log = ZFS_LOG;

    log.info('mount ZFS dataset "%s"', opts.dataset);

    zfsExecCommon(log, [ 'mount', opts.dataset ], function (err, info) {
        if (err) {
            callback(new VE(err, 'mount dataset "%s"', opts.dataset));
            return;
        }

        callback();
    });
}

/*
 * Unmount a ZFS dataset.
 */
function zfsUnmount(opts, callback) {
    assert.object(opts, 'opts');
    assert.string(opts.dataset, 'opts.dataset');
    assert.bool(opts.force, 'opts.force');
    assert.func(callback, 'callback');

    var log = ZFS_LOG;

    log.info('unmount ZFS dataset "%s"' + (opts.force ? ' (force)' : ''),
      opts.dataset);

    var args = [ 'unmount' ];
    if (opts.force) {
        args.push('-f');
    }
    args.push(opts.dataset);

    zfsExecCommon(log, args, function (err, info) {
        if (err) {
            callback(new VE(err, '%sunmount dataset "%s"',
              opts.force ? 'force ' : '', opts.dataset));
            return;
        }

        callback();
    });
}

/*
 * Check if a ZFS dataset exists.
 */
function zfsExists(opts, callback) {
    assert.object(opts, 'opts');
    assert.string(opts.dataset, 'opts.dataset');
    assert.func(callback, 'callback');

    var log = ZFS_LOG;

    log.info('check if ZFS dataset "%s" exists', opts.dataset);

    zfsExecCommon(log, [ 'list', '-Hp', '-o', 'name' ], function (err, info) {
        if (err) {
            callback(new VE(err, 'check for dataset "%s"', opts.dataset));
            return;
        }

        var lines = info.stdout.split('\n');
        var exists = lines.indexOf(opts.dataset) !== -1;

        callback(null, exists);
    });
}



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

    /*
     * XXX
     */
    if (ZFS_LOG === null) {
        ZFS_LOG = options.log.child({ component: 'zfs' });
    }

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

ZfsClient.prototype.restore = function restore(serverUrl, callback) {
    var self = this;
    var log = self._log;

    var oldDataset = null;

    log.info({
        dataset: self._dataset,
        serverUrl: serverUrl,
        pollInterval: self._pollInterval
    }, 'ZFSClient.restore: entering');

    self._client = restify.createJsonClient({
        url: serverUrl,
        version: '*'
    });

    vasync.waterfall([ function (next) {
        /*
         * Before we try to receive a copy of the database from the upstream
         * peer, move the existing dataset (if any) out of the way.
         */
        self._isolateDataset({ dataset: self._dataset, prefix: 'rebuild' },
          next);

    }, function (isolatedName, next) {
        /*
         * Keep the name of the newly isolated dataset so that we can delete
         * it if this restore succeeds.  If there was no extant dataset to
         * isolate, this value will be null.
         */
        oldDataset = isolatedName;

        self._receive(self._dataset, serverUrl, self._pollInterval, next);

    }, function (next) {
        /*
         * Set the "canmount" property to "noauto" so that the system does not
         * try to automatically mount this dataset.  Manatee itself will mount
         * and unmount the dataset as required.
         */
        zfsSet({ dataset: self._dataset, property: 'canmount',
          value: 'noauto' }, next);

    }, function (next) {
        zfsSet({ dataset: self._dataset, property: 'mountpoint',
          value: self._mountpoint }, next);

    }, function (next) {
        zfsMount({ dataset: self._dataset }, next);

    }, function (next) {
        /*
         * Before we begin to use the dataset received from the upstream peer,
         * take an initial snapshot.
         */
        zfsSnapshot({ dataset: self._dataset,
          snapshot: String(new Date().getTime()) }, next);

    } ], function (err) {
        if (err) {
            err = new VE(err, 'receiving snapshot from "%s"', serverUrl);

            log.error({
                err: err,
                dataset: self._dataset,
                oldDataset: oldDataset,
                serverUrl: serverUrl,
                pollInterval: self._pollInterval
            }, 'unable to restore snapshot');
            callback(err, oldDataset);
            return;
        }

        log.info({
            dataset: self._dataset,
            oldDataset: oldDataset,
            serverUrl: serverUrl,
            pollInterval: self._pollInterval
        }, 'ZFSClient.restore: success');
        callback(null, oldDataset);
    });
};

/*
 * XXX MOUNT A DATASET OR CREATE IT IF MISSING.
 */
ZfsClient.prototype.mountDataset = function mountDataset(opts, callback) {
    var self = this;

    assert.object(opts, 'opts');
    assert.bool(opts.createIfMissing, 'opts.createIfMissing');

    var log = self._log;

    log.debug({
        dataset: self._dataset
    }, 'entering ZfsClient.assertDataset');

    vasync.waterfall([ function (next) {
        /*
         * Check to see if the dataset exists already.
         */
        zfsExists({ dataset: self._dataset }, next);

    }, function (exists, next) {
        assert.bool(exists, 'exists');
        assert.func(next, 'next');

        if (exists) {
            setImmediate(next);
            return;
        }

        /*
         * The dataset does not already exist.
         */
        if (!opts.createIfMissing) {
            /*
             * The caller does not want the dataset to be created if it
             * does not exist.  Return without doing any more work.
             */
            callback(null, { exists: false });
            return;
        }

        /*
         * Create the dataset.
         */
        zfsCreate({ dataset: self._dataset, props: { canmount: 'noauto' }},
          next);

    }, function (next) {
        /*
         * Ensure that the "canmount" property is set to "noauto".
         */
        zfsGet({ dataset: self._dataset, property: 'canmount' },
          function (err, value) {
            if (err) {
                next(err);
                return;
            }

            if (value === 'noauto') {
                next();
                return;
            }

            zfsSet({ dataset: self._dataset, property: 'canmount',
              value: 'noauto' }, next);
        });

    }, function (next) {
        /*
         * Ensure that the "mountpoint" property is set correctly.
         */
        zfsGet({ dataset: self._dataset, property: 'mountpoint' },
          function (err, value) {
            if (err) {
                next(err);
                return;
            }

            if (value === self._mountpoint) {
                next();
                return;
            }

            zfsSet({ dataset: self._dataset, property: 'mountpoint',
              value: self._mountpoint }, next);
        });

    }, function (next) {
        /*
         * Ensure that the dataset is mounted.
         */
        zfsGet({ dataset: self._dataset, property: 'mounted' },
          function (err, value) {
            if (err) {
                next(err);
                return;
            }

            if (value === 'yes') {
                setImmediate(next);
                return;
            }

            zfsMount({ dataset: self._dataset }, next);
        });

    }, function (next) {
        /*
         * Confirm in mnttab(4) that the dataset is mounted at the correct
         * location.
         */
        fs.readFile('/etc/mnttab', { encoding: 'utf8' }, function (err, data) {
            if (err) {
                next(new VE(err, 'reading mount table'));
                return;
            }

            var found = 0;
            var lines = data.split('\n');
            for (var i = 0; i < lines.length; i++) {
                var t = lines[i].split('\t');
                if (t.length < 5) {
                    continue;
                }

                var special = t[0];
                var mountpoint = t[1];
                var fstype = t[2];

                if (mountpoint === self._mountpoint) {
                    if (fstype === 'zfs' && special === self._dataset) {
                        found++;
                        continue;
                    }

                    next(new VE('incorrect file system mounted at "%s": %j',
                        self._mountpoint, t));
                    return;
                }

                if (fstype === 'zfs' && special === self._dataset) {
                    assert.notEqual(mountpoint, self._mountpoint);

                    next(new VE('dataset "%s" mounted at "%s" instead of "%s"',
                      self._dataset, mountpoint, self._mountpoint));
                    return;
                }
            }

            if (found !== 1) {
                next(new VE('found %d instead of 1 mount in mnttab(4)',
                  found));
                return;
            }

            next();
        });
    }, function (next) {
        /*
         * In case the PostgreSQL user ID has changed from previous image
         * versions, or is different in a dataset received from a remote peer,
         * reset ownership now.
         */
        lib_common.chown({ path: self._mountpoint, username: self._dbUser,
          recursive: true }, next);

    } ], function (err) {
        if (err) {
            callback(new VE(err, '%smounting dataset "%s" at "%s"',
              opts.createIfMissing ? 'creating/' : '',
              self._dataset, self._mountpoint));
            return;
        }

        callback(null, { exists: true });
    });
};

/*
 * XXX ISOLATE A DATASET -- that is, ensure it is unmounted, won't be remounted,
 * and is renamed off to the side with the prefix provided.
 */
ZfsClient.prototype._isolateDataset = function (opts, callback) {
    assert.object(opts, 'opts');
    assert.string(opts.dataset, 'opts.dataset');
    assert.string(opts.prefix, 'opts.prefix');
    assert.func(callback, 'callback');

    var self = this;
    var log = self._log;
    log.debug({
        dataset: opts.dataset,
        prefix: opts.prefix
    }, 'entering ZfsClient._isolateDataset');

    var isolate = false;

    /*
     * Keep isolated datasets together under a common parent dataset.
     */
    assert.string(self._parentDataset, 'self._parentDataset');
    var isolatedName = [ self._parentDataset, 'isolated',
      opts.prefix + '-' + (new Date()).toISOString() ].join('/');

    vasync.waterfall([ function (next) {
        /*
         * Check to see if the dataset we have been asked to isolate exists.
         */
        zfsExists({ dataset: opts.dataset }, next);

    }, function (exists, next) {
        assert.bool(exists, 'exists');
        assert.func(next, 'next');

        if (!exists) {
            log.info('dataset "%s" does not exist; not preserving',
              opts.dataset);
            isolate = false;
            isolatedName = null;
            setImmediate(next);
            return;
        }

        isolate = true;
        log.info('dataset "%s" exists; renaming to "%s"', opts.dataset,
          isolatedName);

        /*
         * Set "canmount" to "off", which implicitly unmounts the dataset.
         * This will fail if the dataset is busy and cannot be unmounted.  When
         * set to "off", the dataset cannot be mounted in future unless the
         * property is reset to "noauto" or "on".
         */
        zfsSet({ dataset: opts.dataset, property: 'canmount', value: 'off' },
          next);

    }, function (next) {
        if (!isolate) {
            setImmediate(next, null, null);
            return;
        }

        /*
         * Check to make sure the dataset was unmounted as a result of seting
         * the "canmount" property.
         */
        zfsGet({ dataset: opts.dataset, property: 'mounted' }, next);

    }, function (value, next) {
        if (!isolate) {
            setImmediate(next);
            return;
        }

        if (value !== 'no') {
            next(new VE('wanted "no" but found "%s" for property "mounted"',
                value));
            return;
        }

        /*
         * Clear any explicit mount point for this dataset, so that it will
         * inherit the mountpoint of the parent dataset after we rename it.
         */
        zfsInherit({ dataset: opts.dataset, property: 'mountpoint' }, next);

    }, function (next) {
        if (!isolate) {
            setImmediate(next);
            return;
        }

        /*
         * Rename the dataset to place it in the isolated dataset holding
         * area.  The isolate holding dataset might not exist, so request that
         * any intermediate datasets be created automatically.
         */
        zfsRename({ dataset: opts.dataset, target: isolatedName,
          parents: true }, next);

    } ], function (err) {
        if (err) {
            callback(new VE(err, 'preserving dataset "%s"', opts.dataset));
            return;
        }

        callback(null, isolatedName);
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
        callback(err, obj ? obj.jobPath : null);
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
            cb();
        },
        function createServer(_, cb) {
            cb = once(cb);
            server = net.createServer();
            server.on('connection', function (socket) {
                log.info('ZFSClient._receive: got socket, piping to zfs recv');
                socket.pipe(_.zfsRecv.stdin);
                cb();
            });

            log.info({port: self._zfsPort, host: self._zfsHost},
                     'listening for zfs send on');
            server.listen(self._zfsPort, self._zfsHost, 1, function (err) {
                if (err) {
                    log.warn({err: err},
                             'ZfsClient._receive: could not start server');
                    err = new verror.VError(err);
                }
                cb(err);
            });

            server.on('error', function (err) {
                log.warn({err: err}, 'ZfsClient._receive: got socket error');
                cb(new verror.VError(err));
            });
        },
        function _postRestoreRequest(_, cb) {
            self._postRestoreRequest(serverUrl, function (err, jobPath) {
                _.jobPath = jobPath;
                cb(err);
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

        // clear the interval incase one of the other funcs failed
        clearInterval(restoreIntervalId);

        if (err) {
            log.info({
                err: err,
                dataset: dataset,
                serverUrl: serverUrl,
                pollInterval: pollInterval
            }, 'unable to receive snapshot');
            callback(err);
            return;
        }

        log.info({
            dataset: dataset,
            serverUrl: serverUrl,
            pollInterval: pollInterval
        }, 'successfully received zfs dataset');
        callback();
    });
};
