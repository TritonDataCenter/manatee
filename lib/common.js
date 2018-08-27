/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2018, Joyent, Inc.
 */

var mod_fs = require('fs');

var mod_assert = require('assert-plus');
var mod_forkexec = require('forkexec');
var mod_jsprim = require('jsprim');
var mod_verror = require('verror');
var mod_vasync = require('vasync');

var VE = mod_verror.VError;


function replacefile(opts, callback) {
    mod_assert.object(opts, 'opts');
    mod_assert.string(opts.src, 'opts.src');
    mod_assert.string(opts.dst, 'opts.dst');
    mod_assert.func(callback, 'callback');

    mod_vasync.waterfall([ function (next) {
        /*
         * Unlink the destination file.
         */
        mod_fs.unlink(opts.dst, function (err) {
            if (err) {
                if (err.code === 'ENOENT') {
                    /*
                     * The file did not exist already.
                     */
                    next();
                    return;
                }

                next(new VE(err, 'unlink destination file'));
                return;
            }

            next();
        });

    }, function (next) {
        /*
         * Read source file.
         */
        mod_fs.readFile(opts.src, { encoding: null }, function (err, data) {
            if (err) {
                next(new VE(err, 'read source file'));
                return;
            }

            next(null, data);
        });

    }, function (data, next) {
        mod_assert.ok(Buffer.isBuffer(data), 'data (Buffer)');
        mod_assert.func(next, 'next');

        /*
         * Write destination file.
         */
        var mode = parseInt('0644', 8);
        mod_fs.writeFile(opts.dst, data, { mode: mode, flag: 'wx' },
          function (err) {
            if (err) {
                next(new VE(err, 'write destination file'));
                return;
            }

            next();
        });

    } ], function (err) {
        if (err) {
            callback(new VE(err, 'replace file "%s" with contents of "%s"',
              opts.dst, opts.src));
            return;
        }

        callback();
    });
}

function chown(opts, callback) {
    mod_assert.object(opts, 'opts');
    mod_assert.string(opts.path, 'opts.path');
    mod_assert.string(opts.username, 'opts.username');
    mod_assert.bool(opts.recursive, 'opts.recursive');
    mod_assert.func(callback, 'callback');

    var argv = [ '/usr/bin/chown' ];
    if (opts.recursive) {
        argv.push('-R');
    }
    argv.push(opts.username, opts.path);

    mod_forkexec.forkExecWait({ argv: argv, includeStderr: true },
      function (err) {
        if (err) {
            callback(new VE(err, '%schown "%s" to "%s"',
              opts.recursive ? 'recursive ' : '',
              opts.path, opts.username));
            return;
        }

        callback();
    });
}

function chmod(opts, callback) {
    mod_assert.object(opts, 'opts');
    mod_assert.string(opts.path, 'opts.path');
    mod_assert.string(opts.mode, 'opts.mode');
    mod_assert.bool(opts.recursive, 'opts.recursive');
    mod_assert.func(callback, 'callback');

    var argv = [ '/usr/bin/chmod' ];
    if (opts.recursive) {
        argv.push('-R');
    }
    argv.push(opts.mode, opts.path);

    mod_forkexec.forkExecWait({ argv: argv, includeStderr: true },
      function (err) {
        if (err) {
            callback(new VE(err, '%schmod "%s" to %s',
              opts.recursive ? 'recursive ' : '',
              opts.path, opts.mode));
            return;
        }

        callback();
    });
}

/*
 * Invoke zfs(1M) with the specified arguments, generating an appropriate debug
 * log message before and after command execution.  This routine is a helper
 * for all of the other ZFS functions in this file.
 */
function zfsExecCommon(log, argv, callback) {
    mod_assert.object(log, 'log');
    mod_assert.arrayOfString(argv, 'args');
    mod_assert.func(callback, 'callback');

    /*
     * Note that we do not pass our environment on to the "zfs" command, in
     * order to avoid environment-dependent behaviour; e.g., locale-specific
     * error messages or output formatting.  Buffer up to 2MB of output from
     * the ZFS command.
     */
    var opts = {
        argv: [ '/sbin/zfs' ].concat(argv),
        env: {},
        maxBuffer: 2 * 1024 * 1024,
        includeStderr: true
    };

    log.debug({ argv: argv }, 'exec zfs start');
    mod_forkexec.forkExecWait(opts, function (err, info) {
        log.debug({ argv: argv, err: err, info: info }, 'exec zfs end');

        callback(err, info);
    });
}

/*
 * Set a ZFS dataset property.
 */
function zfsSet(opts, callback) {
    mod_assert.object(opts, 'opts');
    mod_assert.object(opts.log, 'opts.log');
    mod_assert.string(opts.dataset, 'opts.dataset');
    mod_assert.string(opts.property, 'opts.property');
    mod_assert.string(opts.value, 'opts.value');
    mod_assert.func(callback, 'callback');

    opts.log.info('set ZFS property "%s" to "%s" on dataset "%s"',
      opts.property, opts.value, opts.dataset);

    zfsExecCommon(opts.log, [ 'set', opts.property + '=' + opts.value,
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
    mod_assert.object(opts, 'opts');
    mod_assert.object(opts.log, 'opts.log');
    mod_assert.string(opts.dataset, 'opts.dataset');
    mod_assert.string(opts.property, 'opts.property');
    mod_assert.func(callback, 'callback');

    opts.log.info('clear ZFS property "%s" on dataset "%s"', opts.property,
      opts.dataset);

    zfsExecCommon(opts.log, [ 'inherit', opts.property, opts.dataset ],
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
    mod_assert.object(opts, 'opts');
    mod_assert.object(opts.log, 'opts.log');
    mod_assert.string(opts.dataset, 'opts.dataset');
    mod_assert.string(opts.property, 'opts.property');
    mod_assert.func(callback, 'callback');

    opts.log.info('get ZFS property "%s" on dataset "%s"', opts.property,
      opts.dataset);

    zfsExecCommon(opts.log, [ 'get', '-Hp', opts.property, opts.dataset ],
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

        opts.log.info('ZFS property "%s" on dataset "%s" has value "%s"',
          opts.property, opts.dataset, t[2]);

        callback(null, t[2]);
    });
}

/*
 * Create a snapshot of a ZFS dataset.
 */
function zfsSnapshot(opts, callback) {
    mod_assert.object(opts, 'opts');
    mod_assert.object(opts.log, 'opts.log');
    mod_assert.string(opts.dataset, 'opts.dataset');
    mod_assert.string(opts.snapshot, 'opts.snapshot');
    mod_assert.func(callback, 'callback');

    opts.log.info('create ZFS snapshot "%s" on dataset "%s"', opts.snapshot,
      opts.dataset);

    zfsExecCommon(opts.log, [ 'snapshot', opts.dataset + '@' + opts.snapshot ],
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
    mod_assert.object(opts, 'opts');
    mod_assert.object(opts.log, 'opts.log');
    mod_assert.string(opts.dataset, 'opts.dataset');
    mod_assert.optionalObject(opts.props, 'opts.props');
    mod_assert.func(callback, 'callback');

    var args = [ 'create' ];
    if (opts.props) {
        mod_jsprim.forEachKey(opts.props, function (prop, val) {
            args.push('-o', prop + '=' + val);
        });
    }
    args.push(opts.dataset);

    opts.log.info('create ZFS dataset "%s"', opts.dataset);

    zfsExecCommon(opts.log, args, function (err, info) {
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
    mod_assert.object(opts, 'opts');
    mod_assert.object(opts.log, 'opts.log');
    mod_assert.string(opts.dataset, 'opts.dataset');
    mod_assert.string(opts.target, 'opts.target');
    mod_assert.bool(opts.parents, 'opts.parents');
    mod_assert.func(callback, 'callback');

    var args = [ 'rename' ];
    if (opts.parents) {
        args.push('-p');
    }
    args.push(opts.dataset, opts.target);

    opts.log.info('rename ZFS dataset from "%s" to "%s"', opts.dataset,
      opts.target);

    zfsExecCommon(opts.log, args, function (err, info) {
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
    mod_assert.object(opts, 'opts');
    mod_assert.object(opts.log, 'opts.log');
    mod_assert.string(opts.dataset, 'opts.dataset');
    mod_assert.func(callback, 'callback');

    opts.log.info('mount ZFS dataset "%s"', opts.dataset);

    zfsExecCommon(opts.log, [ 'mount', opts.dataset ], function (err, info) {
        if (err) {
            callback(new VE(err, 'mount dataset "%s"', opts.dataset));
            return;
        }

        callback();
    });
}

/*
 * Destroy a ZFS dataset.
 */
function zfsDestroy(opts, callback) {
    mod_assert.object(opts, 'opts');
    mod_assert.object(opts.log, 'opts.log');
    mod_assert.string(opts.dataset, 'opts.dataset');
    mod_assert.bool(opts.recursive, 'opts.recursive');
    mod_assert.func(callback, 'callback');

    opts.log.info('destroy ZFS dataset "%s"%s', opts.dataset,
      (opts.recursive ? ' (recursive)' : ''));

    var args = [ 'destroy' ];
    if (opts.recursive) {
        args.push('-r');
    }
    args.push(opts.dataset);

    zfsExecCommon(opts.log, args, function (err, info) {
        if (err) {
            callback(new VE(err, '%sdestroy dataset "%s"',
              opts.recursive ? 'recursively ' : '', opts.dataset));
            return;
        }

        callback();
    });
}

/*
 * Unmount a ZFS dataset.
 */
function zfsUnmount(opts, callback) {
    mod_assert.object(opts, 'opts');
    mod_assert.object(opts.log, 'opts.log');
    mod_assert.string(opts.dataset, 'opts.dataset');
    mod_assert.bool(opts.force, 'opts.force');
    mod_assert.func(callback, 'callback');

    opts.log.info('unmount ZFS dataset "%s"' + (opts.force ? ' (force)' : ''),
      opts.dataset);

    var args = [ 'unmount' ];
    if (opts.force) {
        args.push('-f');
    }
    args.push(opts.dataset);

    zfsExecCommon(opts.log, args, function (err, info) {
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
    mod_assert.object(opts, 'opts');
    mod_assert.object(opts.log, 'opts.log');
    mod_assert.string(opts.dataset, 'opts.dataset');
    mod_assert.func(callback, 'callback');

    opts.log.info('check if ZFS dataset "%s" exists', opts.dataset);

    zfsExecCommon(opts.log, [ 'list', '-Hp', '-o', 'name' ],
      function (err, info) {
        if (err) {
            callback(new VE(err, 'check for dataset "%s"', opts.dataset));
            return;
        }

        var lines = info.stdout.split('\n');
        var exists = lines.indexOf(opts.dataset) !== -1;

        callback(null, exists);
    });
}


module.exports = {
    replacefile: replacefile,
    chown: chown,
    chmod: chmod,
    zfsSet: zfsSet,
    zfsInherit: zfsInherit,
    zfsGet: zfsGet,
    zfsMount: zfsMount,
    zfsUnmount: zfsUnmount,
    zfsSnapshot: zfsSnapshot,
    zfsCreate: zfsCreate,
    zfsDestroy: zfsDestroy,
    zfsRename: zfsRename,
    zfsExists: zfsExists
};
