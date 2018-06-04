/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2018, Joyent, Inc.
 */


var mod_assert = require('assert-plus');
var mod_forkexec = require('forkexec');
var mod_verror = require('verror');

var VE = mod_verror.VError;


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


module.exports = {
    chown: chown,
    chmod: chmod
};
