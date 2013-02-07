// Copyright (c) 2012, Joyent, Inc. All rights reserved.
var assert = require('assert-plus');
var Bunyan = require('bunyan');
var EventEmitter = require('events').EventEmitter;
var spawn = require('child_process').spawn;
var vasync = require('vasync');
var verror = require('verror');

function run(cmd, log, cb) {
        if (!log) {
                log = new Bunyan({
                        name: 'shellSpawner',
                        level: 'info'
                });
        }
        log.debug('spawning command %s', cmd);

        var args = cmd.split(' ');
        var command = args[0];
        args = args.slice(1);

        var stdout = '';
        var stderr = '';
        var child = spawn(command, args);

        child.on('exit', function(code) {
                if (code !== 0) {
                        var err = new verror.VError(stderr, code);
                        log.info({
                                err: err,
                                code: code,
                                cmd: cmd
                        }, 'command failed.');
                        return cb(err, stdout, stderr);
                } else {
                        log.info({
                                cmd: cmd
                        }, 'command successful.');
                        return cb(null, stdout, stderr);
                }
        });

        child.stdout.on('data', function(data) {
                stdout += data;
                log.trace('%s stdout:  %s', cmd, data.toString());
        });

        child.stderr.on('data', function(data) {
                stderr += data;
                log.trace('%s stderr:  %s', cmd, data.toString());
        });
}
exports.spawn = run;
