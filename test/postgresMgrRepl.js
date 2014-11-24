#!/usr/bin/env node

var PostgresMgr = require('../lib/postgresMgr');
var bunyan = require('bunyan');
var fs = require('fs');
var readline = require('readline');

var cfgFilename = '/opt/smartdc/manatee/etc/sitter.json';

var cfg = JSON.parse(fs.readFileSync(cfgFilename, 'utf8')).postgresMgrCfg;

var LOG = bunyan.createLogger({
    name: 'postgresMgr.js',
    serializers: {
        err: bunyan.stdSerializers.err
    },
    streams: [ {
        level: 'debug',
        path: '/var/tmp/postgresMgrRepl.log'
    } ],
    src: true
});

cfg.log = LOG;
cfg.zfsClientCfg.log = LOG;
cfg.snapShotterCfg.log = LOG;

var inited = false;
var pgm = new PostgresMgr(cfg);

function finish() {
    pgm.close(function () {
        console.log('Done.');
        //TODO: Figure out why this doesn't always exit.
        process.exit(0);
    });
}

function status(args, cb) {
    console.log(pgm.status());
    setImmediate(cb);
}

function start(args, cb) {
    pgm.start(cb);
}

function stop(args, cb) {
    pgm.stop(cb);
}

function xlog(args, cb) {
    pgm.getXLogLocation(function (err, res) {
        if (err) {
            return (cb(err));
        }
        console.log(res);
        return (cb());
    });
}

function reconfigure(args, cb) {
    var usage = 'role upstream_ip downstream_ip (can be "null")';
    if (args.length < 2) {
        return (cb(new Error(usage)));
    }
    function id(i) {
        if (!i || i === 'null') {
            return (undefined);
        }
        return ({
            'pgUrl': 'tcp://postgres@' + i + ':5432/postgres',
            'backupUrl': 'http://' + i + ':12345'
        });
    }
    var pgConfig = {
        'role': args[0],
        'upstream': id(args[1]),
        'downstream': id(args[2])
    };
    console.log(pgConfig);
    pgm.reconfigure(pgConfig, cb);
}

function prop(args, cb) {
    console.log(pgm[args[0]]);
    setImmediate(cb);
}

function execute(command, cb) {
    if (command === '') {
        return (setImmediate(cb));
    }
    var cs = command.split(' ');
    var c = cs.shift();
    var funcs = {
        'status': status,
        'start': start,
        'stop': stop,
        'xlog': xlog,
        'reconfigure': reconfigure,
        'prop': prop
    };
    if (!funcs[c]) {
        console.log('command ' + c + ' unknown');
        return (setImmediate(cb));
    }
    funcs[c].call(null, cs, cb);
}

function repl() {
    var rl = readline.createInterface({
        input: process.stdin,
        output: process.stdout
    });

    function doNext() {
        rl.question('> ', function (command) {
            if (command === 'quit' || command === 'q' ||
                command === 'exit') {
                rl.close();
                finish();
            } else {
                execute(command, function (err) {
                    if (err) {
                        console.error(err);
                    }
                    doNext();
                });
            }
        });
    }
    doNext();
}

pgm.on('init', function (online) {
    console.log('Postgres Manager Inited, Postgres is ' +
                (online ? 'online': 'offline'));
    repl();
});
