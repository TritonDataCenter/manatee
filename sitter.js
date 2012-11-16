// Copyright (c) 2012, Joyent, Inc. All rights reserved.
var bunyan = require('bunyan');
var fs = require('fs');
var optimist = require('optimist');
var Shard = require('./lib/shard');

///--- Mainline

var ARGV = optimist.options({
        'd': {
                alias: 'debug',
                describe: 'debug level'
        },
        'f': {
                alias: 'file',
                demand: true,
                describe: 'configuration file'
        }
}).argv;

var CFG;

var LOG = bunyan.createLogger({
        level: ARGV.d ? (ARGV.d > 1 ? 'trace' : 'debug') : 'info',
        name: 'pg-sitter',
        serializers: {
                err: bunyan.stdSerializers.err
        },
        src: ARGV.d ? true : false
});

function readConfig() {
        if (!CFG) {
                CFG = JSON.parse(fs.readFileSync(ARGV.f, 'utf8'));
                LOG.trace({config: CFG, file: ARGV.f}, 'Configuration loaded');
        }

        return (CFG);
}

var cfg = readConfig();
cfg.log = LOG;
cfg.zkCfg.log = LOG;
cfg.postgresMgrCfg.log = LOG;
cfg.postgresMgrCfg.backupClientCfg.log = LOG;
cfg.postgresMgrCfg.snapShotterCfg.log = LOG;
cfg.heartbeaterCfg.log = LOG;

LOG.info('starting manatee');
var shard = new Shard(cfg);

LOG.info('manatee started');
LOG.trace('manatee shard', shard);

process.on('uncaughtException', function (err) {
        LOG.fatal({err: err}, 'uncaughtException (exiting error code 1)');
        process.exit(1);
});
