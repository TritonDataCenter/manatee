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
                LOG.info({config: CFG, file: ARGV.f}, 'Configuration loaded');
        }

        return (CFG);
}

var cfg = readConfig();
cfg.log = LOG;
cfg.zkCfg.log = LOG;
cfg.postgresManCfg.log = LOG;
cfg.postgresManCfg.backupClientCfg.log = LOG;
cfg.postgresManCfg.snapShotterCfg.log = LOG;
cfg.heartbeaterCfg.log = LOG;

var shard = new Shard(cfg);

shard.on('connect', function() {
        shard.init();
});

process.on('uncaughtException', function (err) {
        LOG.fatal({err: err}, 'uncaughtException (exiting error code 1)');
        process.exit(1);
});
