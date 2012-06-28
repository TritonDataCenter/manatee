var bunyan = require('bunyan');
var fs = require('fs');
var optimist = require('optimist');
var Sitter = require('./lib/sitter');

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
  name: 'sitter',
  serializers: {
    err: bunyan.stdSerializers.err
  },
  src: ARGV.d ? true : false,
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
cfg.snapShotterCfg.log = LOG;
cfg.postgresManCfg.log = LOG;
cfg.postgresManCfg.backupClientCfg.log = LOG;
cfg.shardCfg.log = LOG;

/**
 * Delay the start of manatee to account of ZK session timeouts
 */
setTimeout(function() { startSitter(); }, cfg.startupDelay);

function startSitter() {
  var sitter = new Sitter(cfg);
  sitter.init();
}

process.on('uncaughtException', function (err) {
  LOG.fatal({err: err}, 'uncaughtException (exiting error code 1)');
  process.exit(1);
});
