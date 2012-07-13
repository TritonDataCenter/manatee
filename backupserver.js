var bunyan = require('bunyan');
var fs = require('fs');
var optimist = require('optimist');
var BackupServer = require('./lib/backupServer');
var BackupSender = require('./lib/backupSender');

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
  name: 'backupSender',
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
cfg.backupServerCfg.log = LOG;
cfg.backupSenderCfg.log = LOG;

var server = new BackupServer(cfg.backupServerCfg);

// server and sender share the same queue
cfg.backupSenderCfg.queue = server.queue;
var BackupSender = new BackupSender(cfg.backupSenderCfg);

server.init();

process.on('uncaughtException', function (err) {
  LOG.fatal({err: err}, 'uncaughtException (exiting error code 1)');
  process.exit(1);
});
