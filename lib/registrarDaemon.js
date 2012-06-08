var bunyan = require('bunyan');
var fs = require('fs');
var optimist = require('optimist');
var RegistrarServer = require('../lib/registrarServer');

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
 // serializers: {
 //   err: bunyan.stdSerializers.err
 // },
 src: ARGV.d ? true : false,
  //stream: process.stderr
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

var r = new RegistrarServer(cfg);

r.init();
