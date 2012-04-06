var Logger = require('bunyan');
var Registrar = require('./registrar');
var Daemon = require('./daemon');

console.log('hello world!');

/**
 * API
 */
module.exports = {
  Daemon: Daemon,
  Registrar: Registrar,

  createDaemon: function(options) {
    if (typeof (options) !== 'object') {
      throw new TypeError('options (object) required');
    }

    if (!options.log) {
      options.log = new Logger({
        name: 'Scallop-Daemon',
        src: true,
        level: 'trace'
      });
    }

    var daemon = new Daemon(options);
  }
};
