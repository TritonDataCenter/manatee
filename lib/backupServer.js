var EventEmitter = require('events').EventEmitter;
var restify = require('restify');

function BackupServer(options) {
  EventEmitter.call(this);
  this.log = options.log;
  var log = this.log;
  log.debug('new backup server with options', options);

  this.port = options.port;
  this.server = restify.createServer({
    log: log
  });
}

BackupServer.prototype.init = function init() {
  var self = this;
  var server = this.server;

  // ZFS send the latest snapshot to the server
  function sendBackup(req, res, next) {
    res.send('finished', req);
  };

  server.get('/backup/:url', sendBackup);

  server.listen(self.port, function() {

  });
};
