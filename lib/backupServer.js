var assert = require('assert');
var BackupQueue = require('./backupQueue');
var BackupSender = require('./backupSender');
var EventEmitter = require('events').EventEmitter;
var restify = require('restify');
var util = require('util');
var uuid = require('node-uuid');

var assertions = require('./assert');



///--- Globals

var assertFunction = assertions.assertFunction;
var assertNumber = assertions.assertNumber;
var assertObject = assertions.assertObject;
var assertString = assertions.assertString;

function BackupServer(options) {
  assertObject('options', options);
  assertObject('options.log', options.log);
  assertNumber('options.port', options.port);

  EventEmitter.call(this);

  this.log = options.log;
  var log = this.log;
  log.debug('new backup server with options', options);

  this.port = options.port;
  this.server = restify.createServer({
    log: log
  });

  /**
   * The queue of backup jobs in flight.
   */
  this.queue = new BackupQueue({ log: log });
}

module.exports = BackupServer;
util.inherits(BackupServer, EventEmitter);

BackupServer.prototype.init = function init() {
  var self = this;
  var server = this.server;
  var log = self.log;

  server.use(restify.queryParser());
  server.use(restify.bodyParser());

  server.get('/backup/:uuid', checkBackup);
  server.post('/backup/', postBackup);

  server.listen(self.port, function() {
  });

  // send the status of the backup
  function checkBackup(req, res, next) {
    return self.queue.get(req.params.uuid, function(backupJob) {
      if (!backupJob) {
        log.info('no backup job exists with id', req.params.uuid);
        return next(new restify.ResourceNotFoundError());
      }
      res.send(backupJob);
      return next();
    });
  };

  // Enqueue the backup request
  function postBackup(req, res, next) {
    var params = req.params;
    if (!params.host || !params.dataset || !params.port) {
      return next(new restify.MissingParameterError(
        'host, dataset, and port parameters required'));
    }

    var backupJob = {
      uuid: uuid(),
      host:  params.host,
      port: params.port,
      dataset: params.dataset,
      done: false
    }

    self.queue.push(backupJob);

    res.send({ jobid: backupJob.uuid, jobPath: '/backup/' + backupJob.uuid });
    return next();
  };
};

