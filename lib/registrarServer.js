var assert = require('assert');
var assertions = require('./assert');
var EventEmitter = require('events').EventEmitter;
var restify = require('restify');
var util = require('util');
var uuid = require('node-uuid');
var ZooKeeper = require('zookeeper');

///--- Globals

var assertFunction = assertions.assertFunction;
var assertNumber = assertions.assertNumber;
var assertObject = assertions.assertObject;
var assertString = assertions.assertString;

/**
 * The registrar server provides a simple REST shim over ZooKeeper that exposes
 * shard membership to clients.
 * A pre configured znode in ZooKeeper contains under it, emphemeral znodes that
 * represent shards. This service merely keeps a watch on the znodes and updates
 * its internal cache when changes to the znodes occur. This cache is made
 * available to clients via GET /shards/
 *
 * err events are emitted in the event of errors.
 */
function RegistrarServer(options) {
  assertObject('options', options);
  assertObject('options.log', options.log);
  assertNumber('options.port', options.port);
  assertObject('options.zkCfg', options.zkCfg);
  assertString('options.registrarPath', options.registrarPath);

  EventEmitter.call(this);

  var self = this;
  this.log = options.log;
  this.port = options.port;
  this.server = restify.createServer({
    log: self.log
  });
  this.zkCfg = options.zkCfg;
  this.registrarPath = options.registrarPath;
  this.zkSession;
  this.shards = {};

  this.log.info('new Registrar service with options', options);
}

module.exports = RegistrarServer;
util.inherits(RegistrarServer, EventEmitter);

/**
 * Initializes the registrar server. Outpus an 'init' event when done.
 */
RegistrarServer.prototype.init = function init() {
  var self = this;
  var log = self.log;
  log.info('initializing registrar server');

  self.once('zkinit', function() {
    getShards(self);
  });

  self.once('update', function() {
    initRestify(self);
  });

  self.once('restinit', function() {
    self.emit('init');
  });

  initZooKeeper(self);
};

function initRestify(self) {
  var log = self.log;
  log.info('initializing restify server');

  var server = self.server;
  server.use(restify.queryParser());
  server.use(restify.bodyParser());

  server.get('/shards/', function(req, res, next) {
    log.debug('sending shards', self.shards);
    res.send(self.shards);
    return next();
  });

  server.listen(self.port, function() {
    self.emit('restinit');
  });
}

function initZooKeeper(self) {
  var log = self.log;
  log.info('initializing zookeeper');

  // initialize ZK
  self.session = new ZooKeeper(self.zkCfg);
  self.session.connect(function(err) {
    if (err) {
      log.error('unable to connect to zookeeper');
      return self.emit('err', err);
    }
    return self.emit('zkinit');
  });
}

function getShards(self) {
  var log = self.log;
  log.debug('getting shards from %s with watch', self.registrarPath, watch);

  // Get shard and set a watch on the root znode.
  self.session.aw_get_children2(self.registrarPath, watch,
                                function(rc, msg, shards, stat)
  {
    if (rc !== 0) {
      var err = {
        rc: rc,
        msg: msg
      };
      log.error('error getting shards', err);
      return self.emit('err', err);
    }

    log.debug('got shards', shards);

    // short circuit on no shards
    if (shards.length === 0) {
      log.debug('no shards, returning empty shard list');
      self.shards = {};
      return self.emit('update', self.shards);
    }

    /*
     * Note: We must set watches on all of the children, because if the children
     * data changes, the parent watch will not fire. Thus if the data in the
     * child znode changes, only the child watch will inform us of those changes
     */

    // for each shard, update self.shards with that info
    var numberOfShards = 0;
    var updatedShards = {};

    shards.forEach(function(shard) {
      var path = self.registrarPath + '/' + shard;
      log.debug('getting shard data for shard %s', shard);
      self.session.aw_get(path, watch, function(rc, msg, stat, data) {
        log.debug('got shard response', rc, msg, stat, data);
        if (rc != 0) {
          var err = {
            rc: rc,
            msg: msg,
            stat: stat
          };
          log.error('unable to get shard data');
          return self.emit('err', err);
        }

        updatedShards[shard] = JSON.parse(data);
        numberOfShards++;
        if (numberOfShards === shards.length) {
          log.info('got updated shard list', updatedShards);
          self.shards = updatedShards;
          return self.emit('update', updatedShards);
        }
      });
    });
  });

  function watch(type, state, path) {
    log.info('shard change occured, updating shards', type, state, path);
    return getShards(self);
  }
}

