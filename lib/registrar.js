var EventEmitter = require('events').EventEmitter;
var util = require('util');
var ZooKeeper = require('zookeeper');

/**
 * The registrar client to query host membership of shards. Emits an 'init'
 * event when shards have been updated within the registrar. Emits an 'error'
 * event on error.
 * @constructor
 *
 * @param {string} path The root path under ZK where shards are kept.
 * @param {object} zkCfg The ZK configs.
 * @param {object} log The bunyan log object.
 */
function Registrar(path, zkCfg, log) {
  EventEmitter.call(this);

  log.debug('new Registrar with', path, zkCfg);

  this.log = log;

  this.path = path;

  this.zkCfg = zkCfg;

  this.session;

  this.shardMap = {};
}

/**
 *
 */
module.exports = Registrar;
util.inherits(Registrar, EventEmitter);

/**
 * Initializes the Registrar client, emits 'init' event when done
 */
Registrar.prototype.init = function init() {
  var self = this;
  var log = self.log;

  // initialize ZK session
  log.info('establishing session to zk with cfg', self.zkCfg);

  self.session = new ZooKeeper(self.zkCfg);

  self.session.connect(function(err) {
    if (err) {
      return self.emit('error', err);
    }

    return self.refresh();
  });
};

/**
 * searches ZK for shards.
 *
 * @param {int} type The ZK type of the watch event.
 * @param {int} state The ZK state of the sessison.
 * @param {string} path The path of the znode.
 */
Registrar.prototype.refresh = function refresh(type, state, path) {
  var self = this;
  var log = self.log;
  log.debug('entering refresh with ', state, type, path);
  // error check on watch
  if (state < 0) {
    log.error('zk session failed with state ' + state);
    var err = {
      msg: 'zk session failed',
      state: state
    };
    self.emit('error', err);
  }

  log.debug('getting shards under path %s', self.path, watch);

  self.session.aw_get_children2(self.path, watch,
                                function(rc, msg, children, stat) {
    if (rc !== 0) {
      var error = {
        rc: rc,
        msg: msg,
        path: myPath
      };
      return self.emit('error', error);
    }

    log.debug('got shards', children);
    var map = {};
    // it could be that there are no shards yet, in that case, still send out
    // the init event
    if (children.length === 0) {
      log.info('no shards in registrar, emitting init event');
      self.shardMap = map;
      return self.emit('init', map);
    } else { // we got some shards
      var gotChildren = 0;
      self.sortShards(children, function(shards) {
        shards.forEach(function(znode) {
          log.debug('getting registration info for each shard', znode);
          getData(self, self.path + '/' + znode, map, function(err) {
            if (err) {
              return self.emit('error', err);
            }
            gotChildren++;
            if (gotChildren == shards.length) {
              log.info('got shard with registration', map);
              self.shardMap = map;
              log.info('emitting init event');
              return self.emit('init', map);
            }
          });
        });
      });
    }
  });

  function watch(type, state, path) {
    log.debug('watch fired', type, state, path);
    self.refresh(type, state, path);
  }
};

/**
 * return only the largest znode of each child by sorting through the
 * ephemeral nodes under the registrar path.
 *
 * @param {array} znodes The set of znodes under the registrar path, in the
 * for of guid-#.
 *
 * @param {function} callback The callback of the form f(shards) where shards
 * is the array of shards in the registrar.
 */
Registrar.prototype.sortShards = function sortShards(znodes, callback) {
  var log = this.log;
  log.debug('sorting shards', znodes);

  // shards are of the form xxxxxxx-sequence
  var shards = {};

  znodes.forEach(function(znode) {
    var idx = znode.lastIndexOf('-');
    var sequence = parseInt(znode.substring(idx + 1), 10);
    var shardId = znode.substring(0, idx);
    log.debug('got shardid %s, sequence %s', shardId, sequence);

    // update the shard in shards if it has a bigger sequence
    if (shards[shardId]) {
      if (shards[shardId].sequence < sequence) {
        shards[shardId] = {
          path: znode,
          sequence: sequence
        };
      }
    } else {
      shards[shardId] = {
        path: znode,
        sequence: sequence
      };
    }
  });

  // convert shards to an array
  var shardArray = [];
  for (var key in shards) {
    shardArray.push(shards[key].path);
  }

  log.info('sorted shards', shardArray);
  return callback(shardArray, shards);
};

// Gets the data from a specifc znode, and inserts that as a key-val pair in
// the provided map.
var getData = function getData(self, path, map, callback) {
  var log = self.log;

  self.session.a_get(path, false, function(rc, msg, stat, data) {
    if (rc !== 0) {
      var error = {
        rc: rc,
        msg: msg,
        stat: stat
      };
      return callback(err);
    }

    log.debug('got membership for shard %s, with members ', path, data, stat);
    map[path] = JSON.parse(data);
    return callback();
  });
};
