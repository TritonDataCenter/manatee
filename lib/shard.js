var assert = require('assert-plus');
var EventEmitter = require('events').EventEmitter;
var Peer = require('./peer');
var util = require('util');
var ZooKeeper = require('zookeeper');
var zkplus = require('zkplus');

var ROLE = {
  PRIMARY: 0,
  STANDBY: 1
};

var ZTYPE_SEQ_EPH = ZooKeeper.ZOO_SEQUENCE | ZooKeeper.ZOO_EPHEMERAL;

/**
 * Represents a Postgres shard. We want to be as aggressive as possible on
 * pruning errors, which means the shard will emit error events which should
 * indicate to the consumer to restart the shard.
 *
 * Note the Shard assumes that both path, and registrarPath znodes have been
 * created.
 * @constructor
 *
 * @param {string} path The path under which znodes for this shard are stored.
 * Note, it's assumed this path already exists at shard creation time.
 * @param {string} registrarPath The path where registrar info is stored.
 * @param {object} zkCfg The ZK configs.
 * @param {string} url The url of the current node.
 * @param {object} log The bunyan log object.
 */
function Shard(options) {
  assert.object(options, 'options');
  assert.object(options.log, 'options.log');
  assert.string(options.shardPath, 'options.shardPath');
  assert.string(options.shardId, 'options.shardId');
  assert.string(options.registrarPath, 'options.registrarPath');
  assert.string(options.registrarPathPrefix, 'options.registrarPathPrefix');
  assert.object(options.zkCfg, 'options.zkCfg');
  assert.string(options.url, 'options.url');

  EventEmitter.call(this);

  this.log = options.log;

  /**
   * The path under which znodes for this shard are stored
   */
  this.shardPath = options.shardPath;

  this.shardId = options.shardId;

  /**
   * The path which the registrar info is stored
   * e.g. /registrar/$shardid
   */
  this.registrarPath = options.registrarPath;

  /**
   * The root path of the registrarPathPrefix, this is a persistent znode.
   * e.g. /registrar
   */
  this.registrarPathPrefix = options.registrarPathPrefix;

  /**
   * The membership info stored in the registrar about this shard
   */
  this.shardInfo = null;

  /**
   * The zk cfg
   */
  this.zkCfg = options.zkCfg;

  /**
   * The url of this peer
   */
  this.url = options.url;

  /**
   * The ZK session
   */
  this.session = null;

  /**
   * The primary Peer
   */
  this.primary = null;

  /**
   * The synchronous standby Peer.
   */
  this.sync = null;

  /**
   * The asynchronous standby Peer.
   */
  this.async = null;

  /**
   * This peer's current role, one of (primary, standby)
   */
  this.role = null;

  this.pathPrefix = this.shardPath + '/shard-';

  /**
   * The path of this Peer's znode in ZK
   */
  this.myPath = null;

  /**
   * The relative path of this Peer's znode in ZK. i.e. after the last /
   */
  this.myRelativePath = null;

  /**
   * The number of peers in the current shard
   */
  this.peerCount = 0;

  this.log.info('new Shard with options', options);
}

module.exports = Shard;
util.inherits(Shard, EventEmitter);

/**
 * Initializes this peer into the shard
 * returns either a 'primary' or 'standby' event on initialization indicating
 * the status of the peer in the shard.
 *
 * Note standby event may not return shardInfo as there is slack between you
 * becoming the standby and the primary writing the shardInfo. Consumers must
 * check whether shardInfo exists before proceeding as the primary.
 *
 */
Shard.prototype.init = function init() {
  var self = this;
  var log = self.log;
  log.info('initializing shard');
  var client = zkplus.createClient({
    servers: [{
      host: self.zkCfg.connect.split(':')[0],
      port: parseInt(self.zkCfg.connect.split(':')[1], 10)
    }]
  });

  client.on('connect', function () {
    log.info('mkdir -p shard and registrar paths');
    client.mkdirp(self.registrarPathPrefix, function (err) {
      client.mkdirp(self.shardPath, function(err) {
        client.close();
        // initialize default zk client after paths have been initialized.
        initZooKeeper(self);
      });
    });
  });

  self.on('zkinit', function() {
    log.info('joining shard');
    return joinShard(self);
  });
};

/**
 * Writes the shard membership to the registrar.
 * @param {object} registrar The registrar object {
 * type: database,
 * database: {
 *  primary:
 *  sync:
 *  async:
 *  backupUrl:
 * }
 * }
 * @param {function} callback The function of the form f(err)
 */
Shard.prototype.writeRegistrar = function writeRegistrar(registrar, callback) {
  var self = this;
  var log = self.log;
  log.info('writing registrar to zookeeper', registrar);
  // add the znode path of the registrar
  registrar.database.primaryPath = self.myPath;

  // first try to set the znode
  self.session.a_set(self.registrarPath, JSON.stringify(registrar),
                     -1, function(rc, msg, path)
  {
    // if znode dne, create the znode
    if (rc === ZooKeeper.ZNONODE) {
      log.info('previous registrar DNE, writing new registrar znode');
      self.session.a_create(self.registrarPath, JSON.stringify(registrar),
        ZooKeeper.ZOO_EPHEMERAL, function(rc, msg, path)
      {
        if (rc !== 0) {
          var err = {
            rc: rc,
            msg: msg,
            path: path
          };
          log.error('unable to write registrar', err);
          self.emit('error', err);
          return callback(err);
        }

        log.debug('successfully created registrar', path);
        self.shardInfo = registrar.database;
        return callback(null);
      });
    } else if (rc !== 0) {
      var err = {
        rc: rc,
        msg: msg,
        path: path
      };
      log.error('unable to write registrar', err);
      self.emit('error', err);
      return callback(err);
    } else {
      log.debug('successfully updated registrar', path);
      self.shardInfo = registrar.database;
      return callback(null);
    }
  });
};

/**
 * Initializes the zookeeper session. emits 'zkinit' when done.
 */
function initZooKeeper(self) {
  var log = self.log;
  log.info('initializing zookeeper');

  // initialize ZK
  self.session = new ZooKeeper(self.zkCfg);
  self.session.connect(function(err) {
    if (err) {
      log.error('unable to connect to zookeeper');
      return self.emit('error', err);
    }
    return self.emit('zkinit');
  });
}

/**
 * Joins this peer in the shard upon initialization.
 *
 * TODO: Race condition where we dont set the watch on the registrar before we
 * see if we are the standby. the registrar watch never fires and we are stuck
 * in stdby state. This is solved currently by setting high timeouts in SMF such
 * that we always wait 30 seconds before starting, hence always ensuring that 
 * the registrar node exists.
 * 
 */
function joinShard(self) {
  var log = self.log;
  log.info('joining shard', self.shardPath);

  /**
   * 1) create an emphemeral-sequence znode under path + / + shard-
   * 2) get peers under shard.
   * 3) determine if primary.
   * 4) if primary, get list of standbys, emit primary event and finish.
   * 5) if not primary, set watch on registrar znode.
   * 6) emit standby event and finish.
   */
  createZnode(self, self.pathPrefix, self.url, ZTYPE_SEQ_EPH,
              function(err, path)
  {
    if (err) {
      return self.emit('error', err);
    }
    // don't set the watch while fetching peers for the first time. We don't 
    // want to be interrupted. The watch gets set after role determination.
    getPeers(self, self.shardPath, null,  function(err, peers) {
      if (err) {
        return self.emit('error', err);
      }
      determineRole(self, peers, function(err) {
        if (err) {
          return self.emit('error', err);
        }
        // either leader or standby
        if (self.role === ROLE.PRIMARY) { // leader
          // get the standbys, and return primary event
          getStandbys(self, shardWatch, function(err, standbys) {
            if (err) {
              return self.emit('error', err);
            }
            self.standbys = standbys;
            log.info('primary initialized, emiting primary event');
            return self.emit('primary', standbys);
          });
        } else { // standby
          // if standby, then the registrar must exist, hence errNoExist set to
          // true.
          watchRegistrar(self, regWatch, true, function(err, shardInfo) {
            if (err) {
              return self.emit('error', err);
            }

            self.shardInfo = shardInfo;
            return self.emit('standby', shardInfo);
          });
        }
      });
    });
  });

  // various watches

  /**
   * This is a no-op if the current role is STANDBY since primary failures are
   * indicated by watches on the registrar node.
   *
   * If the current role is PRIMARY we merely
   * observe the number of peers in a shard and make no decisions on standbys.
   * 1) get all peers in the shard.
   * 2) update standbys list.
   * 3) emit updated standbys list event such that the consumer can update
   * configs. Note we rely on the consumer to update the registrar after
   * initialization. As only the consumer can determine the shard state. There
   * could be no updates to the registrar as a result of this action.
   */
  function shardWatch(type, state, path) {
    var log = self.log;
    log.info('watch fired, shard membership changed!', type, state, path,
             self.url);
    if (self.role === ROLE.PRIMARY) {
      log.info('primary checking shard status');
      getStandbys(self, shardWatch, function(err, peers) {
        if (err) {
          log.error('error while getting standbys');
          self.emit('error', err);
        }
        peers.sort(compare);
        prevStandbys = self.standbys;
        self.standbys = peers;
        if (JSON.stringify(prevStandbys) === JSON.stringify(peers)) {
          log.info('standby membership not changed, returning');
          return;
        }
        log.debug('updated standbys', self.standbys);
        self.emit('standby-change', self.standbys);
      });
    }
  }

  /**
   * This is a no-op if the current role is Primary.
   *
   * If the current role is STANDBY we:
   * 1) check that the new znode.
   * 2) if the primary has changed from before, we inform the consumer.
   * 3) if the znode has been deleted, we need to leader elect based on the
   * previous state of the shard, the sync becomes the primary.
   * 4) if primary, set role = primary, emit event, and write registrar
   */
  function regWatch(type, state, path) {
    var log = self.log;
    log.info('reg watch fired', type, state, path);
    // add the rewatch
    watchRegistrar(self, regWatch, false, function(err, shardInfo) {
      if (err) {
        return self.emit('error', err);
      }

      var prevPrimary = self.shardInfo.primary;
      var prevSync = self.shardInfo.sync;
      if (shardInfo) {
        self.shardInfo = shardInfo;
      }

      if (self.role === ROLE.STANDBY) {
        switch(type) {
          // inform consumer of the new primary if it's changed.
          case ZooKeeper.ZOO_CREATED_EVENT:
          case ZooKeeper.ZOO_CHANGED_EVENT:
          // child event incase we were watching the parent znode
          case ZooKeeper.ZOO_CHILD_EVENT:
            if (prevPrimary !== shardInfo.primary) {
              log.info('primary changed from %s to %s', prevPrimary,
                       shardInfo.primary);
              return self.emit('primary-change', shardInfo);
            }
            break;
          case ZooKeeper.ZOO_DELETED_EVENT:
            // primary died, elect new primary
            log.info('reg info deleted, elect new primary, ' +
                     'previous sync standby was %s', prevSync);
            if (prevSync && (prevSync === self.url)) {
              // return primary status to consumer
              log.info('you are now the primary', self.myPath);
              self.role = ROLE.PRIMARY;
              getStandbys(self, shardWatch, function(err, standbys) {
                if (err) {
                  log.error('unable to get standbys');
                  return self.emit('error');
                }
                self.standbys = standbys;
                self.emit('primary', standbys);
              });
            }
            break;
          default:
            break;
        }
      }
    });
  }
}

/**
 * Watches for changes with the registrar info. Watches on the prefix path
 * if the registrar DNE such that we can catch when the registrar gets created.
 *
 * @param {boolean} errOnNotExist Determines whether to return error if the
 * registrar dne.
 * @param {function} watch The zk watch to invoke when changes occur.
 * @param {function} callback The callback of the form f(err, registrar)
 */
function watchRegistrar(self, watch, errOnNotExist, callback) {
  var log = self.log;
  log.info('getting shard information as standby', self.myPath);

  self.session.aw_get(self.registrarPath, watch,
                      function(rc, msg, stat, data)
  {
    // set a watch even if the znode dne
    if (rc === ZooKeeper.ZNONODE && !errOnNotExist) {
      log.info('shardinfo dne, watching registrar parent',
               self.registrarPathPrefix);
      // if the shardInfo doesn't exist, watch the parent prefix for changes
      getPeers(self, self.registrarPathPrefix, watch, function(err, shards) {
        if (err) {
          log.error('error while trying to get parent registrar path');
          return callback(err);
        }
        shards.forEach(function(path) {
          log.debug('got registrars', path);
          // if my registrar exists, watch it and return.
          if (path === self.registrarPath) {
            log.debug('got a registrar', path);
            watchRegistrar(self, watch, false, function(err, shardInfo) {
              return callback(err, shardInfo);
            });
          }
        });
      });
      return callback();
    }
    if (rc !== 0) {
      var err = {
        rc: rc,
        msg: msg,
        stat: stat,
        myPath: self.myPath
      };
      log.error('unable to get shard registrar info', err);
      return callback(err);
    }

    log.debug('got shard registrar info', data);
    return callback(null, JSON.parse(data).database);
  });
}

/**
 * Determines the role of self in the shard.
 * You are the primary if:
 * 1) If you are the only peer in the shard.
 * 2) If there are other peers, and you have the lowest sequence.
 *
 * In addition to returning the your role, self.role is also set.
 *
 * @param {array} The paths of all peers within the shard.
 * @param {function} callback The callback of the form f(err, role).
 */
function determineRole(self, peers, callback) {
  var log = self.log;
  log.info('determining leader given peers', peers);

  peers.sort(compare);

  var myPath = self.myPath.substr(self.myPath.lastIndexOf('/') + 1);
  log.debug('peer order', peers, myPath);
  // you are the leader
  if (peers[0] === myPath) {
    log.debug('you are the leader');
    self.role = ROLE.PRIMARY;
  } else {
    log.debug('you are a standby');
    self.role = ROLE.STANDBY;
  }
  return callback(null, self.role);
}

/**
 * Called only by the primary
 */
function getStandbys(self, watch, callback) {
  var log = self.log;
  log.info('getting standbys in shard');
  getPeers(self, self.shardPath, watch, function(err, peers) {
    if (err) {
      log.error('error getting standbys', err);
      return callback(err);
    }

    log.info('got peers', peers, self.myPath);
    var standbys = [];

    if (peers.length === 1 && peers[0] === self.myRelativePath) {
      log.warn('lone peer in shard, no standbys');
      return callback(null, standbys);
    }
    // can't use forEach as js doesn't support continue. sigh
    for (var i = 0; i < peers.length; i++) {
      var peer = peers[i];
      // skip self
      if (peer === self.myRelativePath) {
        log.debug('skipping self', peer);
        continue;
      }
      var peerPath = self.shardPath + '/' + peer;
      log.debug('getting standby urls', peerPath);
      self.session.a_get(peerPath, false, function(rc, msg, stat, data) {
        if (rc !== 0) {
          var err = {
            rc: rc,
            msg: msg,
            stat: stat
          };
          log.error('unable to get standby data', err, peer);
          self.emit('error', err);
          return callback(err);
        }
        log.debug('got standby', data);
        standbys.push(data);
        if (standbys.length === (peers.length - 1)) {
          log.info('got all standbys', standbys);
          return callback(null, standbys);
        }
      });
    }
  });
}

/**
 * Get all peers in the shard.
 *
 * @param {string} path The path of the shard.
 * @param {function} watch The zk watch to set on the shard.
 * @param {function} callback The callback of the form f(err, peers)
 */
function getPeers(self, path, watch, callback) {
  var log = self.log;
  log.info('getting children under path', path);

  if (watch) {
    self.session.aw_get_children2(path, watch,
                                  function(rc, msg, children, stat)
    {
      if (rc !== 0) {
        var err= {
          rc: rc,
          msg: msg,
          path: self.myPath
        };
        self.emit('error', err);
        return callback(err);
      }

      log.debug('got children under path %s mypath %s', path, self.myPath,
                children);
      var initCount = 0;

      self.emit('children', children);
      return callback(null, children);
    });
  } else {
    self.session.a_get_children2(path, false, function(rc, msg, children, stat)
    {
      if (rc !== 0) {
        var err= {
          rc: rc,
          msg: msg,
          path: self.myPath
        };
        self.emit('error', err);
        return callback(err);
      }

      log.debug('got children under path %s mypath %s', path, self.myPath,
                children);
      var initCount = 0;

      self.emit('children', children);
      return callback(null, children);
    });
  }
}

/**
 * Create a znode representing myself under the shard.
 * Sets self.myPath and self.myRelativePath when done.
 * @param {function} callback The callback of the form f(err, path)
 */
function createZnode(self, path, data, znodeType, callback) {
  var log = self.log;
  log.info('creating znode', path, data, znodeType);

  self.session.a_create(path, data, znodeType, function(rc, msg, myPath) {
    if (rc !== 0) {
      var err = {
        rc: rc,
        msg: msg,
        path: myPath
      };
      log.error('unable to create znode', err);
      self.emit('error', err);
      return callback(err);
    }

    log.info('created znode', myPath);
    self.emit('znode', myPath);
    self.myPath = myPath;
    self.myRelativePath = self.myPath.substr(self.myPath.lastIndexOf('/') + 1);
    return callback(null, myPath);
  });
}

/**
 * Compares two lock paths.
 * @param {string} a the lock path to compare.
 * @param {string} b the lock path to compare.
 * @return {int} Positive int if a is bigger, 0 if a, b are the same, and
 * negative int if a is smaller.
 */
function compare(a, b) {
  var seqA = parseInt(a.substring(a.lastIndexOf('-') + 1), 10);
  var seqB = parseInt(b.substring(b.lastIndexOf('-') + 1), 10);

  return seqA - seqB;
}

