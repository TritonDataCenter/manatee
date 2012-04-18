var EventEmitter = require('events').EventEmitter;
var Peer = require('./peer');
var common = require('./common');
var util = require('util');
var ZooKeeper = require('zookeeper');

/**
 * The postfix to the ephemeral znode under shardid/
 */
var POSTFIX = 'shard-';

var ZNODE_TYPE = ZooKeeper.ZOO_SEQUENCE | ZooKeeper.ZOO_EPHEMERAL;

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
function Shard(path, registrarPath, zkCfg, url, log) {
  EventEmitter.call(this);
  log.debug('new shard with', path, registrarPath, zkCfg, url);
  this.log = log;

  /**
   * The path under which znodes for this shard are stored
   */
  this.path = path;

  /**
   * The path which the registrar info is stored
   */
  this.registrarPath = registrarPath;
  /**
   * The zk cfg
   */
  this.zkCfg = zkCfg;

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
   * The url of this peer
   */
  this.url = url;

  /**
   * This peer's current role
   */
  this.role = null;

  /**
   * The path of this Peer's znode in ZK
   */
  this.myPath = null;

  /**
   * The number of peers in the current shard
   */
  this.peerCount = 0;
}

/**
 *
 */
module.exports = Shard;
util.inherits(Shard, EventEmitter);


/**
 * Initializes the current Peer into the shard.
 */
Shard.prototype.init = function init() {
  var self = this;
  var log = self.log;

  // initialize ZK session
  log.info('establishing session to zk with cfg', self.zkCfg);
  self.session = new ZooKeeper(self.zkCfg);
  self.session.connect(function(err) {
    if (err) {
      return self.emit('error', err);
    }

    log.debug('initializing shard with path ', self.path);
    // create your own peer first
    self.session.a_create(self.path + '/' + POSTFIX, self.url, ZNODE_TYPE,
      function(rc, msg, myPath) {
        if (rc !== 0) {
          var error = {
            rc: rc,
            msg: msg,
            path: myPath
          };
          return self.emit('error', error);
        }

        // set my own znode path
        self.myPath = myPath;

        self.elect();
    });
  });
};

/**
 * Scans the Shard and resets the state on any changes to peers in the shard
 * An init event is emitted when election is complete.
 * A role event is emitted when roles have been reassigned. Note this does not
 * mean the shard is ready, we may still not have the urls of the new nodes
 *
 * @param {int} type The ZK type of the watch event.
 * @param {int} state The ZK state of the sessison.
 * @param {string} path The path of the znode.
 */
Shard.prototype.elect = function elect(type, state, path) {
  // What about when you lose connection to ZK? If that occurs, we suicide.
  if (state < 0) {
    log.error('zk session failed with state ' + state);
    var err = {
      msg: 'zk session failed',
      state: state
    };
    self.emit('error', err);
  }

  var self = this;
  var log = self.log;
  log.debug('entering elect');
  var myPath = self.myPath;
  var peers = [];

  // Note that we set the watch again on elect.
  this.session.aw_get_children2(self.path, watch,
                                function(rc, msg, children, stat) {
    if (rc !== 0) {
      var error = {
        rc: rc,
        msg: msg,
        path: myPath
      };
      return self.emit('error', error);
    }

    log.debug('got children', children);
    var initCount = 0;

    // for each child znode, get its data
    children.forEach(function(znode) {
      log.debug('creating peer', znode);
      var p = self.path + '/' + znode;
      // create the peer and push it into the array
      var peer = new Peer.Peer(p, self.session, log, function(err, p) {
        if (err) {
          log.error('error while creating peer', p, err);
          return self.emit('error', err);
        }

        initCount++;
        self.log.debug('created peer', peer.path, initCount);

        // if the data for all children have been gotten, emit init event
        if (initCount == children.length) {
          // Set registrar info if primary, then emit init event
          if (self.role === common.ROLES.PRIMARY) {
            log.info('setting registrar data');
            self.writeRegistrar(function(rc, msg, path) {
              if (rc !== 0) {
                var error = {
                  rc: rc,
                  msg: msg,
                  path: path
                };
                log.error('error writing registrar', error);
                return self.emit('error', error);
              }

              log.debug('updated registrar info', msg, path);
              log.info('shard initialized', peers);
              self.peerCount = initCount;
              self.emit('init', self);
            });
          } else {
            log.info('shard initialized', peers);
            self.peerCount = initCount;
            self.emit('init', self);
          }
        }
      });
      //else just push the peer into the array
      peers.push(peer);
    });


    log.debug('sorting peers ', peers);

    // sort
    peers.sort(Peer.compare);

    //TODO: there might be race conditions here if the sort doesn't finish bef
    //fore the peers are initialized

    /**
    * The peer with the lowest sequence is the primary.
    * The peer with the second lowest sequence is the sync standby.
    * The peer with the lowest sequence is the async standby.
    */
    for (i = 0; i < peers.length; i++) {
      switch (i) {
        case 0:
          self.primary = peers[i];
          if (peers[i].path === myPath) {
            self.role = common.ROLES.PRIMARY;
          }
          break;
        case 1:
          self.sync = peers[i];
          if (peers[i].path === myPath) {
            self.role = common.ROLES.SYNC;
          }
          break;
        case 2:
          self.async = peers[i];
          if (peers[i].path === myPath) {
            self.role = common.ROLES.ASYNC;
          }
          break;
        default:
          log.error('more than 3 children under shard');
          var err = {
            msg: 'more than 3 children under shard'
          };
          return self.emit('error', err);
      }
    }

    /**
    * GC dead peers, i.e. peers that were not part of children, these peers
    * have expired.
    *
    * size can't be 0 since there's at least one peer, us.
    * if size is 1, then we are the primary, we remove everyoe else, if size
    * is 2, then we remove async, if 3, we do nothing
    */
    switch (children.length) {
      case 1:
        self.sync = null;
        self.async = null;
        break;
      case 2:
        self.async = null;
        break;
      case 3:
        break;
      default:
        log.error('incorrect number of children %s under shard',
        children.length);
        var err = {
          msg: 'incorrect number of children %s under shard'
        };
        self.emit('error', err);
    }

    log.info('emitting role');
    // inform consumers that roles were updated
    self.emit('role', self);
  });

  function watch(type, state, path) {
    log.debug('parent watch fired', type, path);
    self.elect();
  }
};

Shard.prototype.writeRegistrar = function writeRegistrar(callback) {
  var self = this;
  var log = self.log;
  log.debug('entering write registrar');

  var hosts = JSON.stringify({
    primary: self.primary.url,
    sync: self.sync ? self.sync.url : null,
    async: self.async ? self.async.url : null
  });

  var regPath = self.registrarPath + self.path + '-';
  log.debug('writing registrar', regPath, hosts);
  //write registrar as follows registrarpath/shardid-#
  self.session.a_create(regPath, hosts, ZNODE_TYPE, callback);
};

/**
 * Disconnects from ZK. Emits a 'disconnect' event when done
 */
Shard.prototype.disconnect = function disconnect() {
  var self = this;
  self.log.info('disconnecting session with zookeeper');
  self.session.close();
  self.emit('disconnect');
};
