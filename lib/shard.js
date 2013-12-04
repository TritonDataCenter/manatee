/**
 * @overview The Manatee Coordinator that manages each node in its shard.
 * @copyright Copyright (c) 2013, Joyent, Inc. All rights reserved.
 *
 *                   _.---.._
 *      _        _.-' \  \    ''-.
 *    .'  '-,_.-'   /  /  /       '''.
 *   (       _                     o  :
 *    '._ .-'  '-._         \  \-  ---]
 *                  '-.___.-')  )..-'
 *                           (_/
 */
var assert = require('assert-plus');
var backoff = require('backoff');
var bunyan = require('bunyan');
var EventEmitter = require('events').EventEmitter;
var HeartbeatServer = require('./heartbeatServer');
var HeartbeatClient = require('./heartbeatClient');
var once = require('once');
var path = require('path');
var PostgresMgr = require('./postgresMgr');
var TransitionQueue = require('./transitionQueue');
var url = require('url');
var util = require('util');
var uuid = require('node-uuid');
var vasync = require('vasync');
var verror = require('verror');
var zkplus = require('zkplus');
var ZK = require('zookeeper');

/**
 * Logical Design
 * This object represents a Postgres node in a shard. A shard is a set of
 * Postgres servers setup in a daisy chain.
 *
 * +-------+     +-------+     +-------+     +-------+
 * |       |     |       |     |       |     |       |
 * |primary|     | sync  |     |async-1|     |async-n|
 * |       +---->|       +---->|       | ... |       |
 * |       |     |       |     |       |     |       |
 * +-------+     +-------+     +-------+     +-------+
 *
 * Each node only knows about the node directly in front and behind it, and
 * does not know about the entire topology of the shard. This is by design as
 * it drastically simplifies the state each node has to keep. All nodes behave
 * the same way with the exception of the first node in the shard, the primary
 * -- which knows about its special status as the first node in the chain.
 *
 * The generic zookeeper leader election algorithm:
 * http://zookeeper.apache.org/doc/trunk/recipes.html#sc_leaderElection is used
 * to keep track of nodes in front and whether the node is the primary via two
 * events {leader, newLeader} from the zkplus generic-election object.
 *
 * 'leader' events implies that this current node is now the head of the daisy
 * chain.
 *
 * 'newLeader' event contains the URL of the new leader, and implies that the
 * node in front of the current node may have changed.
 *
 * HTTP heartbeats are used to keep track of nodes directly behind. Each node
 * will send periodic heartbeats to the node directly in front. Standby
 * nodes (nodes directly behind) are exposed via two events {newStandby,
 * expired} from the heartbeatServer object.
 *
 * 'newStandby' events contain the URL of the new standby node and implies that
 * the node behind may have changed.
 *
 * 'expired' events contain the URL of an expired standby node, and implies
 * that the standby node is no longer in the shard.
 *
 * Given that there are 4 possible state transitions of each node, let's look
 * at them in detail.
 *
 * 1) A non-primary node dies.
 *
 * Suppose the sync node in the daisy chain above dies, the primary will be
 * notified of its death by an 'expired' event, and the async-1 will be
 * notified by a 'newLeader' event. The async-1 will sending
 * heartbeats to the node in front, which will result in a 'newStandby' event
 * to the node in front. The dead node is no longer in the chain.
 *
 * 2) The primary node dies.
 *
 * If the primary node dies, since there are no node directly in front of it,
 * the sync node will only receive a 'leader' event, signaling it that it is
 * now the primary node of the shard. No other events are emitted on any other
 * node.
 *
 * 3) A new node joins the shard.
 *
 * New nodes will always join a shard at the tail end of the chain. If the
 * chain is empty, the new node will become the primary node and will receive
 * 'leader' event. Otherwise, the new node will receive a 'newLeader' event,
 * with the URL of the node directly in front of it. The node directly in front
 * will receive a 'newStandby' event, with the URL of the new node.
 *
 * Each node in a shard uses Postgres streaming replication to replicate to its
 * immediate successor. The first and second node in the chain will always use
 * synchronous replication to guarantee data consistency. Nodes thereafter will
 * use asynchronous replication.
 *
 * Each node uses postgres streaming replication to replicate data to the node
 * behind it. The 1st and 2nd nodes use synchronouse replication, and all other
 * nodes use asynchronous replication. Synchronous replication is used to
 * ensure data consistency, since data will be written to both primary and sync
 * before the request is returned to the client. All other nodes use
 * asynchronous replication, upon the death of the primary node in the shard,
 * the next two nodes which were previously using asynchronous replication will
 * switch over to synchronous replication.
 *
 * There is no practical limit on the size of the daisy chain, however, beyond
 * 2 nodes, there is not much benefit in terms of consistency, and beyond 3
 * nodes, there is not much benefit in terms of availability. However, having
 * more than 3 is useful if re-sharding is needed -- that is, moving some
 * subset of the Postgres data to another node.
 *
 *
 * @constructor
 *
 * @param {object} options Options object.
 * @param {Bunyan} options.log Bunyan logger.
 * @param {number} options.backupPort The port used across the shard for the pg
 * backup server.
 * @param {number} options.ttl The ttl of the registrar node.
 * @param {object} options.HeartbeatClientCfg The heartbeat client config.
 * @param {object} options.HeartbeatServerCfg The heartbeat server config.
 * @param {object} options.PostgresMgrCfg The postgres manager config.
 * @param {object} options.zkCfg Zk client cfgs.
 * @param {string} options.shardPath The path under which znodes for this shard
 * are stored.
 * @param {string} options.ip The IP of this node.
 * @param {string} options.zoneId The UUID of the zone this node runs on.
 */
function Shard(options) {
    assert.object(options, 'options');
    assert.number(options.backupPort, 'options.backupPort');
    assert.number(options.ttl, 'options.ttl');
    assert.object(options.heartbeatClientCfg, 'options.heartbeatClientCfg');
    assert.object(options.heartbeatServerCfg, 'options.heartbeatServerCfg');
    assert.object(options.log, 'options.log');
    assert.object(options.postgresMgrCfg, 'options.postgresMgrCfg');
    assert.object(options.zkCfg, 'options.zkCfg');
    assert.string(options.shardPath, 'options.shardPath');
    assert.string(options.ip, 'options.ip');
    assert.string(options.zoneId, 'options.zoneId');

    var self = this;

    /** @type {Bunyan} The bunyan log object */
    this._log = options.log;

    /**
     * @type {string} Path under which shard metadata such as elections are
     * stored. e.g. /manatee/com.joyent.1
     */
    this._shardPath = options.shardPath;

    /** @type {string} Path under which elections for this shard are held */
    this._shardElectionPath = options.shardPath + '/election';

    /** @type {string} Znode that indicates an error with the shard */
    this._shardErrorPath = options.shardPath + '/error';

    /** @type {string}  The znode under which stores the history of the shard */
    this._shardHistoryPath = options.shardPath + '/history';

    /** @type {string} The uuid of this smartos zone */
    this._zoneId = options.zoneId;

    /**
     * @type {string} The registrar path, since the shardPath is usually
     * /prefix/com.joyent.foo.bar.baz, we strip out the first /prefix replace .
     * with / and append /pg
     *
     * This is the zookeeper object used by clients to discover the topology of
     * this shard
     */
    this._registrarPathPrefix =
    '/' + (self._shardPath.substring(self._shardPath.indexOf('/', 1) + 1))
    .split('.').reverse().join('/');

    this._registrarPath = this._registrarPathPrefix + '/pg';

    /** @type {number} The ttl of the registrar node in ms */
    this._ttl = options.ttl;

    /** @type {string} The pgUrl of the most recent leader */
    this._leader = null;

    /** @type {Object} The zk cfg */
    this._zkCfg = options.zkCfg;
    self._zkCfg.log = self._log.child({
        component: 'zookeeper',
        level: 'trace'
    });
    self._zkCfg.log.level('trace');

    /** @type {string} The IP of this peer, ex. 10.99.99.16 */
    this._ip = options.ip;

    /** @type {PostgresMgr} The postgres manager */
    this._postgresMgr = new PostgresMgr(options.postgresMgrCfg);

    this._heartbeatServerCfg = options.heartbeatServerCfg;
    /** @type {HeartbeatServer} The heartbeat server that listens for standby */
    this._heartbeatServer = new HeartbeatServer(self._heartbeatServerCfg);
    /** @type {HeartbeatClient} The heartbeat client */
    this._heartbeatClient = null;

    //TODO: move hbc creating to a createclietn() call
    /** @type {object} The sample heartbeat client cfg. */
    this._heartbeatClientCfg = options.heartbeatClientCfg;

    /** @type {number} The port used by the pg backup service */
    this._backupPort = options.backupPort;

    /** @type {string} The url of the current standby of this node */
    this._currStandby = null;

    /**
     * @type {TransitionQueue} The queue used to manage transition events such
     * that they occur serially and automically.
     */
    this._transitionQueue = new TransitionQueue({
        log: self._log
    });

    self._transitionQueue.on('execError', function (err) {
        self._log.error({
            err: err
        }, 'Shard: got transition queue execution error');

        throw new verror.VError(err);
    });

    /** @type {zkplus.election} The ZK election */
    this._election = null;
    /** @type {zkplus.client} The ZK client */
    this._zk = null;

    _init(self, function (err) {
        if (err) {
            throw new verror.VError(err, 'unable to initialize shard');
        }
    });
}

module.exports = Shard;

/**
 * @callback Shard-cb
 * @param {Error} err
 */

/*
 * Transition functions.
 *
 * Since each node can get more than 1 state transition event at one time (if
 * more than one node dies in the shard), it's really important that state
 * transitions are processed serially. Thus events are enqueued in a queue and
 * processed in order, one at a time.
 *
 * Each state transition has a corresponding function handler below, which
 * encapsulates the steps of the transition.
 */

/**
 * Take over as the primary of the shard. We are the node that will be taking
 * writes. As such, we need to satisfy the following conditions before we can
 * become the primary.
 *
 * If we were the primary -- check to ensure that the last primary in the shard
 * was us.
 * If we were the sync -- check to ensure that the last primary in the shard
 * was our primary.
 * If we were the async, or we don't satisfy the previous 2 conditions, wait
 * until another node enters the shard and rejoin the election.
 *
 * @param {object} ctx The context associated with this transition.
 * @param {object} ctx.self The shard object.
 * @param {Shard-cb} callback
 */
function _leader(ctx, callback) {
    var self = ctx.self;
    var postgresMgr = self._postgresMgr;
    var log = self._log;
    self._leader = null;
    log.info({currStandby: self._currStandby}, 'Shard._leader: entering');

    var tasks = [
        function stopHeartBeat(_, cb) {
            if (self._heartbeatClient) {
                log.info('stopping heartbeat');
                self._heartbeatClient.stop();
            }

            return cb();
        },
        function canBeLeader(_, cb) {
            _canBeMaster(self, function (err, can) {
                if (err) {
                    return cb(new verror.VError(err));
                }

                if (!can) {
                    /*
                     * first stop checking sync state, but ignore errors, don't
                     * delete the previous sync state until we've actually
                     * become the primary.
                     */
                    self._postgresMgr.stopSyncChecker(false, function () {
                        log.info({can: can}, 'Shard._leader: was async, ' +
                            'aborting _leader and waiting for other peers');
                        _waitForOtherPeers(ctx, callback);
                        return;
                    });
                } else {
                    return cb();
                }
            });
        },
        function primary(_, cb) {
            log.info('_leader; going to pg primary mode');
            postgresMgr.primary(self._currStandby, cb);
        },
        function writeRegistrar(_, cb) {
            log.info('_leader; writing registrar');
            _writeRegistrar(self, cb);
        },
        function writeHistoryNode(_, cb) {
            log.info('_leader; writing historyNode');
            var opts = {
                standby: self._currStandby,
                role: 'Leader',
                action: 'AssumeLeader'
            };
            _writeHistoryNode(self, opts, cb);
        }
    ];

    vasync.pipeline({funcs: tasks, arg: {}}, function (err) {
        log.info({
            err: err,
            currStandby: self._currStandby
        }, 'Shard._leader: finished.');

        if (err && err.__walCorruption) {
            log.error({
                err: err
            }, '_leader: got wal corruption');
            _writeErrorNode(self, self._currStandby, function (err2) {
                if (err2) {
                    return callback(new verror.VError(err2,
                                                'unable to write error node'));
                }

                log.info('_leader: entering shardError state');
                _shardError(self);
                // don't return cb to stop any further state changes
                return (undefined);
            });
        } else if (err) {
            err = new verror.VError(err, 'unable to transition to primary');
            return callback(err);
        } else {
            log.info('_leader: successful transition');
            return callback();
        }

        return (undefined);
    });
}

/**
 * A new leader event signifies that there might be a new leader of the current
 * node. This implies that we are not the leader of the shard, and we must
 * start slaving from the new leader.
 *
 * @param {object} ctx The context associated with this transition.
 * @param {object} ctx.self The shard object.
 * @param {string} ctx.leader The IP of the new leader.
 * @param {Shard-cb} callback
 */
function _newLeader(ctx, callback) {
    var self = ctx.self;
    var log = self._log;
    var myLeader = ctx.leader;
    var pgUrl = _transformPgUrl(self, myLeader);
    var backupUrl = _transformBackupUrl(self, myLeader);
    var heartbeatUrl = _transformHeartbeatUrl(self, myLeader);
    self._leader = pgUrl;
    var postgresMgr = self._postgresMgr;
    log.info({
        leader: myLeader,
        primaryUrl: pgUrl,
        backupUrl: backupUrl,
        heartbeatUrl: heartbeatUrl
    }, 'Shard._newLeader: entering');

    var tasks = [
        function pgStandby(_, cb) {
            log.info({
                pgUrl: pgUrl,
                backupUrl: backupUrl
            }, 'Shard._newLeader: pg going to standby');

            postgresMgr.standby(pgUrl, backupUrl, cb);
        },
        function stopHeartBeat(_, cb) {
            if (self._heartbeatClient) {
                log.info('Shard._newLeader: stopping heartbeat');
                self._heartbeatClient.stop();
            }

            return cb();
        },
        function startHeartbeat(_, cb) {
            log.info({
                primaryUrl: heartbeatUrl,
                ip: self._ip
            }, 'Shard._newLeader: starting heartbeat');
            self._heartbeatClientCfg.primaryUrl = heartbeatUrl;
            self._heartbeatClient = new HeartbeatClient(
                self._heartbeatClientCfg);
            self._heartbeatClient.start(cb);
        },
        function writeHistory(_, cb) {
            log.info('Shard._newLeader: writing history');
            var opts = {
                standby: self._currStandby,
                role: 'Standby',
                leader: self._leader,
                action: 'NewLeader'
            };
            _writeHistoryNode(self, opts, cb);
        }
    ];

    vasync.pipeline({funcs: tasks, arg: {}}, function (err) {
        log.info({
            err: err,
            pgUrl: pgUrl,
            backupUrl: backupUrl
        }, 'Shard._newLeader: finished.');

        if (err)  {
            err = new verror.VError(err, 'Shard._newLeader: failed');
        }

        return callback(err);
    });
}

/**
 * A new standby event signifies that we may have a new standby -- this is only
 * relevant if we are the overall leader of the shard, as we'll need to change
 * our synchronous replication standby to point to the new standby. Otherwise,
 * we do nothing since the replication will be asynchronous and no configs need
 * to be updated on our side.
 *
 * @param {object} ctx The context associated with this transition.
 * @param {object} ctx.self The shard object.
 * @param {string} ctx.standby The IP of the new standby.
 * @param {Shard-cb} callback
 */
function _newStandby(ctx, callback) {
    var self = ctx.self;
    var log = self._log;
    var postgresMgr = self._postgresMgr;
    var standby = ctx.standby;
    var standbyUrl = _transformPgUrl(self, standby);

    var tasks = [
        function checkStandby(_, cb) {
            log.info({
                prevStandby: self._currStandby,
                standbyUrl: standbyUrl
            }, 'Shard._newStandby: checking standby status');

            if (standbyUrl === self._currStandby) {
                log.info('Shard._newStandby: standby not changed');
                _.skipPg = true;
                _.skipHistory = true;
            }

            self._currStandby = standbyUrl;
            if (!self._election.amLeader) {
                log.info({
                    currStandby: self._currStandby
                }, 'Shard._newStandby: not primary, ignoring standby ' +
                   'heartbeats, but updating currStandby');

                _.skipPg = true;
            }
            return cb();
        },
        function updateStandby(_, cb) {
            if (_.skipPg) {
                return cb();
            }
            log.info({
                currStandby: self._currStandby
            }, 'Shard._newStandby: updating pg standby');
            postgresMgr.updateStandby(standbyUrl, cb);

            return (undefined);
        },
        function writeHistory(_, cb) {
            if (_.skipHistory) {
                return cb();
            }
            log.info('Shard._newStandby: writing history');
            var opts = {
                standby: standbyUrl,
                role: self._election.amLeader ? 'Leader' : 'Standby',
                leader: self._leader,
                action: 'NewStandby'
            };
            _writeHistoryNode(self, opts, cb);

            return (undefined);
        }
    ];

    vasync.pipeline({funcs: tasks, arg: {}}, function (err) {
        log.info({
            err: err,
            prevStandby: self._currStandby,
            standbyUrl: standbyUrl
        }, 'Shard._newStandby: finished.');

        if (err && err.__walCorruption) {
            log.error({
                err: err,
                prevStandby: self._currStandby,
                standbyUrl: standbyUrl
            }, 'Shard._newStandby: got wal corruption');

            _writeErrorNode(self, self._currStandby, function (err2) {
                if (err2) {
                    return callback(new verror.VError(err2,
                                                'unable to write error node'));
                }

                log.info('Shard._newStandby: entering shardError state');
                _shardError(self);
                // don't return cb to stop any further state changes
                return (undefined);
            });
        } else if (err) {
            err = new verror.VError(err,
                'Shard._newStandby: unable to add new standby');
            return callback(err);
        } else {
            log.info('Shard._newStandby: successful transition');
            return callback();
        }

        return (undefined);
    });
}

/**
 * An expired event signifies that a standby has expired. If the expired
 * standby is the current standby, and we are the leader of the shard, then we
 * need to explicitly remove the standby from postgresql.conf, as it was
 * slaving synchronously. If we are not the leader, or the standby isn't the
 * current standby, there is nothing to do since in the former case, the
 * standby was an async and no config changes are required, and in the latter,
 * the standby isn't current, so regardless of whether we are the leader or
 * not, the standby isn't in the configs.
 *
 * We always write the history node if the standby is the current standby for
 * posterity.
 * @param {object} ctx The context associated with this transition.
 * @param {object} ctx.self The shard object.
 * @param {string} ctx.expiredStandby The IP of the expired standby.
 *
 * @param {Shard-cb} callback
 */
function _expired(ctx, callback) {
    var self = ctx.self;
    var postgresMgr = self._postgresMgr;
    var expiredStandby = ctx.expiredStandby;
    var log = self._log;
    var expiredStandbyUrl = _transformPgUrl(self, expiredStandby);
    var currStandby = self._currStandby;

    log.info({
        currStandby: self._currStandby,
        expiredStandby: expiredStandbyUrl,
        isLeader: self._election.amLeader
    }, 'Shard._expired: entering');

    var tasks = [
        function checkStandbyAndIsLeader(_, cb) {
            _.continue = false;
            if (expiredStandbyUrl === currStandby && self._election.amLeader) {
                log.info({
                    currStandby: currStandby,
                    expiredStandby: expiredStandbyUrl,
                    isLeader: self._election.amLeader
                }, 'Shard._expired: expired standby matches currStandby and ' +
                   'isLeader, removing expired standby');
                _.continue = true;
            }

            cb();
        },
        function removeStandby(_, cb) {
            if (!_.continue) {
                return cb();
            }
            log.info('removing expired standby');
            postgresMgr.updateStandby(null, cb);
            // fix for MANATEE-87 set remove currStandby if expired
            self._currStandby = null;

            return (undefined);
        },
        function writeHistory(_, cb) {
            if (expiredStandbyUrl === currStandby) {
                log.info('Shard._expired: writing history');
                var opts = {
                    role: self._election.amLeader ? 'Leader' : 'Standby',
                    leader: self._leader,
                    standby: expiredStandbyUrl,
                    action: 'ExpiredStandby'
                };
                _writeHistoryNode(self, opts, cb);
            } else {
                // don't write history if a non-current standby expires, there's
                // no point.
                log.info({
                    currStandby: currStandby,
                    expiredStandby: expiredStandbyUrl,
                    isLeader: self._election.amLeader
                }, 'Shard._expired: not writing history, standby isn\'t ' +
                   'current');
                return cb();
            }

            return (undefined);
        }
    ];

    vasync.pipeline({funcs: tasks, arg: {}}, function (err) {
        if (err) {
            err = new verror.VError(err, 'Shard._expired failed');
        }

        log.info({
            err: err,
            currStandby: self._currStandby,
            expiredStandby: expiredStandbyUrl,
            isLeader: self._election.amLeader
        }, 'Shard._expired: finished');

        return callback(err);
    });
}

/**
 * #@+
 * @private
 * @memberOf Shard
 */

/**
 * When a transition to leader occurs -- and we were previously an asynchronous
 * peer or an expired primary or sync, we _must_ not become the leader and wait
 * for another node to enter the shard first. Since this peer's data can't be
 * guaranteed to be up to date.  If we become the leader, other peers in the
 * shard who used to be the sync or primary will never be able to slave from
 * the async. This will cause the shard to go into the r/o error state. We want
 * to avoid this, and so if our previous state was that of an async, then we
 * wait for another node to join before we try and join the shard.
 *
 * This function is invoked after we've joined the shard -- which complicates
 * things. There could be other events in the queue and currently executing.
 * This event should pre-empt any other events currently in the transition
 * queue -- however, it is unsafe to stop a transition in the middle. We assume
 * this event will always run on the queue when the "leader" event is executed
 * and we were preeviously an async. All previously events on the queue can
 * execute safely since if and when a "leader" event is executed, it will call
 * into this function. Once this function is running and on the queue, we do
 * the following:
 *
 * Close the current ZK connection -- effectively we want to leave the
 * election.
 * Pause the queue and prevent it from receiving other events.
 * Clear any remaining elements in the queue.
 * Once the zk connection has been closed, wait until another node joins the
 * shard.
 * Once another node joins the shard, join the election and resume the queue.
 *
 * @param {object} ctx The context associated with this transition.
 * @param {object} ctx.self The shard object.
 * @param {Shard-cb} callback
 */
function _waitForOtherPeers(ctx, callback) {
    var self = ctx.self;
    var log = self._log;

    log.info('Shard_.waitForOtherPeers: entering');

    self._zk.once('close', function () {
        vasync.pipeline({funcs:  [
            function _createZk(_, _cb) {
                _cb = once(_cb);
                _createZkClient(self._zkCfg, function (err, zkClient) {
                    if (err) {
                        self._log.fatal({err: err}, 'unable to connect to zk');
                        return _cb(err);
                    }
                    self._zk = zkClient;

                    self._zk.on('error', function (err4) {
                        self._log.fatal({err: err4}, 'zk error, exiting');
                        throw err4;
                    });

                    return _cb();

                });
            },
            function _initAndCleanup(_, _cb) {
                _zkInitAndCleanupZkNodes(self, _cb);
            },
            function _checkWatchErr(_, _cb) {
                _checkAndWatchError(self, _cb);
            },
            function _waitForAnotherNode(_, cb) {
                self._log.info('Shard._waitForOtherPeers: waiting for other ' +
                    'peers');
                self._zk.watch(self._shardElectionPath, {method: 'list'},
                         function (err, listn) {
                    cb = once(cb);
                    if (err) {
                        return cb(err);
                    }

                    listn.once('error', function (_err) {
                        return cb(_err);
                    });

                    listn.once('children', function (children) {
                        self._log.info({
                            peers: children
                        }, 'Shard._waitForOtherPeers: got other peers');
                        return cb();
                    });

                    /*
                     * we can't take the risk that someone joined before we set
                     * the watch, so we also do a list after we set the watch,
                     * if either returns someone in the election, then we can
                     * stop waiting.
                     */
                    self._zk.readdir(self._shardElectionPath,
                        function (_err, nodes)
                    {
                        log.trace({err: _err, nodes: nodes},
                        'Shard_.waitForPrimary: returned from zk list');
                        if (_err) {
                            return cb(_err);
                        }

                        if (nodes.length > 0) {
                            self._log.info({
                                peers: nodes
                            }, 'Shard._waitForOtherPeers: got other peers');
                            return cb();
                        }
                    });
                });
            },
            function _joinElection(_, _cb) {
                self._log.info('rejoining election');
                self._election = zkplus.createGenericElection({
                    client: self._zk,
                    path: self._shardElectionPath,
                    pathPrefix: self._ip,
                    object: {
                        zoneId: self._zoneId,
                        ip: self._ip,
                        pgUrl: self._postgresMgr.getUrl().href,
                        backupUrl: self._backupUrl
                    }
                });
                // set the electionlisteners since this election object is new.
                _setZKElectionListeners(self);
                /*
                 * resume the queue once this is complete so we can process
                 * events again.
                 */
                self._transitionQueue.resume();

                return _cb();
            },
            function _vote(_, _cb) {
                self._election.vote(function (err) {
                    if (err) {
                        err = new verror.VError(err);
                    }
                    return _cb(err);
                });
            }
        ]}, function (err) {
            log.info('Shard_.waitForPrimary: exiting');
            return callback(err);
        });
    });

    // Unregister from the election.
    self._zk.close();
    /*
     * pause the queue and clear it to prevent any other events other than the
     * wait to enter the queue.
     */
    self._transitionQueue.pause();
    self._transitionQueue.clear();
    return (undefined);
}

/**
 * @callback Shard-canBeMasterCb
 * @param {Error} err
 * @param {bool} canBeMaster
 */

/**
 * Checks whether this current node can be the leader of this shard.  It's
 * never safe to assume we can be the leader regardless of our previous role.
 *
 * If we were a leader, we need to check that we were in fact the last known
 * leader of the shard.
 * If we were a sync, we need to check that our primary was the last leader of
 * the shard.
 * If we were an async, we can not become the leader.
 * If we never persisted a role, then we are in bootstrapping mode, and it's
 * safe to become the master.
 *
 * @param {object} self The shard object.
 * @param {Shard-canBeMasterCb} callback
 */
function _canBeMaster(self, cb) {
    var log = self._log;
    log.info('Shard._canBeMaster: entering');
    cb = once(cb);

    // get the last leader of the shard.
    function getLastPrimary(_cb) {
        _cb = once(_cb);
        _getShardHistory(self, function (err, history) {
            if (err) {
                return _cb(new verror.VError(err));
            }

            /*
             * the history should be sorted, check from last to first and look
             * for the last primary. history looks like:
             * {
             *   "time": "1382288491791",
             *    "ip": "10.77.77.23",
             *    "action": "NewStandby",
             *    "role": "Leader",
             *    "master": "",
             *    "slave": "10.77.77.24",
             *    "zkSeq": "0000000388"
             * }
             */
            for (var i = history.length -1; i >= 0; i--) {
                var h = history[i];
                if (h.action === 'AssumeLeader') {
                    log.info({
                        primary: h
                    }, 'Shard._canBeMaster.getLastPrimary: got primary');
                    return _cb(null, h);
                }
            }

            return _cb();
        });
    }

    var canBeMaster = false;
    vasync.pipeline({funcs: [
        function _getLastRole(_, _cb) {
            self._postgresMgr.getLastRole(function (err, role) {
                if (err) {
                    return _cb(new verror.VError(err));
                }

                _.role = role;

                return _cb();
            });
        },
        function _checkRole(_, _cb) {
            /*
             * if we don't have a role, then we return true, since we don't
             * know what state we are in. By definition, a virgin manatee won't
             * have a role, and we always want it to enter the shard as primary
             * as this indicates a bootstrapping operation.
             *
             */
            if (!_.role || !_.role.role) {
                canBeMaster = true;
                return _cb();
            } else if (_.role.role === 'async' || _.role.role === 'potential') {
                // bail right away if we are an async
                canBeMaster = false;
                return _cb();
            } else if (_.role.role === 'sync' || _.role.role === 'primary') {
                getLastPrimary(function (err, primary) {
                    if (err) {
                        return _cb(new verror.VError(err));
                    }
                    /*
                     * if there was never a primary in the shard, then it's
                     * safe to be a primary.
                     */
                    if (!primary) {
                        canBeMaster = true;
                        return _cb();
                    }

                    /*
                     * If the previous primary was me or my master, then it's
                     * safe to become primary myself.
                     */
                    var myIp;
                    if (_.role.role === 'primary') {
                        myIp = self._ip;
                    } else {
                        myIp = url.parse(_.role.leader).hostname;
                    }
                    if (primary.ip === myIp) {
                        canBeMaster = true;
                        return _cb();
                    } else {
                        canBeMaster = false;
                        return _cb();
                    }
                });
            } else {
                // we should never hit this.
                return _cb(new verror.VError('Incorrect role: ' +
                    util.inspect(_.role) + ' parsed'));
            }

        }
    ], arg: {}}, function (err, results) {
        if (err) {
            err = new verror.VError(err);
        }
        log.info({err: err, canBeMaster: canBeMaster},
            'Shard.canBeMaster: exiting');

        return cb(err, canBeMaster);
    });
}

/**
 * @callback Shard-getShardHistoryCb
 * @param {Error} error
 * @param {object[]} history The sorted array of history objects.
 * @param {string} history.time Time since epoch in ms the event occurred.
 * @param {string} history.ip Ip of the node.
 * @param {string} history.action Type of event.
 * @param {string} history.role Role of the node.
 * @param {string} history.master Leader of the node.
 * @param {string} history.slave Slave of the node.
 * @param {string} history.zkSeq ZK sequence number of the node.
 */

/**
 * Get every transition that's ever ocurred in this shard, sorted by time.
 * @param {object} self The shard object.
 * @param {Shard-getshardhistorycb} cb
 */
function _getShardHistory(self, cb) {
    var log = self._log;
    log.info('Shard._getShardHistory: entering');

    var history;

    vasync.pipeline({funcs: [
        function listHistory(arg, _cb) {
            var _path = self._shardPath + '/history';
            log.debug({path: _path}, 'listing zk path');
            self._zk.readdir(_path, function (err, nodes) {
                arg.nodes = nodes;
                log.debug({
                    err: err,
                    nodes: nodes
                }, 'returned from listHistory');
                return _cb(err);
            });
        },
        // entry looks like timestamp-ip-role-master-slave-zkseq from zk.
        function formatNodes(arg, _cb) {
            arg.formattedNodes = [];
            for (var i = 0; i < arg.nodes.length; i++) {
                var fNode = arg.nodes[i].split('-');
                arg.nodes[i] = {};
                var node = arg.nodes[i];
                for (var j = 0; j < fNode.length; j++) {
                    var entry = (fNode[j] === null ^ fNode[j] === 'undefined' ^
                                 fNode[j] === 'null') ?  '' : fNode[j];
                    switch (j) {
                        case 0:
                            node['time'] = entry;
                            break;
                        case 1:
                            node['ip'] = entry;
                            break;
                        case 2:
                            node['action'] = entry;
                            break;
                        case 3:
                            node['role'] = entry;
                            break;
                        case 4:
                            node['master'] = entry;
                            break;
                        case 5:
                            node['slave'] = entry;
                            break;
                        case 6:
                            node['zkSeq'] = entry;
                            break;
                        default:
                            break;
                    }
                }
            }

            return _cb();
        },
        function sortNodes(arg, _cb) {
            arg.nodes.sort(function (a, b) {
                return a.zkSeq - b.zkSeq;
            });

            history = arg.nodes;
            return _cb();
        }
    ], arg: {}}, function (err) {
        log.info({err: err}, 'Shard._getShardHistory: exit');
        return cb(err, history);
    });
}

/**
 * Set the listeners on the zk election object.
 * @param {object} self The shard object.
 */
function _setZKElectionListeners(self) {
    var log = self._log;
    self._election.on('leader', function () {
        log.info('leader event');
        var req = {
            invoke: _leader,
            ctx: {
                self: self
            },
            transition: 'leader'
        };

        self._transitionQueue.push(req);
    });

    self._election.on('newLeader', function (myLeader) {
        log.info({
            newLeader: myLeader
        }, 'newLeader event');
        var req = {
            invoke: _newLeader,
            ctx: {
                self: self,
                leader: myLeader
            },
            transition: 'newLeader'
        };

        self._transitionQueue.push(req);
    });
}

/**
 * Initialize this node for entry into the shard. Only invoked when instanting
 * this node.
 *
 * @param {object} self The shard object.
 * @param {Shard-cb} callback
 */
function _init(self, callback) {
    assert.func(callback, 'callback');
    var log = self._log;
    log.info('Shard.init: entering');
    vasync.pipeline({
        funcs: [
            function _createZk(_, _cb) {
                _cb = once(_cb);
                _createZkClient(self._zkCfg, function (err, zkClient) {
                    if (err) {
                        self._log.fatal({err: err}, 'unable to connect to zk');
                        return _cb(new verror.VError(err));
                    }
                    self._zk = zkClient;

                    self._zk.on('error', function (err4) {
                        self._log.fatal({err: err4}, 'zk error, exiting');
                        throw err4;
                    });

                    return _cb();
                });
            },
            function _initAndCleanup(_, _cb) {
                _zkInitAndCleanupZkNodes(self, function (err) {
                    if (err) {
                        err = new verror.VError(err);
                    }
                    return _cb(err);
                });
            },
            function _checkWatchErr(_, _cb) {
                _checkAndWatchError(self, function (err) {
                    if (err) {
                        err = new verror.VError(err);
                    }
                    return _cb(err);
                });
            },
            /*
             * MANATEE-127 Check if we used to be an async. If we were a sync
             * or primary, then it's safe to start as if and when we get
             * promoted to leader, we will check again whether we are suitable
             * to be the primary then.
             */
            function _checkWasAsync(_, _cb) {
                _waitOrStart(self, function (err) {
                    if (err) {
                        err = new verror.VError(err);
                    }
                    return _cb(err);
                });
            },
            function _joinElection(_, _cb) {
                self._election = zkplus.createGenericElection({
                    client: self._zk,
                    path: self._shardElectionPath,
                    pathPrefix: self._ip,
                    object: {
                        zoneId: self._zoneId,
                        ip: self._ip,
                        pgUrl: self._postgresMgr.getUrl().href,
                        backupUrl: self._backupUrl
                    }
                });
                return _cb();
            },
            function _setListeners(_, _cb) {
                var queue = self._transitionQueue;

                _setZKElectionListeners(self);

                self._heartbeatServer.on('newStandby', function (standby) {
                    log.info({
                        standby: standby
                    }, 'newStandby event');
                    var req = {
                        invoke: _newStandby,
                        ctx: {
                            self: self,
                            standby: standby
                        },
                        transition: 'newStandby'
                    };

                    queue.push(req);
                });

                self._heartbeatServer.on('expired', function (standby) {
                    log.info({
                        standby: standby
                    }, 'expired event');
                    var req = {
                        invoke: _expired,
                        ctx: {
                            self: self,
                            expiredStandby: standby
                        },
                        transition: 'expired'
                    };

                    queue.push(req);
                });
                return _cb();
            },
            function _startHeartbeatServer(_, _cb) {
                self._heartbeatServer.init();
                return _cb();
            },
            function _vote(_, _cb) {
                self._election.vote(function (err) {
                    if (err) {
                        err = new verror.VError(err);
                    }
                    return _cb(err);
                });
            }
        ],
        args: {}
    }, function (err) {
        log.info({err: err}, 'Shard.entering: exiting');
        return callback(err);
    });
}

/**
 * Check whether we were an async, if so, then wait until there are others in
 * the shard before returning.
 *
 * @param {object} self The shard object.
 * @param {Shard-cb} cb
 */
function _waitOrStart(self, cb) {
    assert.func(cb, 'callback');
    assert.object(self, 'self');

    var log = self._log;
    var zk = self._zk;
    log.info('Shard._waitForStart: entering');

    vasync.pipeline({funcs: [
        function _checkPreviousSyncState(_, _cb) {
            self._postgresMgr.canStartAsMaster(function (can) {
                _.can = can;

                return _cb();
            });
        },
        function _waitForAnotherNode(_, _cb) {
            cb = once(cb);
            if (!_.can) {
                log.info('Shard._waitForStart: was async, waiting for other ' +
                         'nodes to join');
                zk.watch(self._shardElectionPath, {method: 'list'},
                         function (err, listn) {
                    if (err) {
                        return cb(err);
                    }

                    listn.once('error', function (_err) {
                        return cb(_err);
                    });

                    listn.once('children', function (children) {
                        log.info('Shard._waitForStart: ' +
                                 'other nodes are in shard, can start');
                        return cb();
                    });

                    /*
                     * we can't take the risk that someone joined before we set
                     * the watch, so we also do a list after we set the watch,
                     * if either returns someone in the election, then we can
                     * stop waiting.
                     */
                    zk.readdir(self._shardElectionPath, function (_err, nodes) {
                        log.trace({err: _err, nodes: nodes},
                        'Shard_.waitForPrimary: returned from zk list');
                        if (_err) {
                            return cb(_err);
                        }

                        if (nodes.length > 0) {
                            log.info('Shard._waitForStart: ' +
                                     'other nodes are in shard, can start');
                            return cb();
                        }
                    });
                });
            } else {
                log.info('Shard._waitForStart: was not async, can start');
                return _cb();
            }
        }
    ], arg: {}}, function (err) {
    log.info('Shard._waitForStart: exiting');
        return cb(err);
    });
}

/**
 * @callback Shard-createZkClientCb
 * @param {Error} error
 * @param {zkplusClient} client zkplus client.
 */

/**
 * Create a zookeeper client.
 * @param {object} options ZK Client options.
 * @param {Bunyan} options.log Bunyan logger.
 * @param {object[]} options.servers ZK servers.
 * @param {string} options.servers.host ZK server ip.
 * @param {number} options.servers.port ZK server port.
 * @param {number} options.timeout Time to wait before timing out connect
 * attempt in ms.
 * @param {Shard-createZkClientCb} cb
 */
function _createZkClient(opts, cb) {
    assert.object(opts, 'options');
    assert.object(opts.log, 'options.log');
    assert.arrayOfObject(opts.servers, 'options.servers');
    assert.number(opts.timeout, 'options.timeout');
    assert.func(cb, 'callback');

    assert.ok((opts.servers.length > 0), 'options.servers empty');
    for (var i = 0; i < opts.servers.length; i++) {
        assert.string(opts.servers[i].host, 'servers.host');
        assert.number(opts.servers[i].port, 'servers.port');
    }

    function _createClient(_, _cb) {
        var client = zkplus.createClient(opts);

        function onConnect() {
            client.removeListener('error', onError);
            log.info('zookeeper: connected');
            _cb(null, client);
        }

        function onError(err) {
            client.removeListener('connect', onConnect);
            _cb(err);
        }


        client.once('connect', onConnect);
        client.once('error', onError);

        client.connect();
    }

    var log = opts.log;
    var retry = backoff.call(_createClient, null, cb);
    retry.failAfter(Infinity);
    retry.setStrategy(new backoff.ExponentialStrategy({
        initialDelay: 1000,
        maxDelay: 30000
    }));

    retry.on('backoff', function (number, delay) {
        var level;
        if (number === 0) {
            level = 'info';
        } else if (number < 5) {
            level = 'warn';
        } else {
            level = 'error';
        }
        log[level]({
            attempt: number,
            delay: delay
        }, 'zookeeper: connection attempted (failed)');
    });

    return (retry);
}

/**
 * Writes an error node in ZK if we detect the shard to be in error mode. The
 * error node is written under /shardpath/error and contains the primary, and
 * standby IPs of the shard.
 *
 * @param {Shard} self This shard object.
 * @param {string} standby The standby that caused this shard to enter error
 * mode.
 * @param {Shard-cb} callback
 */
function _writeErrorNode(self, standby, callback) {
    assert.func(callback, 'callback');
    assert.object(self, 'self');
    assert.string(standby, 'standby');
    var log = self._log;
    var zk = self._zk;

    log.info({
        standby: standby
    }, 'entering shard._writeErrorNode');
    var standbyUrl = url.parse(standby).hostname;

    var opts = {
        object: {
            primary: self._ip,
            zoneId: self._zoneId,
            standby: standbyUrl
        }
    };

    log.info({
        error: opts.object
    }, 'shard._writeErrorNode: writing error node to zk');

    zk.creat(self._shardErrorPath, opts, function (err) {
        return callback(err);
    });
}

/**
 * Write a node under the history path about a particular transition.
 *
 * @param {Shard} self This shard object.
 * @param {object} opts Options.
 * @param {string} opts.role Role of this node.
 * @param {string} opts.action Transition action.
 * @param {string} [opts.standby] Standby of this node.
 * @param {string} [opts.leader] Leader of this node.
 * @param {Shard-cb} callback
 */
function _writeHistoryNode(self, opts, callback) {
    assert.func(callback, 'callback');
    assert.object(self, 'self');
    assert.object(opts, 'opts');
    assert.string(opts.role, 'opts.role');
    assert.string(opts.action, 'opts.action');

    var log = self._log;
    var zk = self._zk;
    log.info({
        opts: opts
    }, 'Shard._writeHistoryNode: entering');

    // parse the urls and pull out the hostname
    if (opts.leader) {
        opts.leader = url.parse(opts.leader).hostname;
    }
    if (opts.standby) {
        opts.standby = url.parse(opts.standby).hostname;
    }

    var _path = self._shardHistoryPath + '/' + Date.now() + '-' + self._ip +
                '-' + opts.action + '-' + opts.role + '-' + opts.leader + '-' +
                opts.standby + '-';

    log.info({
        path: _path
    }, 'Shard._writeHistoryNode: writing node');

    zk.create(_path, {flags: ['sequence']}, callback);
}

/**
 * Check and watch the error node. If the shard is in error mode, then put the
 * shard in error state.
 * mode.
 *
 * @param {Shard} self This shard object.
 * @param {Shard-cb} callback
 */
function _checkAndWatchError(self, callback) {
    var log = self._log;
    var zk = self._zk;

    log.info({
        shardErrorPath: self._shardErrorPath
    }, 'entering shard._checkAndWatchError');

    var tasks = [
        function watchForError(_, cb) {
            zk.watch(self._shardPath, { method: 'list' },
                     function (err, listener)
            {
                if (err) {
                    return callback(new verror.VError(err,
                    'unable to set watch on error node'));
                }

                listener.on('children', function (children) {
                    children.forEach(function (child) {
                        if (child === 'error') {
                            log.fatal('got shard error, going to readonly ' +
                                      'mode');
                            _shardError(self);
                        }
                    });
                });

                return cb();
            });
        },
        function checkForError(_, cb) {
            zk.get(self._shardErrorPath, function (err, obj) {
                if (err && err.code === ZK.ZNONODE) {
                    return cb();
                } else if (err) {
                    return cb(new verror.VError(err,
                    'unable to get error node'));
                } else if (obj) {
                    log.fatal('got shard error, going to readonly mode');
                    _shardError(self);
                    // don't return cb to prevent further state transitions
                    return (undefined);
                } else {
                    return cb();
                }
            });
        }
    ];

    vasync.pipeline({funcs: tasks, arg: {}}, function (err) {
        if (err) {
            return callback(new verror.VError(
                err, 'unable to check for shard error state'));
        } else {
            log.info('finished checking for shard errors');
            return callback();
        }
    });
}

/**
 * This function gets called if something catestrophic has happened in the
 * shard. Usually this is a result of out of sync xlogs between the master and
 * the slave. Postgres can not make forward progress, and we do not want to
 * take write in this degraded state.  We put the node in read only mode and
 * remove all listeners and wait for an operator to manually clear the shard
 *
 * @param {Shard} self This shard object.
 */
function _shardError(self) {
    var log = self._log;
    log.fatal('entering shard.shardError: SOMETHING IS LIKELY VERY WRONG ' +
        'WITH THE SHARD, OPERATOR INTERVENTION NEEDED!');
    // remove all listeners
    self.removeAllListeners();
    if (self._heartbeatServer) {
        self._heartbeatServer.removeAllListeners();
    }
    if (self._heartbeatClient) {
        self._heartbeatClient.stop();
    }
    if (self._election) {
        self._election.removeAllListeners();
        self._election.on('error', function () {});
    }
    if (self._zk) {
        self._zk.removeAllListeners();
        // swallow zk errors
        self._zk.on('error', function () {});
        self._zk.close();
    }

    self._postgresMgr.readOnly(function (err) {
        // give node something to do so it doesn't exit.
        setInterval(function () {
            log.trace('In forced read only mode due to error');
        }, 2000);

        if (err) {
            self.emit(verror.VError(err, 'unable to transition to ro mode'));
        }
    });
}

/**
 * Delete any previous emphemeral zk-nodes belonging to this pg node.
 *
 * @param {shard} self this shard object.
 * @param {shard-cb} callback
 */
function _zkInitAndCleanupZkNodes(self, callback) {
    var log = self._log;
    var zk = self._zk;

    log.info({
        shardElectionPath: self._shardElectionPath,
        ip: self._ip
    }, 'Shard._zkInitAndCleanupZkNodes');
    var tasks = [
        function mkdirpElectionPath(_, cb) {
            zk.mkdirp(self._shardElectionPath, function (err) {
                log.debug({
                    path: self._shardElectionPath,
                    err: err
                }, 'returned from mkdirpElectionPath');
                cb(err);
            });
        },
        function mkdirpHistoryPath(_, cb) {
            zk.mkdirp(self._shardHistoryPath, function (err) {
                log.debug({
                    path: self._shardHistoryPath,
                    err: err
                }, 'returned from mkdirpShardHistoryPath');
                cb(err);
            });
        },
        function readdir(_, cb) {
            log.debug({
                shardElectionPath: self._shardElectionPath
            }, 'listing election dir');
            zk.readdir(self._shardElectionPath, function (err, nodes) {
                log.debug({
                    err: err,
                    nodes: nodes
                }, 'returned from readdir');
                if (err) {
                    return cb(err);
                } else {
                    _.nodes = nodes;
                    return cb();
                }
            });
        },
        function determineNodes(_, cb) {
            log.debug({
                nodes: _.nodes
            }, 'entering determine nodes');
            var nodes = [];
            for (var i = 0; i < _.nodes.length; i++) {
                var node = _.nodes[i];
                if (node.split('-')[0] === self._ip) {
                    nodes.push(node);
                }
            }

            _.nodes = nodes;
            cb();
        },
        function delPrevNodes(_, cb) {
            log.debug({
                nodes: _.nodes
            }, 'entering delPrevNodes');
            var done = 0;
            for (var i = 0; i < _.nodes.length; i++) {
                var spath = self._shardElectionPath + '/' + _.nodes[i];
                log.debug({
                    node: spath
                }, 'deleting prev node');
                zk.rmr(spath, function (err) {
                    if (err) {
                        return cb(err);
                    } else {
                        done++;
                        var nLength = _.nodes.length;
                        if (done === (nLength - 1)) {
                            return cb();
                        }
                    }
                    return true;
                });

            }

            return cb();
        }
    ];

    vasync.pipeline({funcs: tasks, arg: {}}, function (err) {
        if (err) {
            log.error('Unable to initialize zk paths or delete previous ' +
                      'emphemeral nodes');
            return callback(err);
        } else {
            log.info('finished initializing zk paths and deleting previous ' +
                     'emphemeral nodes');
            return callback();
        }
    });
}

/**
 * Write the details of this registrar into the ZK registrar node so it's
 * registered in DNS.
 *
 * @param {shard} self this shard object.
 * @param {shard-cb} callback
 */
function _writeRegistrar(self, callback) {
    var log = self._log;
    var zk = self._zk;

    var opts = {
        flags:  ['ephemeral'],
        object: {
            type: 'database',
            database: {
                primary: self._postgresMgr.getUrl().href,
                ttl: self._ttl
            }
        }
    };

    log.info({
        registrarPath: self._registrarPath,
        url: self._postgresMgr.getUrl().href,
        opts: opts
    }, 'entering write registrar');

    var tasks = [
        function mkdirp(_, cb) {
            zk.mkdirp(self._registrarPathPrefix, function (err) {
                var err2;
                if (err)  {
                    err2 = verror.VError(err);
                }
                cb(err2);
            });
        },
        function rm(_, cb) {
            zk.rmr(self._registrarPath, function () {
                // don't check for err on rmr, it could fail if the node DNE
                cb();
            });
        },
        function create(_, cb) {
            zk.create(self._registrarPath, opts, function (err) {
                var err2;
                if (err)  {
                    err2 = verror.VError(err);
                }
                cb(err2);
            });
        }
    ];

    vasync.pipeline({funcs: tasks, arg: {}}, function (err) {
        if (err) {
            log.error('Unable to write registrar');
            return callback(err);
        } else {
            log.info('finished writing registrar');
            return callback();
        }
    });

    return true;
}

/**
 * transform an zk election node name into a postgres url.
 * @param {Shard} self This shard object.
 * @param {string} zkNode The zknode, e.g. 10.77.77.9-0000000057
 */
function _transformPgUrl(self, zkNode) {
    return 'tcp://postgres@' + zkNode.split('-')[0] + ':' +
        self._postgresMgr.getUrl().port + '/postgres';
}

/**
 * transform an zk election node name into a backup server url.
 * @param {Shard} self This shard object.
 * @param {string} zkNode The zknode, e.g. 10.77.77.9-0000000057
 */
function _transformBackupUrl(self, zkNode) {
    return 'http://' + zkNode.split('-')[0] + ':' + self._backupPort;
}

/**
 * transform an zk election node name into hearbeat server url.
 * @param {Shard} self This shard object.
 * @param {string} zkNode The zknode, e.g. 10.77.77.9-0000000057
 */
function _transformHeartbeatUrl(self, zkNode) {
    return 'http://' + zkNode.split('-')[0]+ ':' +
           self._heartbeatServerCfg.port;
}

/** #@- */
