/**
 * @overview The Manatee Coordinator that manages each node in its shard.
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
var leader = require('leader');
var once = require('once');
var path = require('path');
var PostgresMgr = require('./postgresMgr');
var TransitionQueue = require('./transitionQueue');
var url = require('url');
var util = require('util');
var vasync = require('vasync');
var verror = require('verror');
var zk = require('node-zookeeper-client');

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
 * events {leader, newLeader} from the node-leader election object.
 *
 * 'leader' events implies that this current node is now the head of the daisy
 * chain.
 *
 * 'newLeader' event contains the URL of the new leader, and implies that the
 * node in front of the current node may have changed.
 *
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
 * notified by a 'newLeader' event.TODO update The async-1 will sending
 * to the node in front, which will result in a 'newStandby' event
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
 * @param {number} options.backupPort The port of the backup server.
 * @param {number} options.postgresPort  The port of the PG instance.
 * @param {number} options.ttl The ttl of the registrar node.
 * @param {object} options.PostgresMgrCfg The postgres manager config.
 * @param {object} options.zkCfg Zk client cfgs.
 * @param {string} options.shardPath The path under which znodes for this shard
 * are stored.
 * @param {string} options.ip The IP of this node.
 * @param {string} options.zoneId The UUID of the zone this node runs on.
 *
 * @throws {verror.VError} If unable to initialize the shard.
 * @throws {Error} If the options object is malformed.
 */
function Shard(options) {
    assert.object(options, 'options');
    assert.object(options.log, 'options.log');

    assert.number(options.backupPort, 'options.backupPort');
    assert.string(options.ip, 'options.ip');
    assert.object(options.postgresMgrCfg, 'options.postgresMgrCfg');
    assert.number(options.postgresPort, 'options.postgresPort');
    assert.string(options.shardPath, 'options.shardPath');
    assert.number(options.ttl, 'options.ttl');
    assert.object(options.zkCfg, 'options.zkCfg');
    assert.string(options.zkCfg.connStr, 'options.zkCfg.connStr');
    assert.optionalObject(options.zkCfg.opts, 'options.zkCfg.opts');
    assert.string(options.zoneId, 'options.zoneId');

    var self = this;

    /** @type {Bunyan} The bunyan log object */
    this._log = options.log;

    /** @type {number} The backup server port */
    this._backupPort = options.backupPort;

    /** @type {number} The postgresql port */
    this._postgresPort = options.postgresPort;

    /**
     * @type {string} Path under which shard metadata such as elections are
     * stored. e.g. /manatee/com.joyent.1
     */
    this._shardPath = options.shardPath;

    /** @type {string} Path under which elections for this shard are held */
    this._shardElectionPath = options.shardPath + '/election';

    /** @type {string} Znode that indicates an error with the shard */
    this._safeModePath = options.shardPath + '/error';

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

    /** @type {boolean} Whether we have assumed leadership of the shard */
    this._isLeader = false;

    /** @type {Object} The zk cfg */
    this._zkCfg = options.zkCfg;

    /** @type {string} The IP of this peer, ex. 10.99.99.16 */
    this._ip = options.ip;

    /** @type {PostgresMgr} The postgres manager */
    this._postgresMgr = new PostgresMgr(options.postgresMgrCfg);

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

    /**
     * @type {string}
     * Serialized version of this shard's services. This colon seperated into 3
     * fields.
     * f1: IP Address
     * f2: PG port
     * f3: backup port
     */
    this._electionPrefix = self._ip + ':' + self._postgresPort + ':' +
        self._backupPort;
    /** @type {node-leader} The ZK election */
    this._election = null;
    /** @type {node-zookeeper-client} The ZK client */
    this._zk = null;

    self._init(function (err) {
        if (err) {
            throw new verror.VError(err, 'unable to initialize shard');
        }
    });
}

module.exports = {
    start: function (cfg) {
        return new Shard(cfg);
    }
};

/**
 * @callback Shard-cb
 * @param {Error} err
 */

/**
 * Transition functions.
 *
 * Since each node can get more than 1 state transition event at one time (if
 * more than one node dies in the shard), it's really important that state
 * transitions are processed serially. Thus events are enqueued in a queue and
 * processed in order, one at a time.
 *
 * Each state transition has a corresponding function handler below, which
 * encapsulates the steps of the transition.
 *
 * @constant
 * @type {object}
 */
Shard.prototype.TRANSITION_FUNCS = {
    /**
     * Take over as the primary of the shard. We are the node that will be
     * taking writes. As such, we need to satisfy the following conditions
     * before we can become the primary.
     *
     * - If we were the primary -- check to ensure that the last primary in the
     * shard was us.
     * - If we were the sync -- check to ensure that the last primary in the
     * shard was our primary.
     * - If we were the async, or we don't satisfy the previous 2 conditions,
     * wait until another node enters the shard and rejoin the election.
     *
     * @param {object} ctx The context associated with this transition.
     * @param {object} ctx.self The shard object.
     * @param {Shard-cb} callback
     */
    LEADER: function _leader(ctx, callback) {
        var self = ctx.self;
        var postgresMgr = self._postgresMgr;
        var log = self._log;
        log.info({currStandby: self._currStandby}, 'Shard.LEADER: entering');

        vasync.pipeline({funcs: [
            function canBeLeader(_, cb) {
                self._canBePrimary(function (err, can) {
                    if (err) {
                        return cb(new verror.VError(err));
                    }

                    if (!can) {
                        /*
                         * first stop checking sync state, but ignore errors,
                         * don't delete the previous sync state until we've
                         * actually become the primary.
                         */
                        self._postgresMgr.stopSyncChecker(false, function () {
                            log.info({can: can}, 'Shard.LEADER: was async, ' +
                                'aborting LEADER and waiting for other peers');
                        self._waitForOtherPeers(callback);
                        return;
                        });
                    } else {
                        return cb();
                    }
                });
            },
            function primary(_, cb) {
                cb = once(cb);
                log.info('LEADER; going to pg primary mode');
                var e = postgresMgr.primary(self._currStandby);
                e.once('done', function () {
                    return cb();
                });
                e.once('error', function (err) {
                    return cb(err);
                });

                /*
                 * MANATEE-212 explicitly check to make sure we are not stuck
                 * on the transition to primary. We also know that since we are
                 * the primary, any new events will be either expired or new
                 * standby events. Therefore, it is always safe to cancel the
                 * primary transition. The primary function ensures that we
                 * will still be promoted to primary, and only the standby
                 * replication check will be cancelled.
                 */
                (function poll() {
                    // if we've started and there's another element in the
                    // queue
                    if (self._transitionQueue.hasNext()) {
                        log.info('Shard.LEADER: new element in queue, ' +
                                 'cancelling curent operation');
                        e.cancel();
                        _.cancelled = true;
                    } else {
                        if (!cb.called) {
                            setTimeout(poll, 1000);
                        }
                    }
                })();
            },
            function writeRegistrar(_, cb) {
                log.info('LEADER; writing registrar');
                self._writeRegistrar(cb);
            },
            function writeHistoryNode(_, cb) {
                log.info('LEADER; writing historyNode');
                var opts = {
                    standby: _.cancelled ? null : self._currStandby,
                    role: 'Leader',
                    action: 'AssumeLeader'
                };
                self._writeHistoryNode(opts, cb);
            }
        ], arg: {}}, function (err) {
            log.info({
                err: err,
                currStandby: self._currStandby
            }, 'Shard.LEADER: finished.');

            if (err && err.__walCorruption) {
                log.error({err: err}, 'LEADER: got PG WAL corruption');
                self._writeErrorNode(self._currStandby, function (err2) {
                    if (err2) {
                        return callback(new verror.VError(err2,
                           'unable to write error node'));
                    }

                    log.info('LEADER: entering shardError state');
                    self._safeMode();
                    // TODO: instead we should watch for the error node to go
                    // away.  don't return cb to stop any further state changes
                    return (undefined);
                });
            } else if (err) {
                err = new verror.VError(err, 'unable to transition to primary');
                return callback(err);
            } else {
                log.info('LEADER: successful transition');
                self._isLeader = true;
                self._leader = null;
                return callback();
            }
        });
    },

    /**
     * A new leader event signifies that there might be a new leader of the
     * current node. This implies that we are not the leader of the shard, and
     * we must start slaving from the new leader.
     *
     * @param {object} ctx The context associated with this transition.
     * @param {object} ctx.self The shard object.
     * @param {string} ctx.leader The IP of the new leader.
     * @param {Shard-cb} callback
     */
    NEW_LEADER: function _newLeader(ctx, callback) {
        var self = ctx.self;
        var log = self._log;
        var myLeader = ctx.leader;
        var pgUrl = self._transformPgUrl(myLeader);
        var backupUrl = self._transformBackupUrl(myLeader);
        self._leader = pgUrl;
        var postgresMgr = self._postgresMgr;
        log.info({
            leader: myLeader,
            primaryUrl: pgUrl,
            backupUrl: backupUrl
        }, 'Shard.NEW_LEADER: entering');

        vasync.pipeline({funcs: [
            /*
             * MANATEE-207, if we can be the shard primary then we have to
             * ensure that our leader can also be primary. If not, we do
             * nothing and wait for additional events.
             */
            function canIBeprimary(_, cb) {
                self._canBePrimary(function (err, r, v) {
                    if (err) {
                        err = new verror.verror(err);
                    }
                    // Ignore this flag if we are a virgin manatee.
                    _.canBePrimary = r && !v;
                    return cb(err);
                });
            },
            function canLeaderBePrimary(_, cb) {
                cb = once(cb);
                // We only care if we can be primary
                if (!_.canBePrimary) {
                    _.continue = true;
                    return cb();
                }

                function check() {
                    self._canNodeBePrimary(ctx.leader, function (err, r) {
                        if (err) {
                            return cb(new verror.VError(err));
                        }
                        _.continue = r;
                        if (!r) {
                            log.info({leader: myLeader},
                                     'Shard.NEW_LEADER: leader candidate not ',
                                     'acceptible, ignoring event');
                            /*
                             * Avoid deadlock here. If we are the last event
                             * in the queue, we want to keep checking whether
                             * the leader can be a candidate. Otherwise on a
                             * legitimate primary death back to async
                             * transition, we will never join the shard since
                             * there will be no more events even though the
                             * leader could potentially become a candidate.
                             */
                            if (!self._transitionQueue.hasNext()) {
                                log.info({leader: myLeader},
                                         'Shard.NEW_LEADER: checking leader ' +
                                             'candidacy again in 1s');
                                setTimeout(check, 1000);
                            } else {
                                return cb();
                            }
                        } else {
                            return cb();
                        }
                    });
                }

                check();
            },
            function pgStandby(_, cb) {
                if (!_.continue) {
                    return cb();
                }
                log.info({
                    pgUrl: pgUrl,
                    backupUrl: backupUrl
                }, 'Shard.NEW_LEADER: pg going to standby');

                postgresMgr.standby(pgUrl, backupUrl, cb);
            },
            function writeHistory(_, cb) {
                if (!_.continue) {
                    return cb();
                }
                log.info('Shard.NEW_LEADER: writing history');
                var opts = {
                    standby: self._currStandby,
                    role: 'Standby',
                    leader: self._leader,
                    action: 'NewLeader'
                };
                self._writeHistoryNode(opts, cb);
            }
        ], arg: {}}, function (err) {
            log.info({
                err: err,
                pgUrl: pgUrl,
                backupUrl: backupUrl
            }, 'Shard.NEW_LEADER: finished.');

            if (err)  {
                err = new verror.VError(err, 'Shard.NEW_LEADER: failed');
            }

            return callback(err);
        });
    },

    /**
     * A new standby event signifies that we have a new standby -- this is
     * only relevant if we are the overall leader of the shard, as we'll need
     * to change our synchronous replication standby to point to the new
     * standby. Otherwise, we do nothing since the replication will be
     * asynchronous and no configs need to be updated on our side.
     *
     * @param {object} ctx The context associated with this transition.
     * @param {object} ctx.self The shard object.
     * @param {string} ctx.standby The PostgreSQL URL of the standby.
     * @param {Shard-cb} callback
     */
    NEW_STANDBY: function _newStandby(ctx, callback) {
        var self = ctx.self;
        var log = self._log;
        var postgresMgr = self._postgresMgr;
        var standbyUrl = self._transformPgUrl(ctx.standby);
        self._currStandby = standbyUrl;

        log.info({standby: standbyUrl}, 'Shard.NEW_STANDBY: entering');

        if (!self._isLeader) {
            log.info({
                currStandby: self._currStandby
            }, 'Shard.NEW_STANDBY: not primary, ignoring standbys');

            return callback();
        }

        vasync.pipeline({funcs: [
            function updateStandby(_, cb) {
                cb = once(cb);
                log.info({
                    currStandby: self._currStandby
                }, 'Shard.NEW_STANDBY: updating pg standby');
                var e = postgresMgr.updateStandby(standbyUrl);
                e.once('done', cb);
                e.once('error', cb);
                /*
                 * MANATEE-212 explicitly check to make sure we are not stuck
                 * on the transition to primary. We also know that since we are
                 * the primary, any new events will be either expired or new
                 * standby events. Therefore, it is always safe to cancel the
                 * primary transition. The primary function ensures that we
                 * will still be promoted to primary, and only the standby
                 * replication check will be cancelled.
                 */
                (function poll() {
                    // if we've started and there's another element in the
                    // queue
                    if (self._transitionQueue.hasNext()) {
                        log.info('Shard.NEW_STANDBY: new element in queue, ' +
                                 'cancelling curent operation');
                        _.cancelled = false;
                        e.cancel();
                    } else {
                        if (!cb.called) {
                            setTimeout(poll, 1000);
                        }
                    }
                })();
            },
            function writeHistory(_, cb) {
                if (_.cancelled) {
                    return cb();
                }
                // TODO: write history when repl has been cancelled?
                log.info('Shard.NEW_STANDBY: writing history');
                var opts = {
                    standby: standbyUrl,
                    role: 'leader',
                    action: 'NewStandby'
                };
                self._writeHistoryNode(opts, cb);
            }
        ], arg: {}}, function (err) {
            log.info({
                err: err,
                prevStandby: self._currStandby,
                standbyUrl: standbyUrl
            }, 'Shard.NEW_STANDBY: finished.');

            if (err && err.__walCorruption) {
                log.error({
                    err: err,
                    prevStandby: self._currStandby,
                    standbyUrl: standbyUrl
                }, 'Shard.NEW_STANDBY: got wal corruption');

                self._writeErrorNode(self._currStandby, function (err2) {
                    if (err2) {
                        return callback(new verror.VError(err2,
                        'unable to write safe mode node'));
                    }

                    log.info('Shard.NEW_STANDBY: entering safe mode');
                    self._safeMode();
                    // don't return cb to stop any further state changes
                });
            } else if (err) {
                err = new verror.VError(err,
                'Shard.NEW_STANDBY: unable to add new standby');
                return callback(err);
            } else {
                log.info('Shard.NEW_STANDBY: successful transition');
                return callback();
            }
        });
    },

    /**
     * An expired event signifies that a standby has expired. If we are the
     * leader of the shard, then we need to explicitly remove the standby from
     * postgresql.conf, as it was slaving synchronously. If we are not the
     * leader, there is nothing to do since the standby was an async and no
     * config changes are required.
     * We always write the history node for posterity.
     *
     * @param {object} ctx The context associated with this transition.
     * @param {object} ctx.self The shard object.
     *
     * @param {Shard-cb} callback
     */
    EXPIRED: function _expired(ctx, callback) {
        var self = ctx.self;
        var postgresMgr = self._postgresMgr;
        var log = self._log;
        var currStandby = self._currStandby;

        log.info({
            currStandby: self._currStandby,
            isLeader: self._election._isGLeader
        }, 'Shard.EXPIRED: entering');

        vasync.pipeline({funcs: [
            function removeStandby(_, cb) {
                // We can never not be GLeader once we acquire it, so no race
                // conditions here about the state of ourselves in the election.
                if (!self._election._isGLeader) {
                    return cb();
                }
                log.info({
                    currStandby: currStandby,
                    isLeader: self._election._isGLeader
                }, 'Shard.EXPIRED: isLeader, removing expired standby');
                var e = postgresMgr.updateStandby(null, cb);
                e.once('done', cb);
                e.once('error', cb);
                // fix for MANATEE-87 set remove currStandby if expired
                self._currStandby = null;
            },
            function writeHistory(_, cb) {
                log.info('Shard.EXPIRED: writing history');
                var opts = {
                    role: self._election._isGLeader ? 'Leader' : 'Standby',
                    leader: self._leader,
                    standby: currStandby,
                    action: 'ExpiredStandby'
                };
                self._writeHistoryNode(opts, cb);
            }
        ], arg: {}}, function (err) {
            if (err) {
                err = new verror.VError(err, 'Shard.EXPIRED failed');
            }

            log.info({
                err: err,
                currStandby: self._currStandby,
                isLeader: self._election._isGLeader
            }, 'Shard.EXPIRED: finished');

            return callback(err);
        });
    }
};


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
 * Pause the queue and prevent it from receiving other events.
 * Clear any remaining elements in the queue.
 * Leave the current election.
 * once we have left the election, wait until another node joins the shard.
 * Once another node joins the shard, join the election and resume the queue.
 *
 * @param {Shard-cb} callback
 *
 * @throws {verror.VError} If there is an error with the ZK client.
 */
Shard.prototype._waitForOtherPeers = function (callback) {
    var self = this;
    var log = self._log;

    log.info('Shard_.waitForOtherPeers: entering');

    vasync.pipeline({funcs:  [
        function pauseQueue(_, _cb) {
            /*
             * pause the queue and clear it to prevent any other events other
             * than the wait to enter the queue.
             */
            self._transitionQueue.pause();
            self._transitionQueue.clear();

            return _cb();
        },
        function leaveElection(_, _cb) {
            self._election.leave(_cb);
        },
        function stopPostgres(_, _cb) {
            self._postgresMgr.shutdown(_cb);
        },
        function _initAndCleanup(_, _cb) {
            self._zkInitAndCleanupZkNodes(_cb);
        },
        function _checkWatchErr(_, _cb) {
            self._checkAndWatchError(_cb);
        },
        function _waitForAnotherNode(_, _cb) {
            _cb = once(_cb);
            function waitForAnotherNode() {
                self._zk.getChildren(
                    self._shardElectionPath,
                    function watch(event) {
                        if (!_cb.called) {
                            waitForAnotherNode();
                        }
                    },
                    function (err, nodes) {
                        if (err) {
                            return _cb(new verror.VError(err));
                        }
                        if (nodes.length > 0) {
                            log.info('Shard._waitForOtherPeers: ' +
                                     'other nodes are in shard, can start');
                            return _cb();
                        }
                        self._log.info({nodes: nodes},
                                       'Shard._waitForOtherPeers: no nodes ' +
                                           'in shard, waiting for other peers');
                    });
            }
            self._log.info({path: self._shardElectionPath},
                           'Shard._waitForOtherPeers: checking other peers');
            waitForAnotherNode();
        },
        function _joinElection(_, _cb) {
            self._log.info('rejoining election');
            /*
             * resume the queue once this is complete so we can process
             * events again.
             */
            self._transitionQueue.resume();
            self._election = leader.createElection({
                zk: self._zk,
                path: self._shardElectionPath,
                pathPrefix: self._electionPrefix
            }, _cb);
            // set the electionlisteners since this election object is new.
            self._setZKElectionListeners();

            self._election.on('error', function (err) {
                throw new verror.VError(err, 'got election error');
            });
        },
        function _vote(_, _cb) {
            var data = JSON.stringify({
                zoneId: self._zoneId,
                ip: self._ip,
                pgUrl: self._postgresMgr.getUrl().href,
                backupUrl: self._backupUrl
            });
            self._election.vote(data, _cb);
        }
    ]}, function (err) {
        log.info('Shard_.waitForPrimary: exiting');
        return callback(err);
    });
};

/**
 * @callback Shard-canBePrimaryCb
 * @param {Error} err
 * @param {bool} canBePrimary
 * @param {bool} isVirgin
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
 * @param {Shard-canBePrimaryCb} callback
 */
Shard.prototype._canBePrimary = function (cb) {
    var self = this;
    var log = self._log;
    log.info('Shard._canBePrimary: entering');
    cb = once(cb);


    // get the last leader of the shard including its last know standby status
    function getLastPrimary(_cb) {
        _cb = once(_cb);
        var primary;
        var pIdx;
        var h;
        self._getShardHistory(function (err, history) {
            if (err) {
                return _cb(new verror.VError(err));
            }

            /*
             * the history should be sorted, check from last to first and look
             * for the last primary. history looks like:
             * {
             *   "time": "1382288491791",
             *    "ip": "10.77.77.23:5432",
             *    "action": "NewStandby",
             *    "role": "Leader",
             *    "master": "",
             *    "slave": "10.77.77.24:5432",
             *    "zkSeq": "0000000388"
             * }
             */
            for (var i = history.length -1; i >= 0; i--) {
                h = history[i];
                if (!primary && h.action === 'AssumeLeader') {
                    log.info({
                        primary: h
                    }, 'Shard._canBePrimary.getLastPrimary: got primary');
                    primary = h;
                    pIdx = i;
                    break;
                }
            }

            if (!primary) {
                log.info('Shard._canBePrimary.getLastPrimary: no primaries ' +
                         'exist');
                return _cb();
            }

            // once we have found a primary, we need to check the state of its
            // standby.
            for (var j = history.length -1; j > pIdx; j--) {
                // we must check after the assume leader event to see if the
                // primary's standby has changed.
                h = history[j];
                // we only care about events associated with the primary.
                if (h.ip === primary.ip) {
                    if (h.action === 'NewStandby') {
                        primary.slave = h.slave;
                        break;
                    } else if (h.action === 'ExpiredStandby') {
                        primary.slave = null;
                        break;
                    }
                }
            }

            log.info({primary: h},
                     'Shard._canBePrimary.getLastPrimary: exiting');
            return _cb(null, primary);
        });
    }

    var canBePrimary = false;
    // MANATEE-207: Distinguish between a virgin manatee instance. Otherwise
    // with the fixes to MANATEE-207 a 4th manatee can never join.
    var isVirgin = false;
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
             */
            if (!_.role || !_.role.role) {
                canBePrimary = true;
                isVirgin = true;
                return _cb();
            } else if (_.role.role === 'async' || _.role.role === 'potential') {
                // bail right away if we are an async
                canBePrimary = false;
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
                        canBePrimary = true;
                        return _cb();
                    }
                    /*
                     * If the previous primary was me or I was the primary's
                     * standby, then I can become the primary.
                     */
                    var myIp = self._ip + ':' + self._postgresPort;
                    if (primary.ip === myIp ||
                        /*
                         * MANATEE-186
                         * Backwards compatibility -- in older versions we
                         * didn't append the port number along with the ip
                         * address of the primary ip. So we also check whether
                         * the IPs would be identical with the port appended.
                         */
                        (primary.ip + ':' + self._postgresPort) === myIp) {
                        canBePrimary = true;
                        return _cb();
                    } else if (primary.slave && primary.slave === myIp) {
                        canBePrimary = true;
                        return _cb();
                    } else {
                        canBePrimary = false;
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
        log.info({err: err, canBePrimary: canBePrimary},
            'Shard.canBePrimary: exiting');

        return cb(err, canBePrimary, isVirgin);
    });
};

/**
 * @callback Shard-canNodeBePrimaryCb
 * @param {Error} err
 * @param {bool} canBePrimary
 */

/**
 * Checks whether another node can be the primary of the shard.
 *
 * We determine whether a particular node can be primary by checking whether it
 * was the last sync standby or primary of the shard. This is determined by
 * checking against the shard history in ZK.
 *
 * @param {string} ip The ip of the node.
 * @param {Shard-canNodeBePrimaryCb} callback
 */
Shard.prototype._canNodeBePrimary = function (ip, cb) {
    var self = this;
    var log = self._log;
    assert.string(ip, 'ip');
    log.info({ip: ip}, 'Shard._canNodeBePrimary: entering');
    /*
     * make the IP which looks like 127.0.0.1:30002:30001-0000000002
     * into 127.0.0.1:30002
     */
    ip = ip.split(':').splice(0, 2).join(':');
    cb = once(cb);

    self._getShardHistory(function (err, history) {
        if (err) {
            return cb(new verror.VError(err),
                      'Shard._canNodeBePrimary: can not get shard history');
        }

        var primary, sync;
        var canBePrimary = false;

        /*
         * the history should be sorted, check from last to first and look
         * for the last primary. history looks like:
         * {
         *   "time": "1382288491791",
         *    "ip": "10.77.77.23:5432",
         *    "action": "NewStandby",
         *    "role": "Leader",
         *    "master": "",
         *    "slave": "10.77.77.24:5432",
         *    "zkSeq": "0000000388"
         * }
         */
        for (var i = history.length -1; i >= 0; i--) {
            var h = history[i];
            if (!primary && h.action === 'AssumeLeader') {
                log.info({
                    ip: ip,
                    primary: h
                }, 'Shard._canNodeBePrimary: got primary');
                primary = h;
                break;
            }
            /*
             * only check for the new sync if we haven't gotten a primary or a
             * sync. Otherwise, if a primary already exists and we haven't
             * gotten a sync yet, then the sync will either be part of the
             * primary record or one does not exist.
             */
            if (!primary && !sync && h.action === 'NewStandby') {
                log.info({
                    ip: ip,
                    sync: h
                }, 'Shard._canNodeBePrimary: got sync');
                sync = h;
            }
        }
        if (ip === primary.ip) {
            canBePrimary = true;
        } else if (sync && sync.ip === primary.ip && ip === sync.slave) {
            // check if the standby was actually slaving from the primary.
            canBePrimary = true;
        } else if (!sync && primary.slave === ip) {
            /*
             * if the primary assumed leadership after its sync was updated,
             * then we will only have the assumeLeader event, hence we just
             * check the primary's slave against the ip of the node.
             */
            canBePrimary = true;
        }
        log.info({ip: ip, canBePrimary: canBePrimary},
                 'Shard.canNodeBePrimary: exiting');
        return cb(null, canBePrimary);
    });
};

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
 * @param {Shard-getshardhistorycb} cb
 */
Shard.prototype._getShardHistory = function (cb) {
    var self = this;
    var log = self._log;
    log.info('Shard._getShardHistory: entering');

    var history;

    vasync.pipeline({funcs: [
        function listHistory(arg, _cb) {
            var _path = self._shardPath + '/history';
            log.debug({path: _path}, 'listing zk path');
            self._zk.getChildren(_path, function (err, nodes) {
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
                            node.time = entry;
                            break;
                        case 1:
                            node.ip = entry;
                            break;
                        case 2:
                            node.action = entry;
                            break;
                        case 3:
                            node.role = entry;
                            break;
                        case 4:
                            node.master = entry;
                            break;
                        case 5:
                            node.slave = entry;
                            break;
                        case 6:
                            node.zkSeq = entry;
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
};

/**
 * Set the listeners on the zk election object.
 */
Shard.prototype._setZKElectionListeners = function () {
    var self = this;
    var log = self._log;
    self._election.on('gleader', function () {
        log.info('Shard: gleader event');

        self._transitionQueue.push({
            invoke: self.TRANSITION_FUNCS.LEADER,
            ctx: {
                self: self
            },
            transition: 'leader'
        });
    });

    self._election.on('leader', function (myLeader) {
        log.info({newLeader: myLeader}, 'Shard: newLeader event');
        self._transitionQueue.push({
            invoke: self.TRANSITION_FUNCS.NEW_LEADER,
            ctx: {
                self: self,
                leader: myLeader
            },
            transition: 'newLeader'
        });
    });

    self._election.on('follower', function (standby) {
        log.info({
            standby: standby
        }, 'Shard: follower event');
        if (!standby) {
            log.info({standby: self._currStandby}, 'Shard: standby expired.');

            self._transitionQueue.push({
                invoke: self.TRANSITION_FUNCS.EXPIRED,
                ctx: {
                    self: self
                },
                transition: 'expired'
            });
        } else {
            log.info({standby: standby}, 'Shard: got new standby');

            self._transitionQueue.push({
                invoke: self.TRANSITION_FUNCS.NEW_STANDBY,
                ctx: {
                    self: self,
                    standby: standby
                },
                transition: 'newStandby'
            });
        }
    });

};

/**
 * Initialize this node for entry into the shard. Only invoked when instanting
 * this node.
 *
 * @param {Shard-cb} callback
 *
 * @throws {verror.VError} If there is an error with the ZK client.
 */
Shard.prototype._init = function (callback) {
    var self = this;
    assert.func(callback, 'callback');
    var log = self._log;
    log.info('Shard._init: entering');
    vasync.pipeline({
        funcs: [
            function _createZk(_, _cb) {
                log.info({
                    zkCfg: self._zkCfg
                }, 'Shard._init: creating zk client');
                _cb = once(_cb);
                self._zk = zk.createClient(self._zkCfg.connStr,
                                           self._zkCfg.opts);
                self._zk.once('connected', function () {
                    log.info('Shard._init: zk client connected');
                    return _cb();
                });

                self._zk.on('error', function (err4) {
                    self._log.fatal({err: err4}, 'zk error, exiting');
                    throw new verror.VError(err4, 'zk error');
                });

                self._zk.on('disconnected', function () {
                    self._log.fatal('zk disconnected, time to go!');
                    throw new Error('zk disconnected');
                });

                self._zk.connect();
                //TODO: add logic to handle session timeouts and discconects.
            },
            function _initAndCleanup(_, _cb) {
                self._zkInitAndCleanupZkNodes(function (err) {
                    if (err) {
                        err = new verror.VError(err);
                    }
                    return _cb(err);
                });
            },
            function _checkWatchErr(_, _cb) {
                self._checkAndWatchError(function (err) {
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
                self._waitOrStart(function (err) {
                    if (err) {
                        err = new verror.VError(err);
                    }
                    return _cb(err);
                });
            },
            function _joinElection(_, _cb) {
                self._election = leader.createElection({
                    zk: self._zk,
                    path: self._shardElectionPath,
                    pathPrefix: self._electionPrefix
                }, _cb);
                self._election.on('error', function (err) {
                    throw new verror.VError(err, 'got election error');
                });
            },
            function _setListeners(_, _cb) {
                self._setZKElectionListeners();

                return _cb();
            },
            function _vote(_, _cb) {
                var data = JSON.stringify({
                    zoneId: self._zoneId,
                    ip: self._ip,
                    pgUrl: self._postgresMgr.getUrl().href,
                    backupUrl: self._backupUrl
                });
                self._election.vote(data, _cb);
            }
        ],
        args: {}
    }, function (err) {
        log.info({err: err}, 'Shard.entering: exiting');
        return callback(err);
    });
};

/**
 * Check whether we were an async, if so, then wait until there are others in
 * the shard before returning.
 *
 * @param {Shard-cb} cb
 */
Shard.prototype._waitOrStart = function (cb) {
    var self = this;
    assert.func(cb, 'callback');
    assert.object(self, 'self');

    var log = self._log;
    cb = once(cb);
    log.info('Shard._waitForStart: entering');

    function waitForAnotherNode() {
        self._zk.getChildren(
            self._shardElectionPath,
            function watch(event) {
                if (!cb.called) {
                    waitForAnotherNode();
                }
            },
            function (err, nodes) {
                if (err) {
                    return cb(new verror.VError(err));
                }
                if (nodes.length > 0) {
                    log.info('Shard._waitForStart: ' +
                             'other nodes are in shard, can start');
                    return cb();
                }
            });
    }

    self._postgresMgr.canStartAsMaster(function (can) {
        if (can) {
            log.info('Shard._waitForStart: was not async, can start');
            return cb();
        } else {
            waitForAnotherNode();
        }
    });
};

/**
 * Writes an error node in ZK if we detect the shard to be in error mode. The
 * error node is written under /shardpath/error and contains the primary, and
 * standby IPs of the shard.
 *
 * @param {string} standby The standby that caused this shard to enter error
 * mode.
 * @param {Shard-cb} callback
 */
Shard.prototype._writeErrorNode = function (standby, callback) {
    var self = this;
    assert.func(callback, 'callback');
    assert.object(self, 'self');
    assert.string(standby, 'standby');
    var log = self._log;

    log.info({
        standby: standby
    }, 'entering shard._writeErrorNode');
    var standbyUrl = url.parse(standby).hostname;

    var opts = new Buffer(JSON.stringify({
        object: {
            primary: self._ip,
            zoneId: self._zoneId,
            standby: standbyUrl
        }
    }));

    log.info({
        error: opts.object
    }, 'shard._writeErrorNode: writing error node to zk');

    self._zk.create(self._safeModePath, opts, function (err) {
        return callback(err);
    });
};

/**
 * Write a node under the history path about a particular transition.
 *
 * @param {object} opts Options.
 * @param {string} opts.role Role of this node.
 * @param {string} opts.action Transition action.
 * @param {string} [opts.standby] URL of the standby of this node.
 * @param {string} [opts.leader] URL of the leader of this node.
 * @param {Shard-cb} callback
 */
Shard.prototype._writeHistoryNode = function (opts, callback) {
    var self = this;
    assert.func(callback, 'callback');
    assert.object(self, 'self');
    assert.object(opts, 'opts');
    assert.string(opts.role, 'opts.role');
    assert.string(opts.action, 'opts.action');

    var log = self._log;
    log.info({
        opts: opts
    }, 'Shard._writeHistoryNode: entering');

    // parse the urls and pull out the hostname
    if (opts.leader) {
        opts.leader = url.parse(opts.leader).host;
    }
    if (opts.standby) {
        opts.standby = url.parse(opts.standby).host;
    }

    var _path = self._shardHistoryPath + '/' + Date.now() + '-' + self._ip +
        ':' + self._postgresPort + '-' + opts.action + '-' + opts.role + '-' +
        opts.leader + '-' + opts.standby + '-';

    log.info({
        path: _path
    }, 'Shard._writeHistoryNode: writing node');

    self._zk.create(_path, zk.CreateMode.PERSISTENT_SEQUENTIAL, callback);
};

/**
 * Check and watch the error node. If the error znode exists, put the node in
 * safe mode.
 *
 * @param {Shard-cb} callback
 */
Shard.prototype._checkAndWatchError = function (callback) {
    var self = this;
    var log = self._log;

    callback = once(callback);

    log.info({
        shardErrorPath: self._safeModePath
    }, 'Shard._checkAndWatchError: entered');

    function watchForError() {
        self._zk.getChildren(
            self._shardPath,
            function watch(event) {
                // rewatch
                watchForError();
            },
            function (err, children) {
                log.info({
                    err: err,
                    children: children
                }, 'Shard._checkAndWatchError: returned from getChildren');
                if (err) {
                    err = new verror.VError(
                        err, 'unable to check and watch for safe mode state');
                    // not the first time, so we are async and must throw.
                    if (callback.called) {
                        throw err;
                    } else {
                        log.info('exiting shard._checkAndWatchError');
                        return callback(err);
                    }
                }
                children.forEach(function (child) {
                    if (child === 'error') {
                        log.fatal('got shard error, going to safe mode');
                        self._safeMode();
                    }
                });

                return callback();
            });
    }

    watchForError();
};

/**
 * This function gets called if something catestrophic has happened in the
 * shard. Usually this is a result of out of sync xlogs between the primary and
 * the slave. Postgres can not make forward progress, and we do not want to
 * take writes in this degraded state.  We put the node in read only mode and
 * remove all listeners and wait for an operator to manually clear the shard.
 *
 * @throws {verror.VError} If unable to transition to read only mode.
 */
Shard.prototype._safeMode = function () {
    var self = this;
    var log = self._log;
    log.fatal('entering shard._safeMode: ' +
              'SHARD IS IN READ-ONLY SAFE MODE. OPERATOR INTERVENTION NEEDED!');
    // remove all listeners
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
        if (err) {
            throw new verror.VError(err, 'unable to transition to ro mode');
        }
    });
};

/**
 * Delete any previous emphemeral zk-nodes belonging to this pg node.
 *
 * @param {shard-cb} callback
 */
Shard.prototype._zkInitAndCleanupZkNodes = function (callback) {
    var self = this;
    var log = self._log;

    log.info({
        shardElectionPath: self._shardElectionPath,
        ip: self._ip
    }, 'Shard._zkInitAndCleanupZkNodes');

    vasync.pipeline({funcs: [
        function mkdirpElectionPath(_, cb) {
            self._zk.mkdirp(self._shardElectionPath, function (err) {
                log.debug({
                    path: self._shardElectionPath,
                    err: err
                }, 'returned from mkdirpElectionPath');
                cb(err);
            });
        },
        function mkdirpHistoryPath(_, cb) {
            self._zk.mkdirp(self._shardHistoryPath, function (err) {
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
            self._zk.getChildren(self._shardElectionPath,
                                 function (err, nodes) {
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
                // node looks like ip:pgport:bport-zkseq, so delete
                // anything that matches our election prefix
                if (node.split('-')[0] === self._electionPrefix) {
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
                /* jshint loopfunc: true */
                self._zk.remove(spath, function (err) {
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
    ], arg: {}}, function (err) {
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
};

/**
 * Write the details of this registrar into the ZK registrar node so it's
 * registered in DNS.
 *
 * @param {shard-cb} callback
 */
Shard.prototype._writeRegistrar = function (callback) {
    var self = this;
    var log = self._log;

    var data = JSON.stringify({
        type: 'database',
        database: {
            primary: self._postgresMgr.getUrl().href,
            ttl: self._ttl
        }
    });

    log.info({
        registrarPath: self._registrarPath,
        url: self._postgresMgr.getUrl().href,
        data: data
    }, 'entering write registrar');

    vasync.pipeline({funcs: [
        function mkdirp(_, cb) {
            self._zk.mkdirp(self._registrarPathPrefix, function (err) {
                if (err)  {
                    err = new verror.VError(err);
                }
                cb(err);
            });
        },
        function rm(_, cb) {
            self._zk.remove(self._registrarPath, function () {
                // don't check for err on rmr, it could fail if the node DNE
                cb();
            });
        },
        function create(_, cb) {
            self._zk.create(self._registrarPath, new Buffer(data),
                      zk.CreateMode.EPHEMERAL, function (err) {
                if (err)  {
                    err = new verror.VError(err);
                }
                cb(err);
            });
        }
    ], arg: {}}, function (err) {
        if (err) {
            log.error('Unable to write registrar');
            return callback(err);
        } else {
            log.info('finished writing registrar');
            return callback();
        }
    });

    return true;
};

/**
 * transform an zk election node name into a postgres url.
 * @param {string} zkNode The zknode, e.g.
 * 10.77.77.9:pgPort:backupPort:hbPort-0000000057
 *
 * @return {string} The transformed pg url, e.g.
 * tcp://postgres@10.0.0.0:5432/postgres
 */
Shard.prototype._transformPgUrl = function (zkNode) {
    var data = zkNode.split('-')[0].split(':');
    return 'tcp://postgres@' + data[0] + ':' + data[1] + '/postgres';
};

/**
 * transform an zk election node name into a backup server url.
 * @param {string} zkNode The zknode, e.g.
 * 10.77.77.9:pgPort:backupPort:hbPort-0000000057
 *
 * @return {string} The transformed backup server url, e.g.
 * http://10.0.0.0:5432
 */
Shard.prototype._transformBackupUrl = function (zkNode) {
    var data = zkNode.split('-')[0].split(':');
    return 'http://' + data[0] + ':' + data[2];
};

/** #@- */
