/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2016, Joyent, Inc.
 */

/**
 * @overview Administration library.
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
var bignum = require('bignum');
var bunyan = require('bunyan');
var exec = require('child_process').exec;
var fs = require('fs');
var once = require('once');
var path = require('path');
var pg = require('pg');
var progbar = require('progbar');
var prompt = require('prompt');
var restify = require('restify');
var sprintf = require('extsprintf').sprintf;
var util = require('util');
var vasync = require('vasync');
var VError = require('verror');
var zfs = require('./zfsClient');
var zk = require('node-zookeeper-client');

var LOG = bunyan.createLogger({
    name: 'manatee-adm',
    level: (process.env.LOG_LEVEL || 'fatal'),
    src: true,
    serializers: {
        err: bunyan.stdSerializers.err
    }
});

exports.checkLock = checkLock;
exports.freeze = freeze;
exports.unfreeze = unfreeze;
exports.history = history;
exports.rebuild = rebuild;
exports.reap = reap;
exports.setOnwm = setOnwm;
exports.zkActive = zkActive;
exports.zkState = zkState;
exports.stateBackfill = stateBackfill;
exports.status = status;
exports.loadClusterDetails = loadClusterDetails;


// Constants
var PG_REPL_STAT = 'select * from pg_stat_replication;';
var PG_REPL_LAG = 'SELECT now() - pg_last_xact_replay_timestamp() AS time_lag;';
var SPINNER = ['-', '\\', '|', '/'];

// Pipeline Functions

/**
 * @param {object} opts.zk The zk config
 * @param {function} cb
 *
 * @return {object} opts.zkClient A newly connected zookeeper client
 */
function _createZkClient(_, _cb) {
    createZkClient(_.zk, function (err, c) {
        _.zkClient = c;
        return _cb(err);
    });
}


/**
 * @param {object} opts.zkClient The zk config
 */
function _closeZkClient(_) {
    if (_.zkClient) {
        _.zkClient.removeAllListeners();
        _.zkClient.close();
    }
}


/**
 * @param {object} opts.zkClient The zk client
 * @param {function} cb
 *
 * @return {object} opts.shards A string array of all shards, either taken from
 * opts.shard or from zookeeper
 */
function _getShards(_, _cb) {
    _cb = once(_cb);
    _.shards = [];
    if (_.shard) {
        _.shards.push(_.shard);
        return (_cb());
    } else  {
        _.zkClient.getChildren('/manatee', function (err, s) {
            if (err) {
                return _cb(err);
            }
            _.shards = s;
            return (_cb());
        });
    }
}


/**
 * @param {string} opts.shard The shard
 * @param {object} opts.zkClient The zk client
 * @param {function} cb
 *
 * @return {object} opts.state
 */
function _getState(_, _cb) {
    var p = '/manatee/' + _.shard + '/state';
    _.zkClient.getData(p, function (err, data, stat) {
        if (err) {
            return (_cb(err));
        }
        _.state = JSON.parse(data.toString('utf8'));
        _.stateStat = stat;
        return (_cb());
    });
}

/**
 * @param {string} opts.shard The shard
 * @param {object} opts.zkClient The zk client
 * @param {object} opts.state The state to put
 * @param {function} cb
 *
 * @return {object} opts.state
 */
function _putState(_, _cb) {
    var p = '/manatee/' + _.shard + '/state';
    var data = new Buffer(JSON.stringify(_.state, null, 0));
    var historyPath = '/manatee/' + _.shard + '/history/0-';
    var hdata = new Buffer(JSON.stringify(_.state));
    _.zkClient.transaction().
        setData(p, data, _.stateStat.version).
        create(historyPath, hdata, zk.CreateMode.PERSISTENT_SEQUENTIAL).
        commit(function (err) {
            return (_cb(err));
        });
}

/**
 * @param {string} opts.shards The set of shards
 * @param {object} opts.zkClient The zk client
 * @param {function} cb
 *
 * Returns a series of cluster state objects, one for each shard.  For example:
 *
 *     {
 *         "1.moray.coal.joyent.us": {
 *             "primary": {
 *                 "ip": "127.0.0.1",
 *                 ...
 *             },
 *             "sync": <same as above...>
 *             "async": [
 *                 <same as above>, ...
 *             ]
 *          },
 *          "2.moray.coal.joyent.us": {
 *              ...
 *          },
 *          ...
 *     }
 *
 * @return {object} opts.state
 */
function _getClusterStates(_, _cb) {
    function getState(shard, _subcb) {
        var shardPath = '/manatee/' + shard + '/state';
        _.zkClient.getData(shardPath, function (err, sbuffer) {
            if (err) {
                return (_cb(err));
            }
            if (!_.state) {
                _.state = {};
            }
            _.state[shard] = JSON.parse(new Buffer(sbuffer).toString('utf8'));
            return (_subcb());
        });
    }
    vasync.forEachParallel({
        'func': getState,
        'inputs': _.shards
    }, _cb);
}


/**
 * Manatee originally worked by deriving the topology from the order of nodes
 * as they joined ZK.  I'm calling this "legacy" mode.  This function reads what
 * is in the election path and presets that as if it were the correct topology
 * even though the cluster state is the authoritative topology.
 *
 * See the _getClusterStates function for what the return object looks like.
 *
 * @param {string} opts.shards The set of shards
 * @param {object} opts.zkClient The zk client
 * @param {function} cb
 *
 * @return {object} opts.state See _getClusterStates
 */
function _getLegacyClusterStates(opts, _cb) {
    function compareNodeNames(a, b) {
        var seqA = parseInt(a.substring(a.lastIndexOf('-') + 1), 10);
        var seqB = parseInt(b.substring(b.lastIndexOf('-') + 1), 10);

        return (seqA - seqB);
    }

    vasync.pipeline({ 'funcs': [
        function _getShardChildren(_, _subcb) {
            _.children = {};
            _subcb = once(_subcb);
            var barrier = vasync.barrier();

            barrier.on('drain', _subcb);

            _.shards.forEach(function (shard) {
                barrier.start(shard);
                // shard is just the implicit znode name, so we have to
                // prepend the path prefix.
                var p = '/manatee/' + shard + '/election';
                _.zkClient.getChildren(p, function (err1, ch) {
                    if (err1) {
                        return _subcb(err1);
                    }
                    ch.sort(compareNodeNames);
                    _.children[shard] = ch;
                    barrier.done(shard);
                });
            });
        },
        function _getPeerState(_, _subcb) {
            _.state = {};
            _subcb = once(_subcb);
            var barrier = vasync.barrier();
            barrier.on('drain', function () {
                return _subcb();
            });
            Object.keys(_.children).forEach(function (shard) {
                if (!_.state[shard]) {
                    _.state[shard] = {};
                }
                // in case the shard is empty, we set a barrier so we exit.
                barrier.start(shard);
                _.children[shard].forEach(function (peer, i) {
                    var p = '/manatee/' + shard + '/election/' + peer;
                    var peerName;
                    var pos = 0;
                    switch (i) {
                        case 0:
                            peerName = 'primary';
                            break;
                        case 1:
                            peerName = 'sync';
                            break;
                        default:
                            peerName = 'async';
                            pos = i - 2;
                            break;
                    }
                    barrier.start(shard + peerName + pos);
                    _.zkClient.getData(p, function (err, data) {
                        if (err) {
                            return _subcb(err);
                        }
                        data = JSON.parse(data.toString());
                        //Since the old structures don't contain a backupUrl,
                        // add that if one doesn't exist.
                        var bu = transformBackupUrl(peer);
                        data.backupUrl = data.backupUrl ? data.backupUrl : bu;

                        //Also add the id.
                        data.id = peer.substring(0, peer.lastIndexOf('-'));

                        if (['primary', 'sync'].indexOf(peerName) !== -1) {
                            _.state[shard][peerName] = data;
                        } else {
                            if (!_.state[shard][peerName]) {
                                _.state[shard][peerName] = [];
                            }
                            _.state[shard][peerName][pos] = data;
                        }
                        barrier.done(shard + peerName + pos);
                    });
                });
                barrier.done(shard);
            });
        },
        function _getError(_, _subcb) {
            _subcb = once(_subcb);
            var barrier = vasync.barrier();
            barrier.on('drain', function () {
                return _subcb();
            });
            _.shards.forEach(function (shard) {
                // in case the shard is empty, we set a barrier so we exit.
                barrier.start(shard);
                var p = '/manatee/' + shard + '/error';
                _.zkClient.getData(p, function (err, data) {
                    if (err && err.code !== zk.Exception.NO_NODE) {
                        return _subcb(err);
                    }
                    if (data) {
                        _.state[shard].error = JSON.parse(
                            data.toString('utf8'));
                    }
                    barrier.done(shard);
                });
            });
        }
    ], 'arg': opts }, _cb);
}


/**
 * Adds postgres state to cluster state objects.
 *
 * @param {string} opts.state A cluster state object (see above).
 * @param {function} cb
 *
 * @return {object} opts.state See _getClusterStates
 */
function _addPostgresStatus(_, _cb) {
    _cb = once(_cb);
    var barrier = vasync.barrier();
    barrier.on('drain', function () {
        return _cb();
    });
    Object.keys(_.state).forEach(function (shard) {
        // in case the shard is empty, we set a barrier so we exit.
        barrier.start(shard);
        var peers = [];
        var roles = [];
        if (_.state[shard]['primary']) {
            peers.push(_.state[shard]['primary']);
            roles.push('primary');
        }
        if (_.state[shard]['sync']) {
            peers.push(_.state[shard]['sync']);
            roles.push('sync');
        }
        if (_.state[shard]['async']) {
            peers = peers.concat(_.state[shard]['async']);
            //Janky...
            roles = roles.concat(_.state[shard]['async'].map(function () {
                return ('async');
            }));
        }
        peers.forEach(function (entry, i) {
            var pgUrl = entry.pgUrl;
            var peer = roles[i];
            barrier.start(pgUrl);
            queryPg(pgUrl, PG_REPL_STAT, function (err, res) {
                if (err) {
                    entry.error = JSON.stringify(err);
                    entry.online = false;
                    barrier.done(pgUrl);
                } else {
                    entry.online = true;
                    entry.repl = res.rows[0] ? res.rows[0] : {};
                    if (peer !== 'primary' && peer !== 'sync') {
                        queryPg(pgUrl, PG_REPL_LAG,
                                function (err2, res2) {
                                    if (err2) {
                                        entry.error = JSON.stringify(err2);
                                    } else {
                                        entry.lag = res2.rows[0] ?
                                            res2.rows[0] : {};

                                        /*
                                         * The time lag is a time duration value
                                         * in postgres.  node-postgres returns
                                         * these values as an object with
                                         * properties for seconds, minutes, and
                                         * so on, but any zero properties are
                                         * elided.  As a result, an empty object
                                         * denotes a zero interval.  This is
                                         * confusing for operators, so we always
                                         * fill in minutes and seconds with
                                         * explicit zeros if they're not
                                         * present.
                                         */
                                        if (entry.lag.time_lag) {
                                            if (!entry.lag.time_lag.minutes)
                                                entry.lag.time_lag.minutes = 0;
                                            if (!entry.lag.time_lag.seconds)
                                                entry.lag.time_lag.seconds = 0;
                                        }
                                    }

                                    barrier.done(pgUrl);
                                });
                    } else {
                        barrier.done(pgUrl);
                    }

                }
            });
        });
        barrier.done(shard);
    });
}

/**
 * Fills in postgres state like _addPostgresStatus, but without contacting
 * postgres (by filling in with an error).
 *
 * @param {string} opts.state A cluster state object (see above).
 * @param {function} cb
 *
 * @return {object} opts.state See _getClusterStates
 */
function _fillEmptyPostgresStatus(_, _cb) {
    Object.keys(_.state).forEach(function (shard) {
        var peers = [];
        if (_.state[shard]['primary']) {
            peers.push(_.state[shard]['primary']);
        }
        if (_.state[shard]['sync']) {
            peers.push(_.state[shard]['sync']);
        }
        if (_.state[shard]['async']) {
            peers = peers.concat(_.state[shard]['async']);
        }
        if (_.state[shard]['deposed']) {
            peers = peers.concat(_.state[shard]['deposed']);
        }

        peers.forEach(function (entry, i) {
           entry.online = false;
           entry.error = JSON.stringify({
               'name': 'Error',
               'message': 'postgres state not fetched for peer'
           });
        });
    });
}


/**
 * Formats the state object for display, unrolling the async array into
 * top-level members of the map.  Primary and sync stay the same, each of the
 * asyncs after the first have a number appended, in order.
 *
 * @param {string} opts.state A cluster state object (see above).
 * @param {function} cb
 *
 * @return {object} opts.formattedState See _getClusterStates
 */
function _formatState(_, _cb) {
    _.formattedState = {};
    Object.keys(_.state).forEach(function (shard) {
        _.formattedState[shard] = {};
        if (_.state[shard].freeze) {
            var f = _.state[shard].freeze;
            _.formattedState[shard]['__FROZEN__'] = f.date + ': ' +
                f.reason;
        }
        if (_.state[shard].primary) {
            _.formattedState[shard].primary = _.state[shard].primary;
        }
        if (_.state[shard].sync) {
            _.formattedState[shard].sync = _.state[shard].sync;
        }
        if (_.state[shard].async) {
            _.state[shard].async.forEach(function (e, i) {
                _.formattedState[shard]['async' + (i === 0 ? '' : i)] = e;
            });
        }
        if (_.state[shard].deposed) {
            _.state[shard].deposed.forEach(function (e, i) {
                _.formattedState[shard]['deposed' + (i === 0 ? '' : i)] = e;
            });
        }
    });
    setImmediate(_cb());
}


/**
 * @param {Object} opts The options object.
 * @param {Object} opts.zkClient The zk client config.
 * @param {String} opts.shard The name of the shard.
 *
 * @return {object} opts.activeData The active children.
 */
function _active(opts, _cb) {
    var p = '/manatee/' + opts.shard + '/election';
    vasync.pipeline({ funcs: [
        function listChildren(_, cb) {
            _.zkClient.getChildren(p, function (err, ch) {
                if (err) {
                    return (cb(err));
                }
                _.children = ch;
                return (cb());
            });
        },
        function fetchChildren(_, cb) {
            vasync.forEachParallel({
                'inputs': _.children,
                'func': function (c, _subcb) {
                    var pt = p + '/' + c;
                    _.zkClient.getData(pt, function (err, d) {
                        if (err) {
                            return (_subcb(err));
                        }
                        return (_subcb(null, JSON.parse(d.toString('utf8'))));
                    });
                }
            }, function (err, res) {
                if (err) {
                    return (cb(err));
                }
                _.activeData = {};
                res.operations.forEach(function (o, i) {
                    _.activeData[_.children[i]] = o.result;
                });
                return (cb());
            });
        }
    ], arg: opts}, function (err) {
        if (err && err.name === 'NO_NODE') {
            err = new Error('No state exists for shard ' + opts.shard);
        }
        return (_cb(err));
    });
}


/**
 * @param {Object} opts The options object.
 * @param {Object} opts.config The sitter config for this node.
 * @param {Object} opts.activeData The active ZK children.
 *
 * @return {Object} opts.activeZkNode The local peer's ZK object if active in
 * ZK, null if not active.
 */
function _setActiveZkNode(opts, _cb) {
    var active = null;
    Object.keys(opts.activeData).forEach(function (k) {
        var n = opts.activeData[k];
        if (n.zoneId === opts.config.zoneId) {
            active = n;
        }
    });
    opts.activeZkNode = active;
    return (_cb());
}


/**
 * Returns an object summarizing the complete state of this Manatee cluster
 * (shard).  Logically, this is a JavaScript representation that combines both
 * the cluster state stored in ZooKeeper and the Postgres state of each peer.
 * This is used to drive the "pg-status", "peers", "verify", and "show"
 * commands.
 *
 * The return value passed to "callback" is an object with:
 *
 *     pgs_peers        object mapping peer ids (strings) to objects with
 *                      details for each peer.  These objects contain:
 *
 *          pgp_label   display name for this peer
 *
 *          pgp_ident   "peer identifier" object described in the
 *                      manatee-state-machine repo.  The "id" field of this
 *                      object is used as the string peer identifier elsewhere
 *                      in this structure.
 *
 *          pgp_pgerr   if present, describes an error connecting to postgres on
 *                      this peer, which indicates that the rest of the status
 *                      is incomplete
 *
 *          pgp_repl    describes a postgres replication connection for which
 *                      this peer is the upstream.  This object will contain
 *                      fields from postgres's "pg_stat_replication" view.
 *
 *          pgp_lag     if this peer is the target of async replication,
 *                      indicates how long since data was received from
 *                      upstream.  Note that if no writes are being committed
 *                      upstream, this may grow large, and that's not
 *                      necessarily a problem.
 *
 *     pgs_generation   current cluster generation number
 *
 *     pgs_initwal      current cluster generation initial WAL value
 *
 *     pgs_primary      peer id (string) of primary peer
 *
 *     pgs_sync         peer id (string) of sync peer, if any.
 *                      may be null in singleton (one-node-write) mode
 *
 *     pgs_asyncs[]     array of peer ids of async peers
 *
 *     pgs_deposed[]    array of peer ids of deposed peers
 *
 *     pgs_frozen       boolean indicating whether the cluster is frozen
 *
 *     pgs_freeze_reason    if pgs_frozen, then this is a string describing why
 *                          the cluster is frozen
 *
 *     pgs_freeze_time      if pgs_frozen, then this is an ISO 8601 timestamp
 *                          describing when the cluster was frozen
 *
 *     pgs_singleton    whether the cluster is in singleton ("one-node-write")
 *                      mode
 *
 *     pgs_errors[]     array of Errors describing problems with the overall
 *                      health of the cluster that likely mean service is down
 *
 *     pgs_warnings[]   array of Errors describing problems with the cluster
 *                      that likely do not currently affect server.
 *
 * IMPLEMENTATION NOTE: See the IMPORTANT NOTE in the ManateeClusterDetails
 * constructor before making any changes to these fields.
 *
 * Arguments include:
 *
 * @param {Object} args The options object.
 * @param {Object} args.legacyOrderMode fetch state based v1.0 semantics.
 * @param {String} args.zk The zookeeper connection string.
 * @param {String} args.shard The manatee shard name.
 * @param {String} args.skipPostgres Skip postgres-level details.  Each peer's
 * pgp_pgerr will be non-null and other fields will be filled in similarly to
 * if we had failed to contact the corresponding postgres peer.
 */
function loadClusterDetails(args, cb)
{
    var loadinfo;

    assert.object(args, 'args');
    assert.optionalBool(args.legacyOrderMode, 'args.legacyOrderMode');
    assert.string(args.shard, 'args.shard');
    assert.string(args.zk, 'args.zk');
    assert.func(cb, 'cb');

    if (process.env['MANATEE_ADM_TEST_STATE']) {
        loadClusterTestState(args, cb);
        return;
    }

    loadinfo = {
        'legacyOrderMode': args.legacyOrderMode,
        'shard': args.shard,
        'zk': args.zk,
        'zkClient': null,   /* see _createZkClient */
        'shards': null,     /* see _getShards */
        'state': null,      /* see _getClusterStates */
        'fullState': null   /* see _constructClusterDetails */
    };

    vasync.pipeline({
        'arg': loadinfo,
        'funcs': [
            _createZkClient,
            _getShards,
            function getState(_, _cb) {
                if (args.legacyOrderMode) {
                    _getLegacyClusterStates(loadinfo, _cb);
                } else {
                    _getClusterStates(loadinfo, function (err) {
                        if (err && err.name === 'NO_NODE') {
                            err = new Error('no state object exists ' +
                                'for shard: "' + args.shard + '"');
                        }
                        return (_cb(err));
                    });
                }
            },
            function _postgresStatus(_, _cb) {
                if (args.skipPostgres) {
                    _fillEmptyPostgresStatus(_);
                    _cb();
                } else {
                    _addPostgresStatus(_, _cb);
                }
            },
            _constructClusterDetails
        ]
    }, function (err, results) {
        _closeZkClient(loadinfo);

        if (err) {
            cb(err);
            return;
        }

        cb(null, loadinfo.fullState);
    });
}

/*
 * The test suite is allowed to override the internal cluster details object for
 * testing the rest of the "manatee-adm" command.
 */
function loadClusterTestState(args, cb)
{
    var filename;

    filename = process.env['MANATEE_ADM_TEST_STATE'];
    assert.string(filename);
    fs.readFile(filename, function (err, contents) {
        var cs;

        if (err) {
            cb(err);
        } else {
            /*
             * This is not great, but the contract is basically that the test
             * suite has to construct an object that looks exactly like our
             * internal representation (which is at least well documented), so
             * here we treat it just like we had constructed it ourselves.
             */
            cs = JSON.parse(contents);
            cs.__proto__ = ManateeClusterDetails.prototype;
            cs.loadErrors.call(cs);
            cb(null, cs);
        }
    });
}

function _constructClusterDetails(loadinfo, cb)
{
    loadinfo.fullState = new ManateeClusterDetails(loadinfo);
    setImmediate(cb);
}

/*
 * This object represents an interface that will hopefully eventually become
 * public.  It's described under loadClusterDetails() above.
 * XXX it would be nice if the various functions in this file populated their
 * data from this structure rather than filling in this structure based on their
 * results.
 */
function ManateeClusterDetails(loadinfo)
{
    var self = this;
    var st;

    assert.object(loadinfo, 'loadinfo');
    assert.string(loadinfo.shard, 'loadinfo.shard');
    assert.arrayOfString(loadinfo.shards, 'loadinfo.shards');
    /*
     * We would have bailed out already if no shard was found, and the calling
     * interface doesn't support multiple shards.
     */
    assert.ok(loadinfo.shards.length == 1, 'exactly one shard');
    assert.object(loadinfo.state, 'loadinfo.state');
    assert.object(loadinfo.state[loadinfo.shard], 'shard state');

    /*
     * IMPORTANT NOTE: This class represents an interface between the Manatee
     * administrative client library and consumers (e.g., the "manatee-adm"
     * command).  This interface is not yet public or committed.
     *
     * Do NOT add, remove, or modify any fields in this class without updating
     * the corresponding documentation for loadClusterDetails() above and the
     * test suite.  When making changes, make sure that this object remains
     * immutable and be sure to support strict JavaScript conventions: fields
     * should either be present or "null" (not "undefined" or otherwise falsey).
     * Every field should have a manatee-adm consumer.
     */
    st = loadinfo.state[loadinfo.shard];
    this.pgs_peers = {};
    this.pgs_primary = st.primary.id;
    this.pgs_sync = st.sync === null ? null : st.sync.id;
    this.pgs_asyncs = st.async.map(function (p) { return (p.id); });
    this.pgs_deposed = (st.deposed) ?
        st.deposed.map(function (p) { return (p.id); }) : [];
    this.pgs_generation = st.generation;
    this.pgs_initwal = st.initWal;
    this.pgs_singleton = st.oneNodeWriteMode ? true : false;
    this.pgs_frozen = st.freeze ? true : false;

    if (this.pgs_frozen) {
        if (typeof (st.freeze) == 'object') {
            this.pgs_freeze_reason = st.freeze.reason;
            this.pgs_freeze_time = st.freeze.date;
        } else {
            this.pgs_freeze_reason = 'unknown';
            this.pgs_freeze_time = 'unknown';
        }
    } else {
        this.pgs_freeze_reason = null;
        this.pgs_freeze_time = null;
    }

    this.loadPeer(st.primary);
    if (this.pgs_sync !== null)
        this.loadPeer(st.sync);
    st.async.forEach(function (p) { self.loadPeer(p); });
    if (st.deposed) {
        st.deposed.forEach(function (p) { self.loadPeer(p); });
    }

    this.pgs_errors = [];
    this.pgs_warnings = [];
    this.loadErrors();
}

/*
 * Given one of the peer objects from the status output, populate an entry in
 * this.pgs_peers.
 */
ManateeClusterDetails.prototype.loadPeer = function (peerinfo)
{
    var identkeys = [ 'id', 'ip', 'pgUrl', 'zoneId', 'backupUrl' ];
    var peer = {};
    var parsed, err;

    peer.pgp_ident = {};
    identkeys.forEach(function (k) {
        assert.string(peerinfo[k], 'expected string for property ' + k);
        peer.pgp_ident[k] = peerinfo[k];
    });
    peer.pgp_label = peerinfo.zoneId.substr(0, 8);
    peer.pgp_repl = peerinfo.repl && peerinfo.repl.state ?
        peerinfo.repl : null;
    peer.pgp_lag = peerinfo.lag && peerinfo.lag.time_lag ?
        peerinfo.lag.time_lag : null;

    if (peerinfo.error) {
        if (typeof (peerinfo.error) == 'string') {
            parsed = JSON.parse(peerinfo.error);
            if (parsed.severity && parsed.file) {
                /* postgres error */
                err = new VError('postgres: ', peerinfo.error);
            } else if (parsed.message) {
                /* Friendly Node error */
                err = new VError('%s', parsed.message);
            } else if (parsed.errno) {
                /* Less friendly Node error */
                err = new VError('%s', parsed.errno);
            } else {
                /* Ugh. */
                err = new VError(peerinfo.error);
            }
        } else {
            err = new Error(peerinfo.error.toString());
        }

        peer.pgp_pgerr = new VError(err, 'peer "%s"', peer.pgp_label);
    } else {
        peer.pgp_pgerr = null;
    }

    this.pgs_peers[peerinfo.id] = peer;
};

ManateeClusterDetails.prototype.loadErrors = function ()
{
    var self = this;
    var p, s, c;

    p = this.pgs_peers[this.pgs_primary];
    if (p.pgp_pgerr) {
        this.pgs_errors.push(new VError(p.pgp_pgerr,
            'cannot query postgres on primary'));
    }

    if (this.pgs_singleton) {
        c = Object.keys(this.pgs_peers).length;
        if (c > 1) {
            this.pgs_warnings.push(new VError(
                'found %d peers in singleton mode', c));
        }
        return;
    }

    s = this.pgs_peers[this.pgs_sync];
    if (s.pgp_pgerr) {
        this.pgs_errors.push(new VError(s.pgp_pgerr,
            'cannot query postgres on sync'));
    }

    if (this.pgs_deposed.length > 0)
        this.pgs_warnings.push(new VError('cluster has a deposed peer'));

    if (this.pgs_asyncs.length === 0)
        this.pgs_warnings.push(new VError('cluster has no async peers'));

    /*
     * If the sync is down, that's all we can really check for now.
     */
    if (s.pgp_pgerr)
        return;

    this.loadReplErrors(p, this.pgs_sync, 'sync', this.pgs_errors);
    this.loadReplErrors(s,
        this.pgs_asyncs.length === 0 ? null : this.pgs_asyncs[0],
        'async', this.pgs_warnings);
    this.pgs_asyncs.forEach(function (a, i) {
        var peer, next;

        if (i < self.pgs_asyncs.length - 1)
            next = self.pgs_asyncs[i + 1];
        else
            next = null;

        peer = self.pgs_peers[a];
        self.loadReplErrors(peer, next, 'async', self.pgs_warnings);
    });
};

ManateeClusterDetails.prototype.loadReplErrors = function (peer, dspeerid,
    kind, errors)
{
    var preverrcount, dspeer;

    assert.object(peer, 'peer');
    assert.string(kind, 'kind');
    preverrcount = errors.length;

    if (dspeerid === null)
        return;

    assert.string(dspeerid, 'dspeerid');
    dspeer = this.pgs_peers[dspeerid];

    /*
     * If there's no replication state, bail out quickly and stop checking for
     * other problems.
     */
    if (peer.pgp_repl === null) {
        errors.push(new VError('peer "%s": downstream replication ' +
            'peer not connected', peer.pgp_label));
        return;
    }

    /*
     * Check the downstream peer IP and the replication state, which is
     * typically "catchup" or "streaming".  We intentionally do both of these
     * checks before returning to report as many relevant, orthogonal issues as
     * we can.
     */
    if (peer.pgp_repl.client_addr != dspeer.pgp_ident.ip) {
        errors.push(new VError('peer "%s": expected downstream peer ' +
            'to be "%s", but found "%s"', peer.pgp_label,
            dspeer.pgp_ident.ip, peer.pgp_repl.client_addr));
    }

    if (peer.pgp_repl.state != 'streaming') {
        errors.push(new VError('peer "%s": downstream replication not ' +
           'yet established (expected state "streaming", found "%s")',
           peer.pgp_label, peer.pgp_repl.state));
    }

    /*
     * Don't check anything else if streaming replication hasn't been
     * established yet.
     */
    if (errors.length > preverrcount)
        return;

    if (peer.pgp_repl.sync_state != kind) {
        errors.push(new VError('peer "%s": expected downstream replication ' +
            'to be "%s", but found "%s"', peer.pgp_label, kind,
            peer.pgp_repl.sync_state));
    }
};

// Operations

/**
 * Returns a JSON-format summary of the shard's status.
 *
 * @param {Object} opts The options object.
 * @param {Object} opts.legacyOrderMode Get state based on order of nodes in zk.
 * @param {String} opts.zk The zookeeper URL.
 * @param {String} [opts.shard] The manatee shard.
 */
function status(opts, cb) {
    vasync.pipeline({ funcs: [
        _createZkClient,
        _getShards,
        function getState(_, _cb) {
            if (opts.legacyOrderMode) {
                _getLegacyClusterStates(_, _cb);
            } else {
                _getClusterStates(_, function (err) {
                    if (err && err.name === 'NO_NODE') {
                        err = new Error('No state object exists for one or ' +
                                        'more shards: ' +
                                        opts.shards.join(', '));
                    }
                    return (_cb(err));
                });
            }
        },
        _addPostgresStatus,
        _formatState
    ], arg: opts}, function (err, results) {
        _closeZkClient(opts);
        return cb(err, opts.formattedState);
    });
}


/**
 * @param {Object} opts The options object.
 * @param {Object} opts.zk The zk client config.
 * @param {String} opts.shard The name of the shard.
 */
function zkState(opts, cb) {
    vasync.pipeline({ funcs: [
        _createZkClient,
        _getState
    ], arg: opts}, function (err, res) {
        _closeZkClient(opts);
        if (err && err.name === 'NO_NODE') {
            err = new Error('No state exists for shard ' + opts.shard);
        }
        return cb(err, opts.state);
    });
}


/**
 * @param {Object} opts The options object.
 * @param {Object} opts.zk The zk client config.
 * @param {String} opts.shard The name of the shard.
 */
function freeze(opts, cb) {
    vasync.pipeline({ funcs: [
        _createZkClient,
        _getState,
        function _freeze(_, _cb) {
            if (_.state.freeze) {
                return (_cb(new Error('shard is already been frozen: ' +
                                      _.state.freeze.reason)));
            }
            _.state.freeze = {
                'date': new Date().toISOString(),
                'reason': opts.reason
            };
            return (_cb());
        },
        _putState
    ], arg: opts}, function (err, res) {
        _closeZkClient(opts);
        if (err && err.name === 'NO_NODE') {
            err = new Error('No state exists for shard ' + opts.shard);
        }
        return cb(err, opts.data);
    });
}


/**
 * @param {Object} opts The options object.
 * @param {Object} opts.zk The zk client config.
 * @param {String} opts.shard The name of the shard.
 */
function unfreeze(opts, cb) {
    vasync.pipeline({ funcs: [
        _createZkClient,
        _getState,
        function _unfreeze(_, _cb) {
            if (!_.state.freeze) {
                return (_cb(new Error('shard is not frozen')));
            }
            delete _.state.freeze;
            return (_cb());
        },
        _putState
    ], arg: opts}, function (err, res) {
        _closeZkClient(opts);
        if (err && err.name === 'NO_NODE') {
            err = new Error('No state exists for shard ' + opts.shard);
        }
        return cb(err, opts.data);
    });
}


/**
 * @param {Object} opts The options object.
 * @param {Object} opts.zk The zk client config.
 * @param {String} opts.shard The name of the shard.
 * @param {String} opts.zonename The zonename of the node to undepose.
 * @param {String} opts.ip The ip of the node to undepose.
 */
function reap(opts, cb) {
    vasync.pipeline({ funcs: [
        _createZkClient,
        _getState,
        function _reap(_, _cb) {
            var index = -1;
            _.state.deposed.forEach(function (d, i) {
                if ((_.zonename && _.zonename === d.zoneId) ||
                    (_.ip && _.ip === d.ip)) {
                    index = i;
                }
            });
            if (index === -1) {
                var id = _.zonename || _.ip;
                return (_cb(new Error(id + ' not in deposed or does not ' +
                                      'exist')));
            }
            if (index != -1) {
                _.state.deposed.splice(index, 1);
            }
            return (_cb());
        },
        _putState
    ], arg: opts}, function (err, res) {
        _closeZkClient(opts);
        if (err && err.name === 'NO_NODE') {
            err = new Error('No state exists for shard ' + opts.shard);
        }
        return cb(err, opts.data);
    });
}


/**
 * @param {Object} opts The options object.
 * @param {Boolean} opts.mode True for enabled, false for disabled
 * @param {Boolean} opts.ignorePrompts Ignores prompts if set.
 * @param {Object} opts.zk The zk client config.
 * @param {String} opts.shard The name of the shard.
 */
function setOnwm(opts, cb) {
    vasync.pipeline({ funcs: [
        _createZkClient,
        _getState,
        function _noticeChange(_, _cb) {
            if (_.mode === 'on' && _.state.oneNodeWriteMode === true) {
                return (_cb(new Error('One node write mode already enabled')));
            }
            if (_.mode === 'off' && _.state.oneNodeWriteMode === undefined) {
                return (_cb(new Error('One node write mode already disabled')));
            }
            return (_cb());
        },
        function _confirm(_, _cb) {
            if (_.ignorePrompts) {
                return (_cb());
            }
            console.error([
                '!!! WARNING !!!',
                'Enabling or disable one node write mode requires cluster',
                'downtime.  One node write mode in your configuration must',
                'match what is set in the cluster state object in zookeeper.',
                'Please be very careful when enabling or disabling one node',
                'write mode.',
                '!!! WARNING !!!'
            ].join('\n'));
            prompt.get(['yes'], function (err, result) {
                if (err) {
                    return _cb(err);
                }

                if (result.yes !== 'yes' && result.yes !== 'y') {
                    console.error('aborted...');
                    return _cb(new VError('aborting cluster state ' +
                                          'backfill due to user command'));
                }
                return (_cb());
            });
        },
        function _setOnwm(_, _cb) {
            if (_.mode === 'on') {
                _.state.oneNodeWriteMode = true;
            } else {
                delete _.state.oneNodeWriteMode;
            }
            return (_cb());
        },
        _putState
    ], arg: opts}, function (err, res) {
        _closeZkClient(opts);
        if (err && err.name === 'NO_NODE') {
            err = new Error('No state exists for shard ' + opts.shard);
        }
        return cb(err, opts.data);
    });
}


/**
 * @param {Object} opts The options object.
 * @param {Object} opts.zk The zk client config.
 * @param {String} opts.shard The name of the shard.
 */
function zkActive(opts, cb) {
    vasync.pipeline({ funcs: [
        _createZkClient,
        _active
    ], arg: opts}, function (err, res) {
        _closeZkClient(opts);
        if (err && err.name === 'NO_NODE') {
            err = new Error('No state exists for shard ' + opts.shard);
        }
        return cb(err, opts.activeData);
    });
}


/**
 * @param {Object} opts The options object.
 * @param {Boolean} opts.ignorePrompts Ignores prompts if set.
 * @param {Object} opts.zk The zk client config.
 * @param {String} opts.shard The name of the shard.
 */
function stateBackfill(opts, cb) {
    var shardPath = '/manatee/' + opts.shard + '/state';
    var historyPath = '/manatee/' + opts.shard + '/history/0-';
    opts.noPostgres = true;
    vasync.pipeline({ funcs: [
        _createZkClient,
        function _verifyNoState(_, _cb) {
            _.zkClient.getData(shardPath, function (err, sbuffer) {
                if (err && err.name === 'NO_NODE') {
                    return (_cb());
                }
                if (err) {
                    return (_cb(err));
                }
                return (_cb(new Error('State already exists for shard ' +
                                      opts.shard)));
            });
        },
        _getShards,
        _getLegacyClusterStates,
        function _rearrangeState(_, _cb) {
            // Notice we're going to the inner object here...
            var stat = _.state[opts.shard];
            //Shift it all by one.
            if (stat.sync && stat.async && stat.async.length >= 1) {
                var newSync = stat.async.pop();
                stat.async.push(stat.sync);
                stat.sync = newSync;
            }
            if (!stat.sync) {
                stat.sync = null;
            }
            if (!stat.async) {
                stat.async = [];
            }
            stat.generation = 0;
            stat.initWal = '0/0000000';
            stat.freeze = {
                'date': new Date().toISOString(),
                'reason': 'manatee-adm state-backfill'
            };
            _.newState = stat;
            return (_cb());
        },
        function confirm(_, _cb) {
            console.error('Computed new cluster state:');
            console.error(_.newState);
            if (_.ignorePrompts) {
                return (_cb());
            }
            console.error('is this correct(y/n)');
            prompt.get(['yes'], function (err, result) {
                if (err) {
                    return _cb(err);
                }

                if (result.yes !== 'yes' && result.yes !== 'y') {
                    console.error('aborted...');
                    return _cb(new VError('aborting cluster state ' +
                                          'backfill due to user command'));
                }
                return (_cb());
            });
        },
        function writeState(_, _cb) {
            var hdata = new Buffer(JSON.stringify(_.newState));
            var data = new Buffer(JSON.stringify(_.newState));
            _.zkClient.transaction().
                create(historyPath, hdata,
                       zk.CreateMode.PERSISTENT_SEQUENTIAL).
                create(shardPath, data,
                       zk.CreateMode.PERSISTENT).
                commit(_cb);
        }
    ], arg: opts}, function (err, res) {
        _closeZkClient(opts);
        return cb(err, opts.newState);
    });
}

/**
 * @param {Object} opts The options object.
 * @param {String} opts.config The manatee sitter config.
 * @param {Boolean} opts.ignorePrompts Ignores prompts if set.
 */
function rebuild(opts, cb) {
    vasync.pipeline({ funcs: [
        _createZkClient,
        _getShards,
        function _findShard(_, _cb) {
            _.shard = path.basename(_.config.shardPath);
            if (_.shards.indexOf(_.shard) === -1) {
                return (_cb(new Error('unable to determine shards')));
            }
            return (_cb());
        },
        _getState,
        function _checkPrimary(_, _cb) {
            if (!_.state.primary) {
                return (_cb(new Error('no primary')));
            }
            return (_cb());
        },
        function _checkNotPrimary(_, _cb) {
            if (_.state.primary.zoneId === _.config.zoneId) {
                return (_cb(new Error('This node is the primary.  ' +
                                      'Will not rebuild.')));
            }
            return (_cb());
        },
        //Verify there isn't a zk node for this, otherwise, the primary won't
        // add it once the sitter is restarted.
        _active,
        _setActiveZkNode,
        function _checkDeposed(_, _cb) {
            _.removeFromDeposed = false;
            if (!_.state.deposed) {
                return (_cb());
            }
            var index = -1;
            _.state.deposed.forEach(function (n, i) {
                if (n.zoneId === _.config.zoneId) {
                    index = i;
                }
            });
            if (index !== -1) {
                _.deposedIndex = index;
                _.removeFromDeposed = true;
            }
            return (_cb());
        },
        function _warnActive(_, _cb) {
            /*
             * This will be true if the current node is active, and the node
             * is *not* deposed. At this point this could only be a sync or
             * an async.
             */
            if (_.activeZkNode && !_.removeFromDeposed) {
                var sec = 60;
                if (_.config.zkCfg.opts && _.config.zkCfg.opts.sessionTimeout) {
                    sec = _.config.zkCfg.opts.sessionTimeout / 1000;
                }
                return (_cb(new Error('There is an active ZK node for this ' +
                                      'host.  Please disable the sitter and ' +
                                      'let the ZK node timeout (may take up ' +
                                      'to ' + sec + ' seconds).')));
            }
            return (_cb());
        },
        function _promptConfirm(_, _cb) {
            if (_.ignorePrompts) {
                return (_cb());
            }
            var msg = [
                'This operation will remove all local data and rebuild this',
                'peer from another cluster peer.  This operation is',
                'destructive, and you should only proceed after confirming',
                'that this peer\'s copy of the data is no longer required. ',
                '(yes/no)?'
            ].join(' ');
            var deposedMsg = [
                'This peer is a deposed former primary and may have state',
                'that needs to be removed before it can successfully rejoin',
                'the cluster.  This operation will remove all local data',
                'and then unmark this peer as deposed so that it can rejoin',
                'the cluster.  This operation is destructive, and you should',
                'only proceed after confirming that this peer\'s copy of',
                'the data is no longer required. (yes/no)?'
            ].join(' ');

            console.error(_.removeFromDeposed ? deposedMsg : msg);
            prompt.get(['no'], function (err, result) {
                if (err) {
                    return _cb(err);
                }

                if (result.no !== 'yes' && result.no !== 'y') {
                    console.error('aborting rollback');
                    return _cb(new VError('aborting rollback ' +
                                          'due to user command'));
                }
                return _cb();
            });
        },
        function _disableSitter(_, _cb) {
            console.error('Disabling manatee-sitter SMF service to ensure ' +
                          'that Manatee is not running.');
            exec('svcadm disable -s manatee-sitter', _cb);
        },
        function _waitForZkInactive(_, _cb) {
            _cb = once(_cb);
            var waitStart = process.hrtime();
            var waitCount = 0;
            var waitTimeout = 60;
            if (_.config.zkCfg.opts && _.config.zkCfg.opts.sessionTimeout) {
                waitTimeout = _.config.zkCfg.opts.sessionTimeout / 1000;
            }

            /*
             * ZooKeeper might not expire our session in the exact amount of
             * seconds we expect, so we add some leeway into our timer.
             */
            waitTimeout *= 1.5;

            process.stderr.write('Waiting up to ' + waitTimeout + ' seconds ' +
                                 'for this Manatee instance\'s ZooKeeper ' +
                                 'session to expire  ');
            function checkZkActive() {
                vasync.pipeline({ funcs: [
                    _active,
                    _setActiveZkNode
                ], arg: opts}, function (err) {
                    if (err)
                        return _cb(err);

                    if (_.activeZkNode) {
                        if (process.hrtime(waitStart)[0] > waitTimeout) {
                            process.stderr.write('\n');
                            return _cb(new Error('This Manatee instance has ' +
                                                 'a ZooKeeper session even ' +
                                                 'after ' + waitTimeout + ' ' +
                                                 'seconds.  Aborting ' +
                                                 'operation.'));
                        }
                        process.stderr.write('\b' + SPINNER[waitCount++ %
                                             SPINNER.length]);
                        return setTimeout(checkZkActive, 1000);
                    } else {
                        process.stderr.write('\n');
                        return _cb();
                    }
                });
            }
            checkZkActive();
        },
        function _deleteDataDir(_, _cb) {
            console.error('removing zfs dataset');
            var cmd = 'rm -rf ' + _.config.postgresMgrCfg.dataDir + '/*';
            exec(cmd, _cb);
        },
        function _removeFromDeposed(_, _cb) {
            if (!_.removeFromDeposed) {
                return (_cb());
            }
            _.state.deposed.splice(_.deposedIndex, 1);
            _putState(_, _cb);
        },
        //This just lets the node recover "naturally"
        function _restartSitter(_, _cb) {
            console.error('enabling sitter');
            exec('svcadm enable manatee-sitter', _cb);
        },
        function _checkZfsRecv(_, _cb) {
            _cb = once(_cb);
            var client = restify.createJsonClient({
                url: 'http://' + _.config.ip + ':' +
                    (_.config.postgresPort + 1),
                version: '*'
            });

            var bar;
            var lastByte = 0;
            var waitCount = 0;

            process.stderr.write('Waiting for zfs recv  ');
            function checkZfsStatus() {
                client.get('/restore', function (err, req, res, obj) {
                    if (err) {
                        LOG.warn({err: err}, 'unable to query zfs status');
                        // give the sitter 30s to start
                        if (++waitCount > 30) {
                            client.close();
                            return _cb(err, 'unable to query zfs status');
                        }
                        setTimeout(checkZfsStatus, 1000);
                        return;
                    } else if (obj.restore && obj.restore.size) {
                        if (obj.restore.done) {
                            LOG.info('zfs receive is done');
                            process.stderr.write('\n');
                            client.close();
                            return _cb();
                        }
                        if (!bar) {
                            bar = new progbar.ProgressBar({
                                filename: obj.restore.dataset,
                                size: parseInt(obj.restore.size, 10)
                            });
                        }
                        if (obj.restore.completed) {
                            var completed = parseInt(obj.restore.completed, 10);
                            var advance = completed - lastByte;
                            lastByte = completed;
                            bar.advance(advance);
                        }
                        setTimeout(checkZfsStatus, 1000);
                        return;
                    } else {
                        process.stderr.write('\b' + SPINNER[waitCount++ %
                                             SPINNER.length]);
                        setTimeout(checkZfsStatus, 1000);
                        return;
                    }
                });
            }
            checkZfsStatus();
        }
    ], arg: opts}, function (err, results) {
        _closeZkClient(opts);
        return (cb(err));
    });

}

/**
 * @param {Object} opts The options object.
 * @param {String} opts.zk The zookeeper URL.
 * @param {String} opts.path The manatee lock path.
 *
 * Check a zk lock path
 */
function checkLock(opts, cb) {
    vasync.pipeline({ funcs: [
        _createZkClient,
        function _checkNode(_, _cb) {
            _.zkClient.exists(opts.path, function (err, stat) {
                _.result = stat;
                return (_cb(err));
            });
        }
    ], arg: opts}, function (err, results) {
        return (cb(err, opts.result));
    });
}

/**
 * Fetches the history of state changes for this shard from the ZooKeeper
 * cluster, sorts them, and returns them asynchronously via the usual
 * "callback(err, results)" pattern.  If there's no error, then "results" is an
 * array of JavaScript objects denoting a state of the system, including:
 *
 *     time             JavaScript Date object denoting the time this state was
 *                      recorded
 *
 *     state            The system's state (documented with Manatee)
 *
 *     zkSeq            ZooKeeper sequence number for this state.  This is a
 *                      string, possibly with leading zeros.
 *
 * By default, these records are ordered by "zkSeq", which is the order they
 * were recorded in ZooKeeper.  In rare cases when it's useful to order them
 * chronologically instead, use the sortByTime parameter.
 *
 * @param {Object}  args            required arguments
 * @param {Object}  args.zk         ZooKeeper connection string for a cluster
 *                                  storing the history of this shard
 * @param {String}  args.shard      the manatee shard name
 * @param {Boolean} args.sortByTime optional: sort by timestamp rather than ZK
 *                                  sequence number
 */
function history(args, cb) {
    var hist;

    assert.object(args, 'args');
    assert.string(args.zk, 'args.zk');
    assert.string(args.shard, 'args.shard');
    assert.optionalBool(args.sortByTime, 'args.sortByTime');

    hist = {
        'zk': args.zk,
        'shardPath': '/manatee/' + args.shard + '/history',
        'sortByTime': args.sortByTime,
        'zkClient': null,
        'nodes': null,
        'history': null
    };

    vasync.pipeline({ arg: hist, funcs: [
        _createZkClient,

        function fetchHistory(_, _cb) {
            hist.zkClient.getChildren(hist.shardPath, function (err, c) {
                if (!err)
                    hist.nodes = c;
                return _cb(err);
            });
        },

        function formatNodes(_, _cb) {
            vasync.forEachParallel({
                'func': translateHistoryNode,
                'inputs': hist.nodes.map(function (c) {
                    return ({
                        'zkClient': hist.zkClient,
                        'zkPath': hist.shardPath,
                        'zkNode': c
                    });
                })
            }, function (err, res) {
                if (err) {
                    return (_cb(err));
                }

                hist.history = res.operations.map(function (op) {
                    return (op.result);
                });

                hist.history.sort(function (a, b) {
                    var sa, sb;

                    if (hist.sortByTime) {
                        sa = a.time.getTime();
                        sb = b.time.getTime();
                    } else {
                        sa = parseInt(a.zkSeq, 10);
                        sb = parseInt(b.zkSeq, 10);
                    }

                    return (sa - sb);
                });

                var prev = null;
                hist.history.forEach(function (entry) {
                    entry.comment = annotateHistoryNode(entry, prev);
                    prev = entry;
                });

                return (_cb());
            });
        }
    ]}, function (err, results) {
        _closeZkClient(hist);
        return cb(err, hist.history);
    });
}

// private functions

function createZkClient(connStr, cb) {
    cb = once(cb);
    var zkClient = zk.createClient(connStr);
    zkClient.once('connected', function () {
        if (cb.called) {
            // thanks, but we already gave up
            LOG.info('zk spurious connected');
            zkClient.removeAllListeners();
            zkClient.close();
        } else {
            LOG.info('zk connected');
            return cb(null, zkClient);
        }
    });

    zkClient.once('disconnected', function () {
        throw new VError('zk client disconnected!');
    });

    zkClient.on('error', function (err) {
        throw new VError(err, 'got zk client error!');
    });

    LOG.info('connecting to zk');
    zkClient.connect();
    setTimeout(function () {
        return cb(new VError('unable to connect to zk'));
    }, 10000).unref();
}

function queryPg(url, _query, callback) {
    callback = once(callback);
    LOG.debug({
        url: url,
        query: _query
    }, 'query: entering.');

    setTimeout(function () {
        return callback(new VError('postgres request timed out'));
    }, 1000);
    var client = new pg.Client(url);
    client.connect(function (err) {
        if (err) {
            return callback(err);
        }
        LOG.debug({
            sql: _query,
            url: url
        }, 'query: connected to pg, executing sql');
        client.query(_query, function (err2, result) {
            LOG.debug({err: err2, url: url}, 'returned from query');
            client.end();
            return callback(err2, result);
        });
    });
}

function oldHistoryToObj(fNode) {
    var node = {};
    for (var j = 0; j < fNode.length; j++) {
        var entry = (fNode[j] === null ^ fNode[j] === 'undefined' ^
                     fNode[j] === 'null') ?  '' : fNode[j];
        switch (j) {
        case 0:
            node.time = entry;
            node.date = new Date(parseInt(entry, 10));
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

    return (node);
}

function translateHistoryNode(opts, cb) {
    assert.object(opts, 'opts');
    assert.object(opts.zkClient, 'opts.zkClient');
    assert.string(opts.zkPath, 'opts.zkPath');
    assert.string(opts.zkNode, 'opts.zkNode');

    // Old entries look like timestamp-ip-role-master-slave-zkseq from zk.
    // New Entries look like generation-zkseq
    var fNode = opts.zkNode.split('-');
    if (fNode.length > 2) {
        return (cb(null, oldHistoryToObj(fNode)));
    }

    var p = opts.zkPath + '/' + opts.zkNode;
    opts.zkClient.getData(p, function (err, data, stat) {
        if (err) {
            return (cb(err));
        }
        var time = bignum.fromBuffer(stat.ctime).toNumber();
        var ret = {
            'time': new Date(time),
            'state': JSON.parse(data.toString('utf8')),
            'zkSeq': fNode[1]
        };
        return (cb(null, ret));
    });
}

/*
 * Given a manatee history node "evt" and previous node "last", return a
 * human-readable string describing what happened between these states.
 */
function annotateHistoryNode(evt, last)
{
    var nst, lst, changes;
    var oldpeers, newpeers, h;

    if (evt.action) {
        return ('v1.0 event');
    }

    if (last === null) {
        if (evt.state.oneNodeWriteMode)
            return ('cluster setup for singleton ' +
                '(one-node-write) mode');
        return ('cluster setup for normal (multi-peer) mode');
    }

    if (last.action) {
        return ('follow-up from 1.0 event');
    }

    nst = evt.state;
    lst = last.state;

    if (nst.generation < lst.generation) {
        return ('error: gen number went backwards');
    }

    if (!lst.oneNodeWriteMode && nst.oneNodeWriteMode) {
        return ('error: unsupported transition from multi-peer ' +
            'mode to singleton (one-node-write) mode');
    }

    if (lst.oneNodeWriteMode && !nst.oneNodeWriteMode) {
        return ('cluster transitioned from singleton ' +
            '(one-node-write) mode to multi-peer mode');
    }

    if (nst.primary.zoneId != lst.primary.zoneId) {
        if (nst.generation == lst.generation) {
            return ('error: new primary, but same gen number');
        }

        if (lst.sync === null ||
            nst.primary.zoneId != lst.sync.zoneId) {
            return ('error: new primary was not previous sync');
        }

        return (sprintf('sync (%s) took over as primary (from %s)',
            nst.primary.zoneId.substr(0, 8),
            lst.primary.zoneId.substr(0, 8)));
    }

    if (nst.generation > lst.generation) {
        if (nst.sync.zoneId == lst.sync.zoneId) {
            return ('error: gen number changed, but ' +
                'primary and sync did not');
        }

        return (sprintf('primary (%s) selected new sync ' +
            '(was %s, now %s)', nst.primary.zoneId.substr(0, 8),
            lst.sync.zoneId.substr(0, 8),
            nst.sync.zoneId.substr(0, 8)));
    }

    if ((nst.sync === null && lst.sync !== null) ||
        (nst.sync !== null && lst.sync === null) ||
        (nst.sync !== null && lst.sync !== null &&
        nst.sync.zoneId != lst.sync.zoneId)) {
        return ('error: sync changed, but gen number did not');
    }

    changes = [];
    if (nst.freeze && !lst.freeze) {
        changes.push(sprintf('cluster frozen: %s', nst.freeze.reason));
    } else if (!nst.freeze && lst.freeze) {
        changes.push('cluster unfrozen');
    }

    newpeers = {};
    oldpeers = {};
    nst.async.forEach(function (a) { newpeers[a.zoneId] = true; });
    lst.async.forEach(function (a) { oldpeers[a.zoneId] = true; });
    for (h in newpeers) {
        if (!oldpeers.hasOwnProperty(h))
            changes.push(sprintf('async "%s" added',
                h.substr(0, 8)));
    }
    for (h in oldpeers) {
        if (!newpeers.hasOwnProperty(h))
            changes.push(sprintf('async "%s" removed',
                h.substr(0, 8)));
    }

    newpeers = {};
    oldpeers = {};
    nst.deposed.forEach(function (a) { newpeers[a.zoneId] = true; });
    lst.deposed.forEach(function (a) { oldpeers[a.zoneId] = true; });
    for (h in newpeers) {
        if (!oldpeers.hasOwnProperty(h))
            changes.push(sprintf('"%s" deposed',
                h.substr(0, 8)));
    }
    for (h in oldpeers) {
        if (!newpeers.hasOwnProperty(h))
            changes.push(sprintf('"%s" no longer deposed',
                h.substr(0, 8)));
    }

    return (changes.join(', '));
}

/**
 * transform an zk election node name into a backup server url.
 * @param {string} zkNode The zknode, e.g.
 * 10.77.77.9:pgPort:backupPort-0000000057
 *
 * @return {string} The transformed backup server url, e.g.
 * http://10.0.0.0:5432
 */
function transformBackupUrl(zkNode) {
    var data = zkNode.split('-')[0].split(':');
    return 'http://' + data[0] + ':' + data[2];
}

/**
 * transform an zk election node name into a postgres url.
 * @param {string} zkNode The zknode, e.g.
 * 10.77.77.9:pgPort:backupPort-0000000057
 *
 * @return {string} The transformed pg url, e.g.
 * tcp://postgres@10.0.0.0:5432/postgres
 */
function transformPgUrl(zkNode) {
    var data = zkNode.split('-')[0].split(':');
    return 'tcp://postgres@' + data[0] + ':' + data[1] + '/postgres';
}
