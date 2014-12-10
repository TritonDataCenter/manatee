/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2014, Joyent, Inc.
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
var util = require('util');
var vasync = require('vasync');
var verror = require('verror');
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

exports.active = active;
exports.checkLock = checkLock;
exports.freeze = freeze;
exports.unfreeze = unfreeze;
exports.history = history;
exports.rebuild = rebuild;
exports.removeDeposed = removeDeposed;
exports.setOnwm = setOnwm;
exports.state = state;
exports.stateBackfill = stateBackfill;
exports.status = status;


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
    _.zkClient.setData(p, data, _.stateStat.version, function (err) {
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

// Operations

/**
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
function state(opts, cb) {
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
function removeDeposed(opts, cb) {
    vasync.pipeline({ funcs: [
        _createZkClient,
        _getState,
        function _removeDeposed(_, _cb) {
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
                    return _cb(new verror.VError('aborting cluster state ' +
                                                 'backfill due to user ' +
                                                 'command'));
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
function active(opts, cb) {
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
            console.error('is this correct(y/n)');
            prompt.get(['yes'], function (err, result) {
                if (err) {
                    return _cb(err);
                }

                if (result.yes !== 'yes' && result.yes !== 'y') {
                    console.error('aborted...');
                    return _cb(new verror.VError('aborting cluster state ' +
                                                 'backfill due to user ' +
                                                 'command'));
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
        function _checkActive(_, _cb) {
            var act = null;
            Object.keys(_.activeData).forEach(function (k) {
                var n = _.activeData[k];
                if (n.zoneId === _.config.zoneId) {
                    act = n;
                }
            });
            if (act) {
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
            console.error('this will result in data loss if used ' +
                          'incorrectly confirm(y/n)');
            prompt.get(['yes'], function (err, result) {
                if (err) {
                    return _cb(err);
                }

                if (result.yes !== 'yes' && result.yes !== 'y') {
                    console.error('aborting rollback');
                    return _cb(new verror.VError('aborting rollback ' +
                                                  'due to user command'));
                }
                return _cb();
            });
        },
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
            if (index === -1) {
                return (_cb());
            }
            console.error('This zone is deposed.  It must be removed from ' +
                          'this list before continuing.  Proceed (y/n)?');
            prompt.get(['yes'], function (err, result) {
                if (err) {
                    return _cb(err);
                }

                if (result.yes !== 'yes' && result.yes !== 'y') {
                    console.error('aborting rollback');
                    return _cb(new verror.VError('aborting rollback ' +
                                                 'due to user command'));
                }

                //Just record for now.  A later function will take care of
                // removing it from the cluster state.
                _.deposedIndex = index;
                _.removeFromDeposed = true;
                return (_cb());
            });
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
 * @param {Object} opts The options object.
 * @param {String} opts.zk The zookeeper URL.
 * @param {String} opts.shard The manatee shard.
 */
function history(opts, cb) {
    var shardPath = '/manatee/' + opts.shard + '/history';
    vasync.pipeline({ funcs: [
        _createZkClient,
        function _getHistory(_, _cb) {
            _.zkClient.getChildren(shardPath, function (err, c) {
                _.nodes = c;
                return _cb(err);
            });
        },
        function formatNodes(_, _cb) {
            vasync.forEachParallel({
                'func': translateHistoryNode,
                'inputs': _.nodes.map(function (c) {
                    return ({
                        'zkClient': _.zkClient,
                        'zkPath': shardPath,
                        'zkNode': c
                    });
                })
            }, function (err, res) {
                if (err) {
                    return (_cb(err));
                }
                _.history = [];
                res.operations.forEach(function (op) {
                    _.history.push(op.result);
                });

                _.history.sort(function (a, b) {
                    return a.zkSeq - b.zkSeq;
                });
                return (_cb());
            });
        }
    ], arg: opts}, function (err, results) {
        _closeZkClient(opts);
        return cb(err, opts.history);
    });
}

// private functions

function createZkClient(connStr, cb) {
    cb = once(cb);
    var zkClient = zk.createClient(connStr);
    zkClient.once('connected', function () {
        LOG.info('zk connected');
        return cb(null, zkClient);
    });

    zkClient.once('disconnected', function () {
        throw new verror.VError('zk client disconnected!');
    });

    zkClient.on('error', function (err) {
        throw new verror.VError(err, 'got zk client error!');
    });

    LOG.info('connecting to zk');
    zkClient.connect();
    setTimeout(function () {
        return cb(new verror.VError('unable to connect to zk'));
    }, 10000).unref();
}

function queryPg(url, _query, callback) {
    callback = once(callback);
    LOG.debug({
        url: url,
        query: _query
    }, 'query: entering.');

    setTimeout(function () {
        return callback(new verror.VError('postgres request timed out'));
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
            'time': '' + time,
            'date': new Date(time),
            'state': JSON.parse(data.toString('utf8')),
            'zkSeq': fNode[1]
        };
        return (cb(null, ret));
    });
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
