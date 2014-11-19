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
var fs = require('fs');
var exec = require('child_process').exec;
var path = require('path');
var util = require('util');

var bunyan = require('bunyan');
var once = require('once');
var leader = require('leader');
var pg = require('pg');
var progbar = require('progbar');
var prompt = require('prompt');
var restify = require('restify');
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

exports.clear = clearError;
exports.checkLock = checkLock;
exports.history = history;
exports.promote = promote;
exports.rebuild = rebuild;
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
 * TODO: we should just do the work here rather than delegate to the "real"
 * method.
 */
function _liveStatus(_, _cb) {
    liveStatus(_, function (err, s) {
        if (err) {
            return (_cb(err));
        }
        _.status = s;
        return (_cb());
    });
}


/**
 * @param {string} opts.shard The shard
 * @param {object} opts.zkClient The zk client
 * @param {function} cb
 *
 * @return {object} opts.state The state object in zookeeper.
 */
function _getState(_, _cb) {
    var shardPath = '/manatee/' + _.shard + '/state';
    _.zkClient.getData(shardPath, function (err, sbuffer) {
        if (err) {
            return (_cb(err));
        }
        _.state = JSON.parse(new Buffer(sbuffer).toString('utf8'));
        return (_cb());
    });
}



// Operations

/**
 * @param {Object} opts The options object.
 * @param {String} opts.zk The zookeeper URL.
 * @param {String} [opts.shard] The manatee shard.
 *
 * TODO: Change to get the shard list from cluster states rather than from the
 * election.
 */
function status(opts, cb) {
    vasync.pipeline({ funcs: [
        _createZkClient,
        _getShards,
        _liveStatus
    ], arg: opts}, function (err, results) {
        _closeZkClient(opts);
        return cb(err, opts.status);
    });
}

/**
 * @param {Object} opts The options object.
 * @param {Object} opts.zk The zk client config.
 * @param {String} opts.shard The name of the shard.
 */
function state(opts, cb) {
    var stateObj;
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
        _liveStatus,
        function _buildState(_, _cb) {
            // Notice we're going to the inner object here...
            var status = _.status[opts.shard];
            var asyncs = [];
            //Translate to an array of asyncs.  Really should be refactored
            // so that the internal structure is in an array.
            Object.keys(status).forEach(function (role) {
                if (role === 'async') {
                    asyncs[0] = status[role];
                } else if (role.substring(0, 5) === 'async') {
                    asyncs[parseInt(role.substring(5), 10)] = status[role];
                }
            });
            //Shift it all by one.
            if (asyncs.length >= 1) {
                var newSync = asyncs.shift();
                asyncs.push(status['sync']);
            }
            _.newState = {
                'generation': 0,
                'primary': status['primary'],
                'sync': newSync ? newSync : status['sync'],
                'asyncs': asyncs.length > 0 ? asyncs : undefined,
                'initWal': '0/0000000'
            };
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
            // TODO: Make a zookeeper manager function?
            var data = new Buffer(JSON.stringify(_.newState));
            _.zkClient.transaction().
                create(historyPath, data,
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
 * @param {Object} opts.zk The zk client config.
 * @param {String} opts.shard The name of the shard.
 */
function clear(opts, cb) {
    var zkClient;
    var shardPath = '/manatee/' + opts.shard + '/error';
    vasync.pipeline({ funcs: [
        function _createZkClient(_, _cb) {
            createZkClient(opts.zk, function (err, c) {
                zkClient = c;
                return _cb(err);
            });
        },
        function delErrorNode(_, _cb) {
            _cb = once(_cb);
            zkClient.remove(shardPath, function (err) {
                if (err && err.code === zk.Exception.NO_NODE) {
                    return _cb(new verror.VError(err, 'shard ' + opts.shard +
                            ' is not in error or shard does not exist'));
                }

                return _cb(err);
            });
        }
    ], arg: {}}, function (err, results) {
        if (zkClient) {
            zkClient.removeAllListeners();
            zkClient.close();
        }
        return cb(err);
    });
}

/**
 * @param {Object} opts The options object.
 * @param {String} opts.config The manatee sitter config.
 * @param [boolean] opts.full Try a full rebuild without rolling back the
 * snapshot first.
 */
function rebuild(opts, cb) {
    var config = opts.config;
    var pgUrl = 'tcp://postgres@' + config.ip + ':' + config.postgresPort +
                '/postgres';
    var dataset = config.postgresMgrCfg.zfsClientCfg.dataset;
    var shard = path.basename(config.shardPath);

    vasync.pipeline({ funcs: [
        /*
         * 1) disable ourselves.
         * 2) check to see if there are other peers in the shard.
         * 3) rollback zfs dataset enable ourselves see if we can start.
         * 4) check replication status.
         * 5) full rebuild by removing the data dir.
         */
        function _disableSitter(_, _cb) {
            console.error('disabling manate-sitter');
            var cmd = 'svcadm disable manatee-sitter';
            exec(cmd, _cb);
        },
        function _getShardStatus(_, _cb) {
            _cb = once(_cb);
            process.stderr.write('getting shard status ');
            status({shard: shard, zk: config.zkCfg.connStr},
                   function (err, stat) {
                if (err) {
                    console.error('no shard status');
                    return _cb(err,
                               new verror.VError('can\'t get shard status'));
                }
                _.status = stat[shard];
                if (!stat || !stat[shard] || !stat[shard].primary ||
                    stat && stat[shard] && stat[shard].primary &&
                    stat[shard].primary.error) {
                    console.error('no primary in shard');
                    return _cb(new verror.VError('Primary not available'));
                }
                console.error(' ok');
                return _cb();
            });
        },
        function _checkDataset(_, _cb) {
            var cmd = 'zfs list ' + dataset;
            exec(cmd, function (err) {
                if (err) {
                    console.error('zfs dataset dne, full rebuild needed');
                    // full rebuild if the dataset DNE
                    _.fullRebuild = true;
                }

                return _cb();
            });
        },
        function _getLatestSnapshot(_, _cb) {
            if (opts.full) {
                return _cb();
            }
            if (_.fullRebuild) {
                return _cb();
            }
            _cb = once(_cb);
            process.stderr.write('getting latest zfs snapshot ');
            /*
             * get the snapshot and sort descending by name. This guarantees the
             * earliest snapshot is on top.
             */
            var cmd = 'zfs list -t snapshot -H -d 1 -S name -o name ' +
                config.postgresMgrCfg.zfsClientCfg.dataset;
            exec(cmd, function (err, stdout, stderr) {
                LOG.debug({snapshots: stdout}, 'got snapshots');
                if (err) {
                    return _cb(err);
                }
                var snapshots = stdout.split('\n');
                LOG.debug({snapshots: snapshots}, 'got snapshots');
                /*
                 * MANATEE-214 A snapshot name is just time since epoch in ms.
                 * So it's a 13 digit number like 1405378955344. We only want
                 * snapshots that look like this to avoid using other snapshots
                 * as they may have been created by an operator.
                 */
                var regex = /^\d{13}$/;
                var snapshotIndex = -1;
                for (var i = 0; i < snapshots.length; i++) {
                    var snapshot = snapshots[i].split('@')[1];
                    LOG.debug({snapshot: snapshot},
                              'testing snapshot against regex');
                    if (regex.test(snapshot) === true) {
                        snapshotIndex = i;
                        break;
                    }
                }
                if (snapshots.length === 0 || snapshotIndex === -1) {
                    console.error('no snapshots.');
                    _.fullRebuild = true;
                    return _cb();
                }

                LOG.debug('latest snapshot is %s', snapshots[snapshotIndex]);
                _.snapshot = snapshots[snapshotIndex];
                console.error(_.snapshot);
                return _cb();
            });
        },
        function _rollbackSnapshot(_, _cb) {
            _cb = once(_cb);
            if (opts.full || _.fullRebuild) {
                return _cb();
            }
            console.error('rolling back to previous snapshot ' + _.snapshot);
            var cmd = 'zfs rollback -rf ' + _.snapshot;
            console.error('this will result in data loss if used incorrectly ' +
                          'confirm(y/n)');
            prompt.get(['yes'], function (err, result) {
                if (err) {
                    return _cb(err);
                }

                if (result.yes !== 'yes' && result.yes !== 'y') {
                    console.error('aborting rollback');
                    return _cb(new verror.VError('aborting rollback due to ' +
                                                 'user command'));
                }
                exec(cmd, _cb);
            });
        },
        function _restartSitter(_, _cb) {
            if (opts.full || _.fullRebuild) {
                return _cb();
            }
            console.error('restarting manatee-sitter');
            var cmd = 'svcadm enable manatee-sitter';
            exec(cmd, _cb);
        },
        function _verify(_, _cb) {
            if (opts.full || _.fullRebuild) {
                return _cb();
            }
            var timeoutId = setTimeout(function () {
                // TODO: We *could* just keep on rolling snaphots back here and
                // only suck over all the data when there are no more to roll
                // back.
                LOG.info('unable to rebuild from last snapshot, trying full ' +
                         'rebuild');
                _.fullRebuild = true;
                console.error('full rebuild needed');
                return _cb();
            }, config.postgresMgrCfg.opsTimeout).unref();

            var checkShardTimeoutId;
            var waitCount = 0;
            process.stderr.write('verifying shard status (can take ' +
                                 config.postgresMgrCfg.opsTimeout/1000 + 's) ');
            checkShard();
            function checkShard() {
                status({shard: shard, zk: config.zkCfg.connStr},
                       function (err, stat) {
                    if (err) {
                        return retry();
                    }
                    if (!stat || !stat[shard]) {
                        return retry();
                    }
                    stat = stat[shard];
                    /*
                     * we can only be the sync or async at this point, so check
                     * and see if the primary or sync has replication state
                     * about us
                     */
                    if (stat && stat.primary && stat.primary.repl &&
                        stat.primary.repl.application_name === pgUrl &&
                        stat.primary.repl.sync_state === 'sync') {
                        return success();
                    } else if (stat && stat.sync && stat.sync.repl &&
                               stat.sync.repl.application_name == pgUrl &&
                               stat.sync.repl.sync_state === 'async') {
                        return success();
                    } else {
                        return retry();
                    }
                });

                function success() {
                    clearTimeout(checkShardTimeoutId);
                    clearTimeout(timeoutId);
                    console.error('\nshard status verified');
                    return _cb();
                }

                function retry() {
                    process.stderr.write('\b' + SPINNER[++waitCount %
                                         SPINNER.length]);
                    checkShardTimeoutId = setTimeout(checkShard, 3000);
                }
            }
        },
        function _rebuild(_, _cb) {
            if (!_.fullRebuild && !opts.full) {
                return _cb();
            }
            console.error('performing full rebuild');
            vasync.pipeline({funcs: [
                function _promptConfirm(__, _cb2) {
                    console.error('this will result in data loss if used ' +
                                  'incorrectly confirm(y/n)');
                    prompt.get(['yes'], function (err, result) {
                        if (err) {
                            return _cb2(err);
                        }

                        if (result.yes !== 'yes' && result.yes !== 'y') {
                            console.error('aborting rollback');
                            return _cb2(new verror.VError('aborting rollback ' +
                                           'due to user command'));
                        }
                        return _cb2();
                    });
                },
                function _disableSitter1(__, _cb2) {
                    console.error('disabling sitter');
                    exec('svcadm disable manatee-sitter', _cb2);
                },
                function _deleteDataDir(__, _cb2) {
                    console.error('removing zfs dataset');
                    var cmd = 'rm -rf ' + config.postgresMgrCfg.dataDir +
                        '/*';
                    exec(cmd, _cb2);
                },
                function _restartSitter1(__, _cb2) {
                    console.error('enabling sitter');
                    exec('svcadm enable manatee-sitter', _cb2);
                }
            ], args: {}}, function (err, results) {
                return _cb(err);
            });
        },
        function _checkZfsRecv(_, _cb) {
            if (!_.fullRebuild && !opts.full) {
                return _cb();
            }
            _cb = once(_cb);
            var client = restify.createJsonClient({
                url: 'http://' + config.ip + ':' + (config.postgresPort + 1),
                version: '*'
            });

            var bar;
            var lastByte = 0;
            var waitCount = 0;

            process.stderr.write('Waiting for zfs recv  ');
            checkZfsStatus();
            function checkZfsStatus() {
                client.get('/status', function (err, req, res, obj) {
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
        },
        function _verifyAgain(_, _cb) {
            _cb = once(_cb);
            if (!_.fullRebuild && !opts.full) {
                return _cb();
            }
            var timeoutId = setTimeout(function () {
                console.error('rebuild failed.');
                return _cb(new verror.VError('unable to rebuild from leader'));
            }, config.postgresMgrCfg.opsTimeout).unref();

            var checkShardTimeoutId;
            var waitCount = 0;
            process.stderr.write('verifying shard status (can take ' +
                                 config.postgresMgrCfg.opsTimeout/1000 + 's) ');
            checkShard();
            function checkShard() {
                status({shard: shard, zk: config.zkCfg.connStr},
                       function (err, stat) {
                    if (err) {
                        return retry();
                    }
                    if (!stat| !stat[shard]) {
                        return retry();
                    }
                    stat = stat[shard];
                    /*
                     * we can only be the sync or async at this point, so check
                     * and see if the primary or sync has replication state
                     * about us
                     */
                    if (stat && stat.primary && stat.primary.repl &&
                        stat.primary.repl.application_name === pgUrl &&
                        stat.primary.repl.sync_state === 'sync') {
                        return success();
                    } else if (stat && stat.sync && stat.sync.repl &&
                               stat.sync.repl.application_name == pgUrl &&
                               stat.sync.repl.sync_state === 'async') {
                        return success();
                    } else {
                        return retry();
                    }
                });

                function success() {
                    clearTimeout(checkShardTimeoutId);
                    clearTimeout(timeoutId);
                    console.error(' done');
                    return _cb();
                }

                function retry() {
                    LOG.info('replication not established, re-checking ' +
                             'in 4s');
                    process.stderr.write('\b' + SPINNER[++waitCount %
                                         SPINNER.length]);
                    checkShardTimeoutId = setTimeout(checkShard, 4000);
                }
            }
        }
    ], arg: {}}, function (err, results) {
        if (err) {
            console.error('rebuild failed, check postgresql and sitter logs');
        } else {
            console.error('rebuild sucessful');
        }
        return cb(err);
    });

}

/**
 * @param {Object} opts The options object.
 * @param {String} opts.config The manatee sitter config.
 * @param [boolean] opts.full Try a full rebuild without rolling back the
 * snapshot first.
 */
function promote(opts, cb) {
    var config = opts.config;
    var cookie = config.postgresMgrCfg.syncStateCheckerCfg.cookieLocation;
    var shard = path.basename(config.shardPath);
    var pgUrl = 'tcp://postgres@' + config.ip + ':' + config.postgresPort +
                '/postgres';

    vasync.pipeline({funcs: [
        function _getShardStatus(_, _cb) {
            _cb = once(_cb);
            process.stderr.write('getting shard status ');
            status({shard: shard, zk: config.zkCfg.connStr},
                   function (err, stat) {
                if (err || !stat[shard]) {
                    console.error('no shard status');
                    return _cb(err,
                               new verror.VError('can\'t get shard status'));
                }
                stat = stat[shard];
                if (stat && stat.primary) {
                    console.error('can\'t promote, primary still exists');
                    console.error('disable all other peers first and wait for' +
                                  ' them to drop out of ZK');
                    if (stat.primary.pgUrl === pgUrl) {
                        console.error('peer is already the primary');
                    }
                    return _cb(new verror.VError('primary still exists'));
                } else {
                    return _cb();
                }
            });
        },
        function _disableSitter(_, _cb) {
            console.error('disabling manate-sitter');
            var cmd = 'svcadm disable manatee-sitter';
            exec(cmd, _cb);
        },
        function _removeCookie(_, _cb) {
            process.stderr.write('removing sync state cookie ');
            fs.unlink(cookie, function (err) {
                if (err && err.code !== 'ENOENT') {
                    console.error('unable to remove sync state cookie');
                    return _cb(err);
                }

                console.error('done');
                return _cb();
            });
        },
        function _enableSitter(_, _cb) {
            console.error('enabling manate-sitter');
            var cmd = 'svcadm enable manatee-sitter';
            exec(cmd, _cb);
        },
        function _verify(_, _cb) {
            _cb = once(_cb);
            var timeoutId = setTimeout(function () {
                console.error('promotion failed.');
                return _cb(new verror.VError('promotion failed'));
            }, 300000).unref();

            var checkShardTimeoutId;
            var waitCount = 0;
            process.stderr.write('\nverifying shard status  ');
            checkShard();
            function checkShard() {
                status({shard: shard, zk: config.zkCfg.connStr},
                       function (err, stat) {
                    if (err) {
                        return retry();
                    }
                    if (!stat| !stat[shard]) {
                        return retry();
                    }
                    stat = stat[shard];
                    if (stat && stat.primary && stat.primary.pgUrl === pgUrl) {
                        return success();
                    } else {
                        return retry();
                    }
                });

                function success() {
                    clearTimeout(checkShardTimeoutId);
                    clearTimeout(timeoutId);
                    console.error(' done');
                    return _cb();
                }

                function retry() {
                    LOG.info('not primary yet');
                    process.stderr.write('\b' + SPINNER[++waitCount %
                                         SPINNER.length]);
                    checkShardTimeoutId = setTimeout(checkShard, 4000);
                }
            }
        }
    ], args: {}}, function (err, results) {
        if (err) {
            console.error('promotion failed');
        }
        return cb(err);
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
    var zkClient;
    var result;

    vasync.pipeline({ funcs: [
        function _createZkClient(_, _cb) {
            createZkClient(opts.zk, function (err, c) {
                zkClient = c;
                return _cb(err);
            });
        },
        function _checkNode(_, _cb) {
            zkClient.exists(opts.path, function (err, stat) {
                result = stat;
                return _cb(err);
            });
        }
    ], arg: {}}, function (err, results) {
        zkClient.removeAllListeners();
        zkClient.close();
        return cb(err, result);
    });
}

/**
 * @param {Object} opts The options object.
 * @param {String} opts.zk The zookeeper URL.
 * @param {String} opts.shard The manatee shard.
 */
function history(opts, cb) {
    var shardPath;
    var zkClient;
    var result;

    shardPath = '/manatee/' + opts.shard + '/history';
    vasync.pipeline({ funcs: [
        function _createZkClient(_, _cb) {
            createZkClient(opts.zk, function (err, c) {
                zkClient = c;
                return _cb(err);
            });
        },
        function _getHistory(_, _cb) {
            zkClient.getChildren(shardPath, function (err, c) {
                _.nodes = c;
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
            }

            return _cb();
        },
        function sortNodes(arg, _cb) {
            arg.nodes.sort(function (a, b) {
                return a.zkSeq - b.zkSeq;
            });

            result = arg.nodes;
            return _cb();
        }
    ], arg: {}}, function (err, results) {
        if (zkClient) {
            zkClient.removeAllListeners();
            zkClient.close();
        }
        return cb(err, result);
    });
}

/**
 * @param {Object} opts The options object.
 * @param {Object} opts.zk The zk client config.
 * @param {String} opts.shard The name of the shard.
 */
function clearError(opts, cb) {
    var zkClient;
    var shardPath = '/manatee/' + opts.shard + '/error';
    vasync.pipeline({ funcs: [
        function _createZkClient(_, _cb) {
            createZkClient(opts.zk, function (err, c) {
                zkClient = c;
                return _cb(err);
            });
        },
        function delErrorNode(_, _cb) {
            _cb = once(_cb);
            zkClient.remove(shardPath, function (err) {
                if (err && err.code === zk.Exception.NO_NODE) {
                    return _cb(new verror.VError(err, 'shard ' + opts.shard +
                            ' is not in error or shard does not exist'));
                }

                return _cb(err);
            });
        }
    ], arg: {}}, function (err, results) {
        if (zkClient) {
            zkClient.removeAllListeners();
            zkClient.close();
        }
        return cb(err);
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

/**
 * Fetch the "live" status for the given list of shards.
 *
 * @param {Object} opts.zkClient A connected ZK client.
 * @param {Array} opts.shards A list of shard names.
 *
 * @return {object} The live status for the list of shards.
 */
function liveStatus(opts, cb) {
    var shards = {};
    var zkClient = opts.zkClient;
    var shardList = opts.shards;

    vasync.pipeline({ funcs: [
        function _getShardChildren(_, _cb) {
            _.shards = {};
            _cb = once(_cb);
            var barrier = vasync.barrier();

            barrier.on('drain', _cb);

            shardList.forEach(function (shard) {
                barrier.start(shard);
                // shard is just the implicit znode name, so we have to
                // prepend the path prefix.
                var p = '/manatee/' + shard + '/election';
                zkClient.getChildren(p, function (err1, ch) {
                    if (err1) {
                        return _cb(err1);
                    }
                    ch.sort(compareNodeNames);
                    _.shards[shard] = ch;
                    barrier.done(shard);
                });
            });
        },
        function _getPeerState(_, _cb) {
            _cb = once(_cb);
            var barrier = vasync.barrier();
            barrier.on('drain', function () {
                return _cb();
            });
            Object.keys(_.shards).forEach(function (shard) {
                shards[shard] = {};
                // in case the shard is empty, we set a barrier so we exit.
                barrier.start(shard);
                _.shards[shard].forEach(function (peer, i) {
                    var p = '/manatee/' + shard + '/election/' + peer;
                    var peerName;
                    switch (i) {
                        case 0:
                            peerName = 'primary';
                            break;
                        case 1:
                            peerName = 'sync';
                            break;
                        case 2:
                            peerName = 'async';
                            break;
                        default:
                            peerName = 'async' + (i - 2);
                            break;
                    }
                    barrier.start(shard + peerName);
                    zkClient.getData(p, function (err, data) {
                        if (err) {
                            return _cb(err);
                        }
                        shards[shard][peerName] = JSON.parse(data.toString());
                        shards[shard][peerName]['id'] =
                            peer.substring(0, peer.lastIndexOf('-'));
                        barrier.done(shard + peerName);
                    });
                });
                barrier.done(shard);
            });
        },
        function _queryPgStat(_, _cb) {
            _cb = once(_cb);
            if (opts.noPostgres) {
                return (_cb());
            }
            var barrier = vasync.barrier();
            barrier.on('drain', function () {
                return _cb();
            });
            Object.keys(shards).forEach(function (shard) {
                // in case the shard is empty, we set a barrier so we exit.
                barrier.start(shard);
                Object.keys(shards[shard]).forEach(function (peer) {
                    var entry = shards[shard][peer];
                    var pgUrl = entry.pgUrl;
                    barrier.start(pgUrl);
                    queryPg(pgUrl, PG_REPL_STAT, function (err, res) {
                        if (err) {
                            entry.error = JSON.stringify(err);
                            barrier.done(pgUrl);
                        } else {
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
        },
        function _getError(_, _cb) {
            _cb = once(_cb);
            var barrier = vasync.barrier();
            barrier.on('drain', function () {
                return _cb();
            });
            Object.keys(_.shards).forEach(function (shard) {
                // in case the shard is empty, we set a barrier so we exit.
                barrier.start(shard);
                var p = '/manatee/' + shard + '/error';
                zkClient.getData(p, function (err, data) {
                    if (err && err.code !== zk.Exception.NO_NODE) {
                        return _cb(err);
                    }
                    if (data) {
                        shards[shard].error = JSON.parse(data.toString());
                    }
                    barrier.done(shard);
                });
            });
        }
    ], arg: {}}, function (err, results) {
        return cb(err, shards);
    });

    // private functions
    function compareNodeNames(a, b) {
        var seqA = parseInt(a.substring(a.lastIndexOf('-') + 1), 10);
        var seqB = parseInt(b.substring(b.lastIndexOf('-') + 1), 10);

        return (seqA - seqB);
    }
}
