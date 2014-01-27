var assert = require('assert-plus');
var bunyan = require('bunyan');
var ConfParser = require('../lib/confParser');
var fs = require('fs');
var exec = require('child_process').exec;
var path = require('path');
var mantee_common = require('../bin/manatee_common');
var Manatee = require('./testManatee');
var once = require('once');
var spawn = require('child_process').spawn;
var shelljs = require('shelljs');
var util = require('util');
var uuid = require('node-uuid');
var vasync = require('vasync');
var verror = require('verror');

var FS_PATH_PREFIX = process.env.FS_PATH_PREFIX || '/var/tmp/manatee_tests';
var ZK_URL = process.env.ZK_URL || 'localhost:2181';
var PARENT_ZFS_DS = process.env.PARENT_ZFS_DS;
var SHARD_ID = uuid.v4();
var SHARD_PATH = '/manatee/' + SHARD_ID;
var SITTER_CFG = './etc/sitter.json';
var BS_CFG = './etc/backupserver.json';
var SS_CFG = './etc/snapshotter.json';
var MY_IP = '127.0.0.1';
var ZK_CLIENT = null;

var LOG = bunyan.createLogger({
    level: (process.env.LOG_LEVEL || 'info'),
    name: 'manatee-integ-tests',
    serializers: {
        err: bunyan.stdSerializers.err
    },
    src: true
});

var MANATEES = {};

//setTimeout(function() { console.error(process._getActiveHandles(), process._getActiveRequests()); }, 30000).unref();

/*
 * Tests
 */

exports.before = function (t) {
    var n1 = uuid.v4();
    var n1Port = 10000;
    var n2 = uuid.v4();
    var n2Port = 20000;
    var n3 = uuid.v4();
    var n3Port = 30000;
    var n1Opts = {
        zfsDataset: PARENT_ZFS_DS + '/' + n1,
        zfsPort: n1Port,
        heartbeatServerPort: ++n1Port,
        mountPoint: FS_PATH_PREFIX + '/' + n1,
        backupPort: ++n1Port,
        postgresPort: ++n1Port,
        cookieLocation: FS_PATH_PREFIX + '/' + n1 + '_metadata' + '/cookie',
        backupServerPort: ++n1Port,
        configLocation: FS_PATH_PREFIX + '/' + n1 + '_metadata' + '/config',
        metadataDir: FS_PATH_PREFIX + '/' + n1 + '_metadata',
        shardPath: SHARD_PATH,
        log: LOG
    };
    var n2Opts = {
        zfsDataset: PARENT_ZFS_DS + '/' + n2,
        zfsPort: n2Port,
        heartbeatServerPort: ++n2Port,
        mountPoint: FS_PATH_PREFIX + '/' + n2,
        backupPort: ++n2Port,
        postgresPort: ++n2Port,
        cookieLocation: FS_PATH_PREFIX + '/' + n2 + '_metadata' + '/cookie',
        backupServerPort: ++n2Port,
        configLocation: FS_PATH_PREFIX + '/' + n2 + '_metadata' + '/config',
        metadataDir: FS_PATH_PREFIX + '/' + n2 + '_metadata',
        shardPath: SHARD_PATH,
        log: LOG
    };
    var n3Opts = {
        zfsDataset: PARENT_ZFS_DS + '/' + n3,
        zfsPort: n3Port,
        heartbeatServerPort: ++n3Port,
        mountPoint: FS_PATH_PREFIX + '/' + n3,
        backupPort: ++n3Port,
        postgresPort: ++n3Port,
        cookieLocation: FS_PATH_PREFIX + '/' + n3 + '_metadata' + '/cookie',
        backupServerPort: ++n3Port,
        configLocation: FS_PATH_PREFIX + '/' + n3 + '_metadata' + '/config',
        metadataDir: FS_PATH_PREFIX + '/' + n3 + '_metadata',
        shardPath: SHARD_PATH,
        log: LOG
    };

    vasync.pipeline({funcs: [
        function _createZkClient(_, _cb) {
            mantee_common.createZkClient({
                zk: ZK_URL,
                shard: SHARD_PATH
            }, function (err, zk) {
                ZK_CLIENT = zk;

                return _cb(err);
            });
        },
        function _destroyZfsDataset(_, _cb) {
            exec('zfs destroy -r ' + PARENT_ZFS_DS, function (err) {
                return _cb();
            });
        },
        //function _removeMetadata(_, _cb) {
            //exec('rm -rf ' + FS_PATH_PREFIX, _cb);
        //},
        function _cleanupZK(_, _cb) {
            ZK_CLIENT.rmr('/manatee', function() { return _cb(); });
        },
        function _startN1(_, _cb) {
            var manatee = new Manatee(n1Opts, function (err) {
                if (err) {
                    LOG.error({err: err}, 'could not start manatee');
                    return (_cb);
                }

                MANATEES[manatee.pgUrl] = manatee;
                return _cb();
            });
        },
        function _startN2(_, _cb) {
            var manatee = new Manatee(n2Opts, function (err) {
                if (err) {
                    LOG.error({err: err}, 'could not start manatee');
                    return (_cb);
                }

                MANATEES[manatee.pgUrl] = manatee;
                return _cb();
            });
        },
        function _startN3(_, _cb) {
            var manatee = new Manatee(n3Opts, function (err) {
                if (err) {
                    LOG.error({err: err}, 'could not start manatee');
                    return (_cb);
                }

                MANATEES[manatee.pgUrl] = manatee;
                return _cb();
            });
        }
    ], arg: {}}, function (err, results) {
        if (err) {
            t.fail(err);
        }
        t.done();
    });
};

exports.verifyShard = function (t) {
    vasync.pipeline({funcs: [
        function loadTopology(_, _cb) {
            mantee_common.loadTopology(ZK_CLIENT, function (err, topology) {
                LOG.info({topology: topology});
                LOG.info({shardId: SHARD_ID});
                _.topology = topology[SHARD_ID];
                LOG.info({topology: _.topology});
                if (err) {
                    return _cb(err);
                }
                return _cb();
            });
        },
        function getPgStatus(_, _cb) {
            mantee_common.pgStatus([_.topology], _cb);
        },
        function verifyTopology(_, _cb) {
            /*
             * here we only have to check the sync states of each of the nodes.
             * if the sync states are correct, then we know replication is
             * working.
             */
            t.ok(_.topology, 'shard topology DNE');
            t.ok(_.topology.primary, 'primary DNE');
            t.ok(_.topology.primary.repl, 'no sync repl state');
            t.equal(_.topology.primary.repl.sync_state,
                    'sync',
                    'no sync replication state.');
            t.ok(_.topology.sync, 'sync DNE');
            t.equal(_.topology.sync.repl.sync_state,
                    'async',
                    'no async replication state');
            t.ok(_.topology.async, 'async DNE');
            return _cb();
        }
    ], arg: {}}, function (err, results) {
        if (err) {
            LOG.error({err: err, results: results},
                      'check shard status failed');
                      t.fail(err);
        }
        t.done();
    });
};

exports.primaryDeath = function (t) {
    vasync.pipeline({funcs: [
        function loadTopology(_, _cb) {
            mantee_common.loadTopology(ZK_CLIENT, function (err, topology) {
                if (err) {
                    return _cb(err);
                }
                _.topology = topology[SHARD_ID];
                assert.ok(_.topology);
                assert.ok(_.topology.primary.pgUrl);
                _.primaryPgUrl = _.topology.primary.pgUrl;
                LOG.info({topology: topology}, 'got topology');
                return _cb();
            });
        },
        function killPrimary(_, _cb) {
            MANATEES[_.primaryPgUrl].kill(_cb);
        },
        function getNewTopology(_, _cb) {
            _cb = once(_cb);
            var topology;
            var intervalId = setInterval(function() {vasync.pipeline({funcs: [
                function _loadTopology(_2, _cb2) {
                    mantee_common.loadTopology(ZK_CLIENT, function (err, top) {
                        if (err) {
                            return _cb2(err);
                        }
                        topology = top[SHARD_ID];
                        try {
                            assert.ok(topology, 'topology DNE');
                            assert.ok(topology.primary, 'primary DNE');
                            assert.ok(topology.sync, 'sync DNE');
                            assert.equal(topology.async, null,
                                         'async should not exist after primary ' +
                                             'death');

                            LOG.info({topology: topology}, 'got topology');
                            return _cb2();
                        } catch (e) {
                            LOG.warn({err: e, topology: topology},
                                     'got unexpected topology');
                            return _cb2(e);
                        }
                   });
                },
                function _getPgStatus(_2, _cb2) {
                    try {
                        mantee_common.pgStatus([topology], _cb2);
                    } catch (e) {
                        LOG.warn({err: e, topology: topology},
                                 'could not get pg status');
                        return _cb2(e);
                    }
                },
                function _verifyTopology(_2, _cb2) {
                    try {
                        /*
                         * here we only have to check the sync states of each
                         * of the nodes.  if the sync states are correct, then
                         * we know replication is working.
                         */
                        assert.ok(topology, 'shard topology DNE');
                        assert.ok(topology.primary, 'primary DNE');
                        assert.ok(topology.primary.repl, 'no sync repl state');
                        /*
                         * empty repl fields look like this: repl: {}. So we
                         * have to check the key length in order to figure out
                         * that it is an empty object.
                         */
                        assert.equal(Object.keys(topology.sync.repl).length, 0,
                                'sync should not have replication state.');
                        return _cb2();
                    } catch (e) {
                        LOG.warn({err: e, topology: topology},
                                 'unable to verify topology');
                        return _cb2(e);
                    }
                }
            ], arg:{}}, function (err, results) {
                if (err) {
                    LOG.warn({err: err, results: results, topology: topology},
                             'still waiting for correct shard state');
                    return;
                } else {
                    clearInterval(intervalId);
                    return _cb();
                }

            });}, 3000);

            setTimeout(function() {
                clearInterval(intervalId);
                return _cb(new verror.VError('shard did not flip in time'));
            }, 30000);
        },
        function addNewManatee(_, _cb) {
            LOG.info({url: _.primaryPgUrl}, 'adding back old primary');
            MANATEES[_.primaryPgUrl].start(_cb);
        },
        function checkTopology2(_, _cb) {
            _cb = once(_cb);
            var intervalId = setInterval(function() {vasync.pipeline({funcs: [
                function _getTopology(_2, _cb2) {
                    mantee_common.loadTopology(ZK_CLIENT, function (err, topology) {
                        _2.topology = topology[SHARD_ID];
                        if (err) {
                            return _cb2(err);
                        }
                        LOG.info({topology: topology});
                        return _cb2();
                    });
                },
                function _getPgStatus(_2, _cb2) {
                    try {
                        mantee_common.pgStatus([_2.topology], _cb2);
                    } catch (e) {
                        LOG.warn({err: e, topology: _2.topology});
                        return _cb2(e);
                    }
                },
                function _verifyTopology(_2, _cb2) {
                    _cb2 = once(_cb2);
                    /*
                     * here we only have to check the sync states of each of
                     * the nodes.  if the sync states are correct, then we know
                     * replication is working.
                     */
                    try {
                        assert.ok(_2.topology, 'shard topology DNE');
                        assert.ok(_2.topology.primary, 'primary DNE');
                        assert.ok(_2.topology.primary.repl, 'no sync repl state');
                        assert.equal(_2.topology.primary.repl.sync_state,
                                'sync',
                                'no sync replication state.');
                        assert.ok(_2.topology.sync, 'sync DNE');
                        assert.equal(_2.topology.sync.repl.sync_state,
                                'async',
                                'no async replication state');
                        assert.ok(_2.topology.async, 'async DNE');
                        assert.notEqual(_2.topology.primary.pgUrl, _2.primaryPgUrl,
                                   'primary should not be killed primary');
                        return _cb2();
                    } catch (e) {
                        LOG.warn({err: e, topology: _2.topology});
                        return _cb2(e);
                    }
                }
            ], arg:{}}, function (err, results) {
                if (err) {
                    LOG.warn({err: err, results: results},
                             'topology not correct');
                    return;
                }
                clearInterval(intervalId);
                return _cb();
            });}, 3000);

            setTimeout(function() {
                clearInterval(intervalId);
                return _cb(new verror.VError(
                    'new peer did not join shard in time'));
            }, 30000);
        },
    ], arg: {}}, function (err, results) {
        if (err) {
            LOG.error({err: err, results: results});
            t.fail(err);
        }
        t.done();
    });
};

exports.syncDeath = function (t) {
    vasync.pipeline({funcs: [
        function loadTopology(_, _cb) {
            mantee_common.loadTopology(ZK_CLIENT, function (err, topology) {
                if (err) {
                    return _cb(err);
                }
                _.topology = topology[SHARD_ID];
                assert.ok(_.topology);
                assert.ok(_.topology.sync);
                assert.ok(_.topology.sync.pgUrl);
                _.syncPgUrl = _.topology.sync.pgUrl;
                LOG.info({topology: topology}, 'got topology');
                return _cb();
            });
        },
        function killSync(_, _cb) {
            MANATEES[_.syncPgUrl].kill(_cb);
        },
        function getNewTopology(_, _cb) {
            _cb = once(_cb);
            var topology;
            var intervalId = setInterval(function() {vasync.pipeline({funcs: [
                function _loadTopology(_2, _cb2) {
                    mantee_common.loadTopology(ZK_CLIENT, function (err, top) {
                        if (err) {
                            return _cb2(err);
                        }
                        topology = top[SHARD_ID];
                        try {
                            assert.ok(topology, 'topology DNE');
                            assert.ok(topology.primary, 'primary DNE');
                            assert.ok(topology.sync, 'sync DNE');
                            assert.equal(topology.async, null,
                                         'async should not exist after sync ' +
                                             'death');

                            LOG.info({topology: topology}, 'got topology');
                            return _cb2();
                        } catch (e) {
                            LOG.warn({err: e, topology: topology},
                                     'got unexpected topology');
                            return _cb2(e);
                        }
                   });
                },
                function _getPgStatus(_2, _cb2) {
                    try {
                        mantee_common.pgStatus([topology], _cb2);
                    } catch (e) {
                        LOG.warn({err: e, topology: topology},
                                 'could not get pg status');
                        return _cb2(e);
                    }
                },
                function _verifyTopology(_2, _cb2) {
                    try {
                        /*
                         * here we only have to check the sync states of each
                         * of the nodes.  if the sync states are correct, then
                         * we know replication is working.
                         */
                        assert.ok(topology, 'shard topology DNE');
                        assert.ok(topology.primary, 'primary DNE');
                        assert.ok(topology.primary.repl, 'no sync repl state');
                        /*
                         * empty repl fields look like this: repl: {}. So we
                         * have to check the key length in order to figure out
                         * that it is an empty object.
                         */
                        assert.equal(Object.keys(topology.sync.repl).length, 0,
                                'sync should not have replication state.');
                        assert.notEqual(topology.primary.url, _.syncPgUrl,
                                        'old sync is still in shard');
                        return _cb2();
                    } catch (e) {
                        LOG.warn({err: e, topology: topology},
                                 'unable to verify topology');
                        return _cb2(e);
                    }
                }
            ], arg:{}}, function (err, results) {
                if (err) {
                    LOG.warn({err: err, results: results, topology: topology},
                             'still waiting for correct shard state');
                    return;
                } else {
                    clearInterval(intervalId);
                    return _cb();
                }

            });}, 3000);

            setTimeout(function() {
                clearInterval(intervalId);
                return _cb(new verror.VError('shard did not flip in time'));
            }, 30000);
        },
        function addNewManatee(_, _cb) {
            LOG.info({url: _.syncPgUrl}, 'adding back old sync');
            MANATEES[_.syncPgUrl].start(_cb);
        },
        function checkTopology2(_, _cb) {
            _cb = once(_cb);
            var intervalId = setInterval(function() {vasync.pipeline({funcs: [
                function _getTopology(_2, _cb2) {
                    mantee_common.loadTopology(ZK_CLIENT, function (err, topology) {
                        _2.topology = topology[SHARD_ID];
                        if (err) {
                            return _cb2(err);
                        }
                        LOG.info({topology: topology});
                        return _cb2();
                    });
                },
                function _getPgStatus(_2, _cb2) {
                    try {
                        mantee_common.pgStatus([_2.topology], _cb2);
                    } catch (e) {
                        LOG.warn({err: e, topology: _2.topology});
                        return _cb2(e);
                    }
                },
                function _verifyTopology(_2, _cb2) {
                    _cb2 = once(_cb2);
                    try {
                         /*
                          * here we only have to check the sync states of each
                          * of the nodes.  if the sync states are correct, then
                          * we know replication is working.
                          */
                         assert.ok(_2.topology, 'shard topology DNE');
                         assert.ok(_2.topology.primary, 'primary DNE');
                         assert.ok(_2.topology.primary.repl,
                                   'no sync repl state');
                         assert.equal(_2.topology.primary.repl.sync_state,
                                      'sync',
                                      'no sync replication state.');
                         assert.ok(_2.topology.sync, 'sync DNE');
                         assert.equal(_2.topology.sync.repl.sync_state,
                                      'async',
                                      'no async replication state');
                         assert.ok(_2.topology.async, 'async DNE');
                         assert.equal(_2.topology.async.pgUrl, _.syncPgUrl,
                                      'async ' + _2.topology.async.pgUrl +
                                          'should be old sync peer ' +
                                          _.syncPgUrl);
                        return _cb2();
                    } catch (e) {
                        LOG.warn({err: e, topology: _2.topology});
                        return _cb2(e);
                    }
                }
            ], arg:{}}, function (err, results) {
                if (err) {
                    LOG.warn({err: err, results: results},
                             'topology not correct');
                    return;
                }
                clearInterval(intervalId);
                return _cb();
            });}, 3000);

            setTimeout(function() {
                clearInterval(intervalId);
                return _cb(new verror.VError(
                    'new peer did not join shard in time'));
            }, 30000);
        },
    ], arg: {}}, function (err, results) {
        if (err) {
            LOG.info({err: err, results: err ? results : null},
                     'finished syncDeath()');
            t.fail(err);
        }
        t.done();
    });
};

exports.asyncDeath = function (t) {
    vasync.pipeline({funcs: [
        function loadTopology(_, _cb) {
            mantee_common.loadTopology(ZK_CLIENT, function (err, topology) {
                if (err) {
                    return _cb(err);
                }
                _.topology = topology[SHARD_ID];
                assert.ok(_.topology);
                assert.ok(_.topology.sync);
                assert.ok(_.topology.sync.pgUrl);
                _.asyncPgUrl = _.topology.async.pgUrl;
                LOG.info({topology: topology}, 'got topology');
                return _cb();
            });
        },
        function killAsync(_, _cb) {
            MANATEES[_.asyncPgUrl].kill(_cb);
        },
        function getNewTopology(_, _cb) {
            _cb = once(_cb);
            var topology;
            var intervalId = setInterval(function() {vasync.pipeline({funcs: [
                function _loadTopology(_2, _cb2) {
                    mantee_common.loadTopology(ZK_CLIENT, function (err, top) {
                        if (err) {
                            return _cb2(err);
                        }
                        topology = top[SHARD_ID];
                        try {
                            assert.ok(topology, 'topology DNE');
                            assert.ok(topology.primary, 'primary DNE');
                            assert.ok(topology.sync, 'sync DNE');
                            assert.equal(topology.async, null,
                                         'async should not exist after ' +
                                             'async death');

                            LOG.info({topology: topology}, 'got topology');
                            return _cb2();
                        } catch (e) {
                            LOG.warn({err: e, topology: topology},
                                     'got unexpected topology');
                            return _cb2(e);
                        }
                   });
                },
                function _getPgStatus(_2, _cb2) {
                    try {
                        mantee_common.pgStatus([topology], _cb2);
                    } catch (e) {
                        LOG.warn({err: e, topology: topology},
                                 'could not get pg status');
                        return _cb2(e);
                    }
                },
                function _verifyTopology(_2, _cb2) {
                    try {
                        /*
                         * here we only have to check the sync states of each
                         * of the nodes.  if the sync states are correct, then
                         * we know replication is working.
                         */
                        assert.ok(topology, 'shard topology DNE');
                        assert.ok(topology.primary, 'primary DNE');
                        assert.ok(topology.primary.repl, 'no sync repl state');
                        /*
                         * empty repl fields look like this: repl: {}. So we
                         * have to check the key length in order to figure out
                         * that it is an empty object.
                         */
                        assert.equal(Object.keys(topology.sync.repl).length, 0,
                                'sync should not have replication state.');
                        assert.notEqual(topology.primary.url, _.asyncPgUrl,
                                        'old async is still in shard');
                        return _cb2();
                    } catch (e) {
                        LOG.warn({err: e, topology: topology},
                                 'unable to verify topology');
                        return _cb2(e);
                    }
                }
            ], arg:{}}, function (err, results) {
                if (err) {
                    LOG.warn({err: err, results: results, topology: topology},
                             'still waiting for correct shard state');
                    return;
                } else {
                    clearInterval(intervalId);
                    return _cb();
                }

            });}, 3000);

            setTimeout(function() {
                clearInterval(intervalId);
                return _cb(new verror.VError('shard did not flip in time'));
            }, 30000);
        },
        function addNewManatee(_, _cb) {
            LOG.info({url: _.asyncPgUrl}, 'adding back old async');
            MANATEES[_.asyncPgUrl].start(_cb);
        },
        function checkTopology2(_, _cb) {
            _cb = once(_cb);
            var intervalId = setInterval(function() {vasync.pipeline({funcs: [
                function _getTopology(_2, _cb2) {
                    mantee_common.loadTopology(ZK_CLIENT, function (err, topology) {
                        _2.topology = topology[SHARD_ID];
                        if (err) {
                            return _cb2(err);
                        }
                        LOG.info({topology: topology});
                        return _cb2();
                    });
                },
                function _getPgStatus(_2, _cb2) {
                    try {
                        mantee_common.pgStatus([_2.topology], _cb2);
                    } catch (e) {
                        LOG.warn({err: e, topology: _2.topology});
                        return _cb2(e);
                    }
                },
                function _verifyTopology(_2, _cb2) {
                    _cb2 = once(_cb2);
                    try {
                         /*
                          * here we only have to check the sync states of each
                          * of the nodes.  if the sync states are correct, then
                          * we know replication is working.
                          */
                         assert.ok(_2.topology, 'shard topology DNE');
                         assert.ok(_2.topology.primary, 'primary DNE');
                         assert.ok(_2.topology.primary.repl,
                                   'no sync repl state');
                         assert.equal(_2.topology.primary.repl.sync_state,
                                      'sync',
                                      'no sync replication state.');
                         assert.ok(_2.topology.sync, 'sync DNE');
                         assert.equal(_2.topology.sync.repl.sync_state,
                                      'async',
                                      'no async replication state');
                         assert.ok(_2.topology.async, 'async DNE');
                         assert.equal(_2.topology.async.pgUrl, _.asyncPgUrl,
                                      'async ' + _2.topology.async.pgUrl +
                                          'should be old async peer ' +
                                          _.asyncPgUrl);
                        return _cb2();
                    } catch (e) {
                        LOG.warn({err: e, topology: _2.topology},
                                 'unable to verify topology');
                        return _cb2(e);
                    }
                }
            ], arg:{}}, function (err, results) {
                if (err) {
                    LOG.warn({err: err, results: results},
                             'topology not correct');
                    return;
                }
                clearInterval(intervalId);
                return _cb();
            });}, 3000);

            setTimeout(function() {
                clearInterval(intervalId);
                return _cb(new verror.VError(
                    'new peer did not join shard in time'));
            }, 30000);
        },
    ], arg: {}}, function (err, results) {
        if (err) {
            LOG.info({err: err, results: err ? results : null},
                     'finished asyncDeath()');
            t.fail(err);
        }
        t.done();
    });
};

exports.everyoneDies = function (t) {
    vasync.pipeline({funcs: [
        function loadTopology(_, _cb) {
            mantee_common.loadTopology(ZK_CLIENT, function (err, topology) {
                if (err) {
                    return _cb(err);
                }
                _.topology = topology[SHARD_ID];
                assert.ok(_.topology);
                assert.ok(_.topology.sync);
                assert.ok(_.topology.sync.pgUrl);
                _.asyncPgUrl = _.topology.async.pgUrl;
                _.syncPgUrl = _.topology.sync.pgUrl;
                _.primaryPgUrl = _.topology.primary.pgUrl;
                LOG.info({topology: topology}, 'got topology');
                return _cb();
            });
        },
        function killEveryone(_, _cb) {
            var barrier = vasync.barrier();
            barrier.on('drain', _cb);

            // hacky way to kill everyone in random order.
            var seed = Math.round(Math.random() * 2);
            var m = Object.keys(MANATEES);
            var first = m[seed];
            m.splice(seed, 1);
            seed = Math.round(Math.random());
            var second = m[seed];
            m.splice(seed, 1);
            var third = m[0];

            barrier.start(0);
            MANATEES[first].kill(function () {
                barrier.done(0);
            });

            barrier.start(1);
            MANATEES[second].kill(function () {
                barrier.done(1);
            });

            barrier.start(2);
            MANATEES[third].kill(function () {
                barrier.done(2);
            });
        },
        function waitForEveryoneToDie(_, _cb) {
            _cb = once(_cb);
            var topology;
            var intervalId = setInterval(function() {vasync.pipeline({funcs: [
                function _loadTopology(_2, _cb2) {
                    mantee_common.loadTopology(ZK_CLIENT, function (err, top) {
                        if (err) {
                            return _cb2(err);
                        }
                        topology = top[SHARD_ID];
                        try {
                            assert.equal(topology.primary, null,
                                         'did not find empty topology');
                            assert.equal(topology.sync, null,
                                         'did not find empty topology');
                            assert.equal(topology.async, null,
                                         'did not find empty topology');
                            return _cb2();
                        } catch (e) {
                            LOG.warn({err: e, topology: topology},
                                     'got unexpected topology');
                            return _cb2(e);
                        }
                   });
                }
            ], arg:{}}, function (err, results) {
                if (err) {
                    LOG.warn({err: err, results: results, topology: topology},
                             'still waiting for correct shard state');
                    return;
                } else {
                    clearInterval(intervalId);
                    return _cb();
                }

            });}, 3000);

            setTimeout(function() {
                clearInterval(intervalId);
                return _cb(new verror.VError('shard did not flip in time'));
            }, 30000);
        },
        function restartmanatees(_, _cb) {
            _cb = once(_cb);
            var barrier = vasync.barrier();
            barrier.on('drain', _cb);

            // hacky way to start everyone in random order.
            var seed = Math.round(Math.random() * 2);
            var m = Object.keys(MANATEES);
            var first = m[seed];
            m.splice(seed, 1);
            seed = Math.round(Math.random());
            var second = m[seed];
            m.splice(seed, 1);
            var third = m[0];

            barrier.start(0);
            MANATEES[first].start(function (err) {
                if (err) {
                    return _cb(err);
                }
                barrier.done(0);
            });

            barrier.start(1);
            MANATEES[second].start(function (err) {
                if (err) {
                    return _cb(err);
                }
                barrier.done(1);
            });

            barrier.start(2);
            MANATEES[third].start(function (err) {
                if (err) {
                    return _cb(err);
                }
                barrier.done(2);
            });
        },
        function checkTopology2(_, _cb) {
            _cb = once(_cb);
            var intervalId = setInterval(function() {vasync.pipeline({funcs: [
                function _getTopology(_2, _cb2) {
                    mantee_common.loadTopology(ZK_CLIENT, function (err, topology) {
                        _2.topology = topology[SHARD_ID];
                        if (err) {
                            return _cb2(err);
                        }
                        LOG.info({topology: topology});
                        return _cb2();
                    });
                },
                function _getPgStatus(_2, _cb2) {
                    try {
                        mantee_common.pgStatus([_2.topology], _cb2);
                    } catch (e) {
                        LOG.warn({err: e, topology: _2.topology});
                        return _cb2(e);
                    }
                },
                function _verifyTopology(_2, _cb2) {
                    _cb2 = once(_cb2);
                    try {
                         /*
                          * here we only have to check the sync states of each
                          * of the nodes.  if the sync states are correct, then
                          * we know replication is working.
                          */
                         assert.ok(_2.topology, 'shard topology DNE');
                         assert.ok(_2.topology.primary, 'primary DNE');
                         assert.ok(_2.topology.primary.repl,
                                   'no sync repl state');
                         assert.equal(_2.topology.primary.repl.sync_state,
                                      'sync',
                                      'no sync replication state.');
                         assert.ok(_2.topology.sync, 'sync DNE');
                         assert.equal(_2.topology.sync.repl.sync_state,
                                      'async',
                                      'no async replication state');
                         assert.ok(_2.topology.async, 'async DNE');
                        return _cb2();
                    } catch (e) {
                        LOG.warn({err: e, topology: _2.topology},
                                 'unable to verify topology');
                        return _cb2(e);
                    }
                }
            ], arg:{}}, function (err, results) {
                if (err) {
                    LOG.warn({err: err, results: results},
                             'topology not correct');
                    return;
                }
                clearInterval(intervalId);
                return _cb();
            });}, 3000);

            setTimeout(function() {
                clearInterval(intervalId);
                return _cb(new verror.VError(
                    'new peer did not join shard in time'));
            }, 30000);
        },
    ], arg: {}}, function (err, results) {
        if (err) {
            LOG.info({err: err, results: err ? results : null},
                     'finished asyncDeath()');
            t.fail(err);
        }
        t.done();
    });
};

exports.primarySyncInstantaneousDeath = function (t) {
    vasync.pipeline({funcs: [
        function loadTopology(_, _cb) {
            mantee_common.loadTopology(ZK_CLIENT, function (err, topology) {
                if (err) {
                    return _cb(err);
                }
                _.topology = topology[SHARD_ID];
                assert.ok(_.topology);
                assert.ok(_.topology.primary.pgUrl);
                _.asyncPgUrl = _.topology.async.pgUrl;
                _.syncPgUrl = _.topology.sync.pgUrl;
                _.primaryPgUrl = _.topology.primary.pgUrl;
                LOG.info({topology: topology}, 'got topology');
                return _cb();
            });
        },
        function killPrimaryAndSync(_, _cb) {
            // hacky way to pick either sync or primary first
            var seed = Math.round(Math.random());
            var barrier = vasync.barrier();
            barrier.on('drain', _cb);
            barrier.start(0);
            barrier.start(1);
            if (seed < 1) {
                MANATEES[_.primaryPgUrl].kill(function () {
                    barrier.done(0);
                });
                MANATEES[_.syncPgUrl].kill(function () {
                    barrier.done(1);
                });
            } else {
                MANATEES[_.syncPgUrl].kill(function () {
                    barrier.done(1);
                });
                MANATEES[_.primaryPgUrl].kill(function () {
                    barrier.done(0);
                });
            }
        },
        function getNewTopology(_, _cb) {
            _cb = once(_cb);
            var topology;
            var intervalId = setInterval(function() {vasync.pipeline({funcs: [
                function _loadTopology(_2, _cb2) {
                    mantee_common.loadTopology(ZK_CLIENT, function (err, top) {
                        if (err) {
                            return _cb2(err);
                        }
                        topology = top[SHARD_ID];
                        try {
                            /*
                             * when we kill the sync and primary in quick
                             * succession, the async will not promote itself to
                             * primary because it is not up to date. Therefore
                             * we expect that there are no entries in the
                             * topology.
                             */
                            assert.ok(topology, 'topology DNE');
                            assert.equal(topology.primary, null, 'primary exists');
                            assert.equal(topology.sync, null, 'sync exists');
                            assert.equal(topology.async, null, 'async exists');

                            return _cb2();
                        } catch (e) {
                            LOG.warn({err: e, topology: topology},
                                     'got unexpected topology');
                            return _cb2(e);
                        }
                   });
                }
            ], arg:{}}, function (err, results) {
                if (err) {
                    LOG.warn({err: err, results: results, topology: topology},
                             'still waiting for correct shard state');
                    return;
                } else {
                    clearInterval(intervalId);
                    return _cb();
                }

            });}, 3000);

            setTimeout(function() {
                clearInterval(intervalId);
                return _cb(new verror.VError('shard did not flip in time'));
            }, 30000);
        },
        function restartmanatees(_, _cb) {
            _cb = once(_cb);
            // hacky way to pick either sync or primary first
            var seed = Math.round(Math.random());
            var barrier = vasync.barrier();
            barrier.on('drain', _cb);
            barrier.start(0);
            barrier.start(1);
            if (seed < 1) {
                MANATEES[_.primaryPgUrl].start(function (err) {
                    if (err) {
                        return _cb(err);
                    }
                    barrier.done(0);
                });
                MANATEES[_.syncPgUrl].start(function (err) {
                    if (err) {
                        return _cb(err);
                    }
                    barrier.done(1);
                });
            } else {
                MANATEES[_.syncPgUrl].start(function (err) {
                    if (err) {
                        return _cb(err);
                    }
                    barrier.done(1);
                });
                MANATEES[_.primaryPgUrl].start(function (err) {
                    if (err) {
                        return _cb(err);
                    }
                    barrier.done(0);
                });
            }
        },
        function checkTopology2(_, _cb) {
            _cb = once(_cb);
            var intervalId = setInterval(function() {vasync.pipeline({funcs: [
                function _getTopology(_2, _cb2) {
                    mantee_common.loadTopology(ZK_CLIENT, function (err, topology) {
                        _2.topology = topology[SHARD_ID];
                        if (err) {
                            return _cb2(err);
                        }
                        LOG.info({topology: topology});
                        return _cb2();
                    });
                },
                function _getPgStatus(_2, _cb2) {
                    try {
                        mantee_common.pgStatus([_2.topology], _cb2);
                    } catch (e) {
                        LOG.warn({err: e, topology: _2.topology});
                        return _cb2(e);
                    }
                },
                function _verifyTopology(_2, _cb2) {
                    _cb2 = once(_cb2);
                    try {
                         /*
                          * here we only have to check the sync states of each
                          * of the nodes.  if the sync states are correct, then
                          * we know replication is working.
                          */
                         assert.ok(_2.topology, 'shard topology DNE');
                         assert.ok(_2.topology.primary, 'primary DNE');
                         assert.ok(_2.topology.primary.repl,
                                   'no sync repl state');
                         assert.equal(_2.topology.primary.repl.sync_state,
                                      'sync',
                                      'no sync replication state.');
                         assert.ok(_2.topology.sync, 'sync DNE');
                         assert.equal(_2.topology.sync.repl.sync_state,
                                      'async',
                                      'no async replication state');
                         assert.ok(_2.topology.async, 'async DNE');
                        return _cb2();
                    } catch (e) {
                        LOG.warn({err: e, topology: _2.topology},
                                 'unable to verify topology');
                        return _cb2(e);
                    }
                }
            ], arg:{}}, function (err, results) {
                if (err) {
                    LOG.warn({err: err, results: results},
                             'topology not correct');
                    return;
                }
                clearInterval(intervalId);
                return _cb();
            });}, 3000);

            setTimeout(function() {
                clearInterval(intervalId);
                return _cb(new verror.VError(
                    'new peer did not join shard in time'));
            }, 30000);
        },
    ], arg: {}}, function (err, results) {
        if (err) {
            LOG.error({err: err, results: results});
            t.fail(err);
        }
        t.done();
    });
};

exports.after = function (t) {
    vasync.pipeline({funcs: [
        function _stopManatees(_, _cb) {
            var barrier = vasync.barrier();
            barrier.on('drain', function () {
                return _cb();
            });

            Object.keys(MANATEES).forEach(function (m) {
                var id = uuid.v4();
                barrier.start(id);
                MANATEES[m].kill(function () {
                    barrier.done(id);
                });
            });
        },
        function _destroyZfsDataset(_, _cb) {
            _cb = once(_cb);
            var intervalId = setInterval(function () {
                exec('zfs destroy -r ' + PARENT_ZFS_DS, function (err) {
                    if (!err) {
                        clearInterval(intervalId);
                        return _cb();
                    }
                    LOG.warn({err: err}, 'unable to destroy zfs dataset');
                });
            }, 2000);

            setTimeout(function () {
                clearInterval(intervalId);
                return _cb(new verror.VError(
                    'timed out trying to destroy dataset'));
            }, 30000);
        },
        function _cleanupZK(_, _cb) {
            ZK_CLIENT.rmr('/manatee', _cb);
        },
    ], arg: {}}, function (err, results) {
        LOG.info({err: err, results: err ? results : null}, 'finished after()');
        t.done();
    });
};
