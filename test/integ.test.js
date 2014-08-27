/*
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
var bunyan = require('bunyan');
var leader = require('leader');
var ManateeClient = require('node-manatee');
var ConfParser = require('../lib/confParser');
var fs = require('fs');
var exec = require('child_process').exec;
var path = require('path');
var manatee_common = require('../node_modules/node-manatee/bin/manatee_common');
var Manatee = require('./testManatee');
var once = require('once');
var shelljs = require('shelljs');
var util = require('util');
var uuid = require('node-uuid');
var vasync = require('vasync');
var verror = require('verror');
var zk = require('node-zookeeper-client');

var FS_PATH_PREFIX = process.env.FS_PATH_PREFIX || '/var/tmp/manatee_tests';
var ZK_URL = process.env.ZK_URL || 'localhost:2181';
var PARENT_ZFS_DS = process.env.PARENT_ZFS_DS;
//var SHARD_ID = '8dbdcef3-a82b-4403-bab0-a5c4053bb40f';
var SHARD_ID = uuid.v4();
var SHARD_PATH = '/manatee/' + SHARD_ID;
console.error('shard path', SHARD_PATH + '/election');
var SITTER_CFG = './etc/sitter.json';
var BS_CFG = './etc/backupserver.json';
var SS_CFG = './etc/snapshotter.json';
var MY_IP = '127.0.0.1';
var ZK_CLIENT = null;

var TIMEOUT = process.env.TEST_TIMEOUT || (30 * 1000);
var PG_UID = process.env.PG_UID ? parseInt(process.env.PG_UID, 10) : null;

var LOG = bunyan.createLogger({
    level: (process.env.LOG_LEVEL || 'warn'),
    name: 'manatee-integ-tests',
    serializers: {
        err: bunyan.stdSerializers.err
    },
    src: true
});

var n1Opts = null;
var n2Opts = null;
var n3Opts = null;

var MANATEES = {};

var MANATEE_CLIENT = null;

/* JSSTYLED */
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
    n1Opts = {
        zfsDataset: PARENT_ZFS_DS + '/' + n1,
        zfsPort: n1Port,
        mountPoint: FS_PATH_PREFIX + '/' + n1,
        backupPort: ++n1Port,
        postgresPort: ++n1Port,
        cookieLocation: FS_PATH_PREFIX + '/' + n1 + '_metadata' + '/cookie',
        backupServerPort: ++n1Port,
        configLocation: FS_PATH_PREFIX + '/' + n1 + '_metadata' + '/config',
        metadataDir: FS_PATH_PREFIX + '/' + n1 + '_metadata',
        shardPath: SHARD_PATH,
        log: LOG,
        postgresUserId: PG_UID
    };
    n2Opts = {
        zfsDataset: PARENT_ZFS_DS + '/' + n2,
        zfsPort: n2Port,
        mountPoint: FS_PATH_PREFIX + '/' + n2,
        backupPort: ++n2Port,
        postgresPort: ++n2Port,
        cookieLocation: FS_PATH_PREFIX + '/' + n2 + '_metadata' + '/cookie',
        backupServerPort: ++n2Port,
        configLocation: FS_PATH_PREFIX + '/' + n2 + '_metadata' + '/config',
        metadataDir: FS_PATH_PREFIX + '/' + n2 + '_metadata',
        shardPath: SHARD_PATH,
        postgresUserId: PG_UID,
        log: LOG
    };
    n3Opts = {
        zfsDataset: PARENT_ZFS_DS + '/' + n3,
        zfsPort: n3Port,
        mountPoint: FS_PATH_PREFIX + '/' + n3,
        backupPort: ++n3Port,
        postgresPort: ++n3Port,
        cookieLocation: FS_PATH_PREFIX + '/' + n3 + '_metadata' + '/cookie',
        backupServerPort: ++n3Port,
        configLocation: FS_PATH_PREFIX + '/' + n3 + '_metadata' + '/config',
        metadataDir: FS_PATH_PREFIX + '/' + n3 + '_metadata',
        shardPath: SHARD_PATH,
        postgresUserId: PG_UID,
        log: LOG
    };

    vasync.pipeline({funcs: [
        function _createZkClient(_, _cb) {
            manatee_common.createZkClient({
                zk: ZK_URL,
                shard: SHARD_PATH
            }, function (err, zook) {
                ZK_CLIENT = zook;

                return _cb(err);
            });
        },
        function _destroyZfsDataset(_, _cb) {
            exec('zfs destroy -r ' + PARENT_ZFS_DS, function (err) {
                return _cb();
            });
        },
        function _removeMetadata(_, _cb) {
            exec('rm -rf ' + FS_PATH_PREFIX, _cb);
        },
        function _startN1(_, _cb) {
            var manatee = new Manatee(n1Opts, function (err) {
                if (err) {
                    LOG.warn({err: err}, 'could not start manatee');
                    return (_cb);
                }

                MANATEES[manatee.pgUrl] = manatee;
                return _cb();
            });
        },
        function _startN2(_, _cb) {
            var manatee = new Manatee(n2Opts, function (err) {
                if (err) {
                    LOG.warn({err: err}, 'could not start manatee');
                    return (_cb);
                }

                MANATEES[manatee.pgUrl] = manatee;
                return _cb();
            });
        },
        function _waitForSyncReplication(_, _cb) {
            _cb = once(_cb);
            var topology;
            var intervalId = setInterval(function () {vasync.pipeline({funcs: [
                function _loadTopology(_2, _cb2) {
                    manatee_common.loadTopology(ZK_CLIENT, function (err, top) {
                        if (err) {
                            return _cb2(err);
                        }
                        topology = top[SHARD_ID];
                        try {
                            assert.ok(topology, 'topology DNE');
                            assert.ok(topology.primary, 'primary DNE');
                            assert.ok(topology.sync, 'sync DNE');

                            return _cb2();
                        } catch (e) {
                            LOG.info({err: e, topology: topology},
                                     'got unexpected topology');
                            return _cb2(e);
                        }
                   });
                },
                function _getPgStatus(_2, _cb2) {
                    try {
                        manatee_common.pgStatus([topology], _cb2);
                    } catch (e) {
                        LOG.info({err: e, topology: topology},
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
                        assert.equal(topology.primary.repl.sync_state,
                                'sync',
                                'no sync replication state.');
                        return _cb2();
                    } catch (e) {
                        LOG.info({err: e, topology: topology},
                                 'unable to verify topology');
                        return _cb2(e);
                    }
                }            ], arg: {}}, function (err, results) {
                if (err) {
                    LOG.info({err: err, results: results, topology: topology},
                             'still waiting for correct shard state');
                    return;
                } else {
                    clearInterval(intervalId);
                    return _cb();
                }

            }); }, 3000);

            setTimeout(function () {
                clearInterval(intervalId);
                return _cb(new verror.VError('shard did not flip in time'));
            }, TIMEOUT).unref();
        },
        function _startN3(_, _cb) {
            var manatee = new Manatee(n3Opts, function (err) {
                if (err) {
                    LOG.warn({err: err}, 'could not start manatee');
                    return (_cb);
                }

                MANATEES[manatee.pgUrl] = manatee;
                return _cb();
            });
        },
        function _waitForSyncReplication2(_, _cb) {
            _cb = once(_cb);
            var topology;
            var intervalId = setInterval(function () {vasync.pipeline({funcs: [
                function _loadTopology(_2, _cb2) {
                    manatee_common.loadTopology(ZK_CLIENT, function (err, top) {
                        if (err) {
                            return _cb2(err);
                        }
                        topology = top[SHARD_ID];
                        try {
                            assert.ok(topology, 'topology DNE');
                            assert.ok(topology.primary, 'primary DNE');
                            assert.ok(topology.sync, 'sync DNE');
                            assert.ok(topology.async, 'async DNE');

                            return _cb2();
                        } catch (e) {
                            LOG.info({err: e, topology: topology},
                                     'got unexpected topology');
                            return _cb2(e);
                        }
                   });
                },
                function _getPgStatus(_2, _cb2) {
                    try {
                        manatee_common.pgStatus([topology], _cb2);
                    } catch (e) {
                        LOG.info({err: e, topology: topology},
                                 'could not get pg status');
                        return _cb2(e);
                    }
                },
                function _verifyTopology(_2, _cb2) {
                    _2.topology = topology;
                    try {
                        /*
                         * here we only have to check the sync states of each
                         * of the nodes.  if the sync states are correct, then
                         * we know replication is working.
                         */
                        assert.ok(topology, 'shard topology DNE');
                        assert.ok(topology.primary, 'primary DNE');
                        assert.ok(topology.primary.repl, 'no sync repl state');
                        assert.equal(topology.primary.repl.sync_state,
                                'sync',
                                'no sync replication state.');
                        assert.equal(topology.sync.repl.sync_state,
                                'async',
                                'no sync replication state.');
                        return _cb2();
                    } catch (e) {
                        LOG.info({err: e, topology: topology},
                                 'unable to verify topology');
                        return _cb2(e);
                    }
                },
                function _verifySyncState(_2, _cb2) {
                    try {
                        var p = fs.readFileSync(MANATEES[_2.
                                                topology.primary.pgUrl].
                                                cookieLocation, 'utf8');
                        var s = fs.readFileSync(MANATEES[_2.topology.sync.
                                                pgUrl].cookieLocation, 'utf8');
                        var a = fs.readFileSync(MANATEES[_2.topology.async.
                                                pgUrl].cookieLocation, 'utf8');

                        assert.equal('primary', JSON.parse(p).role);
                        assert.equal('sync', JSON.parse(s).role);
                        assert.equal('async', JSON.parse(a).role);
                        return _cb2();
                    } catch (e) {
                        LOG.error({err: e, topology: _2.topology},
                                 'unable to verify sync state');
                        return _cb2(e);
                    }
                }
            ], arg: {}}, function (err, results) {
                if (err) {
                    LOG.info({err: err, results: results, topology: topology},
                             'still waiting for correct shard state');
                    return;
                } else {
                    clearInterval(intervalId);
                    return _cb();
                }

            }); }, 3000);

            setTimeout(function () {
                clearInterval(intervalId);
                return _cb(new verror.VError('shard did not flip in time'));
            }, TIMEOUT).unref();
        }
    ], arg: {}}, function (err, results) {
        if (err) {
            t.fail(err);
        }
        t.done();
    });
};

//exports.initClient = function (t) {
function clientTest(t) {
    MANATEE_CLIENT = ManateeClient.createClient({
        path: SHARD_PATH + '/election',
        zk: {
            servers: [ {host: '127.0.0.1', port: 2181} ],
            timeout: 30000
        }
    });
    var emitReady = false;
    var done = once(t.done);
    var id = setTimeout(function () {
        t.fail('client test exceeded tiemout');
        MANATEE_CLIENT.removeAllListeners();
        done();
    }, TIMEOUT).unref();

    MANATEE_CLIENT.once('topology', function (dbs) {
        var barrier = vasync.barrier();
        barrier.on('drain', function () {
            t.ok(emitReady, 'manatee client did not emit ready event');
            clearTimeout(id);
            MANATEE_CLIENT.removeAllListeners();
            done();
        });
        Object.keys(MANATEES).forEach(function (k) {
            var m = MANATEES[k];
            barrier.start(m.getPgUrl());
            if (dbs.indexOf(m.getPgUrl()) === -1) {
                t.fail('client did not get url ' + m.getPgUrl());
            }
            barrier.done(m.getPgUrl());
        });
    });

    MANATEE_CLIENT.once('ready', function () {
        emitReady = true;
    });
}

exports.setupMoray = function (t) {
    /* JSSTYLED */
    var createBucketConfig = 'sudo -u postgres psql -p 10002 -c "CREATE TABLE buckets_config ( name text PRIMARY KEY, index text NOT NULL, pre text NOT NULL, post text NOT NULL, options text, mtime timestamp without time zone DEFAULT now() NOT NULL);"';
    exec(createBucketConfig, function (err) {
        if (err) {
            LOG.warn({err: err}, 'unable to create moray bucket config');
        }

        t.done();
    });
};

exports.verifyShard = function (t) {
    vasync.pipeline({funcs: [
        function loadTopology(_, _cb) {
            manatee_common.loadTopology(ZK_CLIENT, function (err, topology) {
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
            manatee_common.pgStatus([_.topology], _cb);
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
            LOG.warn({err: err, results: results},
                      'check shard status failed');
                      t.fail(err);
        }
        t.done();
    });
};

exports.primaryDeath = function (t) {
//function foo () {
    vasync.pipeline({funcs: [
        function loadAndVerifyTopology(_, _cb) {
            getTopology(function (err, topology) {
                _.topology = topology;
                _.primaryPgUrl = _.topology.primary.pgUrl;
                return _cb(err);
            });
        },
        function killPrimary(_, _cb) {
            MANATEES[_.primaryPgUrl].kill(_cb);
        },
        function getNewTopology(_, _cb) {
            _cb = once(_cb);
            var topology;
            var intervalId = setInterval(function () {vasync.pipeline({funcs: [
                function _loadTopology(_2, _cb2) {
                    manatee_common.loadTopology(ZK_CLIENT, function (err, top) {
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
                                             'primary death');

                            LOG.info({topology: topology}, 'got topology');
                            return _cb2();
                        } catch (e) {
                            LOG.info({err: e, topology: topology},
                                     'got unexpected topology');
                            return _cb2(e);
                        }
                   });
                },
                function _getPgStatus(_2, _cb2) {
                    try {
                        manatee_common.pgStatus([topology], _cb2);
                    } catch (e) {
                        LOG.info({err: e, topology: topology},
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
                        LOG.info({err: e, topology: topology},
                                 'unable to verify topology');
                        return _cb2(e);
                    }
                }
            ], arg: {}}, function (err, results) {
                if (err) {
                    LOG.info({err: err, results: results, topology: topology},
                             'still waiting for correct shard state');
                    return;
                } else {
                    clearInterval(intervalId);
                    return _cb();
                }

            }); }, 3000);

            setTimeout(function () {
                clearInterval(intervalId);
                return _cb(new verror.VError('shard did not flip in time'));
            }, TIMEOUT).unref();
        },
        function addNewManatee(_, _cb) {
            LOG.info({url: _.primaryPgUrl}, 'adding back old primary');
            MANATEES[_.primaryPgUrl].start(_cb);
        },
        function checkTopology2(_, _cb) {
            _cb = once(_cb);
            var intervalId = setInterval(function () {vasync.pipeline({funcs: [
                function _getTopology(_2, _cb2) {
                    manatee_common.loadTopology(ZK_CLIENT,
                                                function (err, topology) {
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
                        manatee_common.pgStatus([_2.topology], _cb2);
                    } catch (e) {
                        LOG.info({err: e, topology: _2.topology});
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
                        assert.notEqual(_2.topology.primary.pgUrl,
                                        _2.primaryPgUrl,
                                        'primary should not be killed primary');
                        return _cb2();
                    } catch (e) {
                        LOG.info({err: e, topology: _2.topology});
                        return _cb2(e);
                    }
                },
                function _verifySyncState(_2, _cb2) {
                    try {
                        var p = fs.readFileSync(MANATEES[_2.
                                                topology.primary.pgUrl].
                                                cookieLocation, 'utf8');
                        var s = fs.readFileSync(MANATEES[_2.topology.sync.
                                                pgUrl].cookieLocation, 'utf8');
                        var a = fs.readFileSync(MANATEES[_2.topology.async.
                                                pgUrl].cookieLocation, 'utf8');

                        assert.equal('primary', JSON.parse(p).role);
                        assert.equal('sync', JSON.parse(s).role);
                        assert.equal('async', JSON.parse(a).role);
                        return _cb2();
                    } catch (e) {
                        LOG.warn({err: e, topology: _2.topology},
                                 'unable to verify sync state');
                        return _cb2(e);
                    }
                }
            ], arg: {}}, function (err, results) {
                if (err) {
                    LOG.info({err: err, results: results},
                             'topology not correct');
                    return;
                }
                clearInterval(intervalId);
                return _cb();
            }); }, 3000);

            setTimeout(function () {
                clearInterval(intervalId);
                return _cb(new verror.VError(
                    'new peer did not join shard in time'));
            }, TIMEOUT).unref();
        }
    ], arg: {}}, function (err, results) {
        if (err) {
            LOG.error({err: err, results: results});
            t.fail(err);
        }
        t.done();
    });
};

//function foo () {
exports.syncDeath = function (t) {
    vasync.pipeline({funcs: [
        function loadAndVerifyTopology(_, _cb) {
            getTopology(function (err, topology) {
                _.topology = topology;
                _.primaryPgUrl = _.topology.primary.pgUrl;
                _.syncPgUrl = _.topology.sync.pgUrl;
                _.asyncPgUrl = _.topology.async.pgUrl;
                return _cb(err);
            });
        },
        function killSync(_, _cb) {
            MANATEES[_.syncPgUrl].kill(_cb);
        },
        function getNewTopology(_, _cb) {
            _cb = once(_cb);
            var topology;
            var intervalId = setInterval(function () {vasync.pipeline({funcs: [
                function _loadTopology(_2, _cb2) {
                    manatee_common.loadTopology(ZK_CLIENT, function (err, top) {
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
                            LOG.info({err: e, topology: topology},
                                     'got unexpected topology');
                            return _cb2(e);
                        }
                   });
                },
                function _getPgStatus(_2, _cb2) {
                    try {
                        manatee_common.pgStatus([topology], _cb2);
                    } catch (e) {
                        LOG.info({err: e, topology: topology},
                                 'could not get pg status');
                        return _cb2(e);
                    }
                },
                function _verifyTopology(_2, _cb2) {
                    _2.topology = topology;
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
                        LOG.info({err: e, topology: topology},
                                 'unable to verify topology');
                        return _cb2(e);
                    }
                },
                function _verifySyncState(_2, _cb2) {
                    try {
                        var p = fs.readFileSync(MANATEES[_2.
                                                topology.primary.pgUrl].
                                                cookieLocation, 'utf8');
                        var s = fs.readFileSync(MANATEES[_2.topology.sync.
                                                pgUrl].cookieLocation, 'utf8');

                        assert.equal('primary', JSON.parse(p).role);
                        assert.equal('sync', JSON.parse(s).role);
                        return _cb2();
                    } catch (e) {
                        LOG.warn({err: e, topology: _2.topology},
                                 'unable to verify sync state');
                        return _cb2(e);
                    }
                }
            ], arg: {}}, function (err, results) {
                if (err) {
                    LOG.info({err: err, results: results, topology: topology},
                             'still waiting for correct shard state');
                    return;
                } else {
                    clearInterval(intervalId);
                    return _cb();
                }

            }); }, 3000);

            setTimeout(function () {
                clearInterval(intervalId);
                return _cb(new verror.VError('shard did not flip in time'));
            }, TIMEOUT).unref();
        },
        function addNewManatee(_, _cb) {
            LOG.info({url: _.syncPgUrl}, 'adding back old sync');
            MANATEES[_.syncPgUrl].start(_cb);
        },
        function checkTopology2(_, _cb) {
            _cb = once(_cb);
            var intervalId = setInterval(function () {vasync.pipeline({funcs: [
                function _getTopology(_2, _cb2) {
                    manatee_common.loadTopology(ZK_CLIENT,
                                                function (err, topology) {
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
                        manatee_common.pgStatus([_2.topology], _cb2);
                    } catch (e) {
                        LOG.info({err: e, topology: _2.topology});
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
                        LOG.info({err: e, topology: _2.topology});
                        return _cb2(e);
                    }
                },
                function _verifySyncState(_2, _cb2) {
                    try {
                        var p = fs.readFileSync(MANATEES[_2.
                                                topology.primary.pgUrl].
                                                cookieLocation, 'utf8');
                        var s = fs.readFileSync(MANATEES[_2.topology.sync.
                                                pgUrl].cookieLocation, 'utf8');
                        var a = fs.readFileSync(MANATEES[_2.topology.async.
                                                pgUrl].cookieLocation, 'utf8');

                        assert.equal('primary', JSON.parse(p).role);
                        assert.equal('sync', JSON.parse(s).role);
                        assert.equal('async', JSON.parse(a).role);
                        return _cb2();
                    } catch (e) {
                        LOG.warn({err: e, topology: _2.topology},
                                 'unable to verify sync state');
                        return _cb2(e);
                    }
                }
            ], arg: {}}, function (err, results) {
                if (err) {
                    LOG.info({err: err, results: results},
                             'topology not correct');
                    return;
                }
                clearInterval(intervalId);
                return _cb();
            }); }, 3000);

            setTimeout(function () {
                clearInterval(intervalId);
                return _cb(new verror.VError(
                    'new peer did not join shard in time'));
            }, TIMEOUT).unref();
        }
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
//function foo () {
    vasync.pipeline({funcs: [
        function loadAndVerifyTopology(_, _cb) {
            getTopology(function (err, topology) {
                _.topology = topology;
                _.primaryPgUrl = _.topology.primary.pgUrl;
                _.syncPgUrl = _.topology.sync.pgUrl;
                _.asyncPgUrl = _.topology.async.pgUrl;
                return _cb(err);
            });
        },
        function killAsync(_, _cb) {
            MANATEES[_.asyncPgUrl].kill(_cb);
        },
        function getNewTopology(_, _cb) {
            _cb = once(_cb);
            var topology;
            var intervalId = setInterval(function () {vasync.pipeline({funcs: [
                function _loadTopology(_2, _cb2) {
                    manatee_common.loadTopology(ZK_CLIENT, function (err, top) {
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
                            LOG.info({err: e, topology: topology},
                                     'got unexpected topology');
                            return _cb2(e);
                        }
                   });
                },
                function _getPgStatus(_2, _cb2) {
                    try {
                        manatee_common.pgStatus([topology], _cb2);
                    } catch (e) {
                        LOG.info({err: e, topology: topology},
                                 'could not get pg status');
                        return _cb2(e);
                    }
                },
                function _verifyTopology(_2, _cb2) {
                    _2.topology = topology;
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
                        LOG.info({err: e, topology: topology},
                                 'unable to verify topology');
                        return _cb2(e);
                    }
                },
                function _verifySyncState(_2, _cb2) {
                    try {
                        var p = fs.readFileSync(MANATEES[_2.
                                                topology.primary.pgUrl].
                                                cookieLocation, 'utf8');
                        var s = fs.readFileSync(MANATEES[_2.topology.sync.
                                                pgUrl].cookieLocation, 'utf8');

                        assert.equal('primary', JSON.parse(p).role);
                        assert.equal('sync', JSON.parse(s).role);
                        return _cb2();
                    } catch (e) {
                        LOG.warn({err: e, topology: _2.topology},
                                 'unable to verify sync state');
                        return _cb2(e);
                    }
                }
            ], arg: {}}, function (err, results) {
                if (err) {
                    LOG.info({err: err, results: results, topology: topology},
                             'still waiting for correct shard state');
                    return;
                } else {
                    clearInterval(intervalId);
                    return _cb();
                }

            }); }, 3000);

            setTimeout(function () {
                clearInterval(intervalId);
                return _cb(new verror.VError('shard did not flip in time'));
            }, TIMEOUT).unref();
        },
        function addNewManatee(_, _cb) {
            LOG.info({url: _.asyncPgUrl}, 'adding back old async');
            MANATEES[_.asyncPgUrl].start(_cb);
        },
        function checkTopology2(_, _cb) {
            _cb = once(_cb);
            var intervalId = setInterval(function () {vasync.pipeline({funcs: [
                function _getTopology(_2, _cb2) {
                    manatee_common.loadTopology(ZK_CLIENT,
                                                function (err, topology) {
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
                        manatee_common.pgStatus([_2.topology], _cb2);
                    } catch (e) {
                        LOG.info({err: e, topology: _2.topology});
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
                        LOG.info({err: e, topology: _2.topology},
                                 'unable to verify topology');
                        return _cb2(e);
                    }
                },
                function _verifySyncState(_2, _cb2) {
                    try {
                        var p = fs.readFileSync(MANATEES[_2.
                                                topology.primary.pgUrl].
                                                cookieLocation, 'utf8');
                        var s = fs.readFileSync(MANATEES[_2.topology.sync.
                                                pgUrl].cookieLocation, 'utf8');
                        var a = fs.readFileSync(MANATEES[_2.topology.async.
                                                pgUrl].cookieLocation, 'utf8');

                        assert.equal('primary', JSON.parse(p).role);
                        assert.equal('sync', JSON.parse(s).role);
                        assert.equal('async', JSON.parse(a).role);
                        return _cb2();
                    } catch (e) {
                        LOG.warn({err: e, topology: _2.topology},
                                 'unable to verify sync state');
                        return _cb2(e);
                    }
                }
            ], arg: {}}, function (err, results) {
                if (err) {
                    LOG.info({err: err, results: results},
                             'topology not correct');
                    return;
                }
                clearInterval(intervalId);
                return _cb();
            }); }, 3000);

            setTimeout(function () {
                clearInterval(intervalId);
                return _cb(new verror.VError(
                    'new peer did not join shard in time'));
            }, TIMEOUT).unref();
        }
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
//function foo () {
    vasync.pipeline({funcs: [
        function loadAndVerifyTopology(_, _cb) {
            getTopology(function (err, topology) {
                _.topology = topology;
                _.primaryPgUrl = _.topology.primary.pgUrl;
                _.syncPgUrl = _.topology.sync.pgUrl;
                _.asyncPgUrl = _.topology.async.pgUrl;
                return _cb(err);
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
            var intervalId = setInterval(function () {vasync.pipeline({funcs: [
                function _loadTopology(_2, _cb2) {
                    manatee_common.loadTopology(ZK_CLIENT, function (err, top) {
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
                            LOG.info({err: e, topology: topology},
                                     'got unexpected topology');
                            return _cb2(e);
                        }
                   });
                }
            ], arg: {}}, function (err, results) {
                if (err) {
                    LOG.info({err: err, results: results, topology: topology},
                             'still waiting for correct shard state');
                    return;
                } else {
                    clearInterval(intervalId);
                    return _cb();
                }

            }); }, 3000);

            setTimeout(function () {
                clearInterval(intervalId);
                return _cb(new verror.VError('shard did not flip in time'));
            }, TIMEOUT).unref();
        },
        function restartManatees(_, _cb) {
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
            var intervalId = setInterval(function () {vasync.pipeline({funcs: [
                function _getTopology(_2, _cb2) {
                    manatee_common.loadTopology(ZK_CLIENT,
                                                function (err, topology) {
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
                        manatee_common.pgStatus([_2.topology], _cb2);
                    } catch (e) {
                        LOG.info({err: e, topology: _2.topology});
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
                        LOG.info({err: e, topology: _2.topology},
                                 'unable to verify topology');
                        return _cb2(e);
                    }
                },
                function _verifySyncState(_2, _cb2) {
                    try {
                        var p = fs.readFileSync(MANATEES[_2.
                                                topology.primary.pgUrl].
                                                cookieLocation, 'utf8');
                        var s = fs.readFileSync(MANATEES[_2.topology.sync.
                                                pgUrl].cookieLocation, 'utf8');
                        var a = fs.readFileSync(MANATEES[_2.topology.async.
                                                pgUrl].cookieLocation, 'utf8');

                        assert.equal('primary', JSON.parse(p).role);
                        assert.equal('sync', JSON.parse(s).role);
                        assert.equal('async', JSON.parse(a).role);
                        return _cb2();
                    } catch (e) {
                        LOG.warn({err: e, topology: _2.topology},
                                 'unable to verify sync state');
                        return _cb2(e);
                    }
                }
            ], arg: {}}, function (err, results) {
                if (err) {
                    LOG.info({err: err, results: results},
                             'topology not correct');
                    return;
                }
                clearInterval(intervalId);
                return _cb();
            }); }, 3000);

            setTimeout(function () {
                clearInterval(intervalId);
                return _cb(new verror.VError(
                    'new peer did not join shard in time'));
            }, TIMEOUT).unref();
        }
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
//function foo() {
    var topo;
    vasync.pipeline({funcs: [
        function loadAndVerifyTopology(_, _cb) {
            getTopology(function (err, topology) {
                _.topology = topology;
                _.primaryPgUrl = _.topology.primary.pgUrl;
                _.syncPgUrl = _.topology.sync.pgUrl;
                _.asyncPgUrl = _.topology.async.pgUrl;
                return _cb(err);
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
            var intervalId = setInterval(function () {vasync.pipeline({funcs: [
                function _loadTopology(_2, _cb2) {
                    manatee_common.loadTopology(ZK_CLIENT, function (err, top) {
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
                            assert.equal(topology.primary, null,
                                         'primary exists');
                            assert.equal(topology.sync, null, 'sync exists');
                            assert.equal(topology.async, null, 'async exists');

                            return _cb2();
                        } catch (e) {
                            LOG.info({err: e, topology: topology},
                                     'got unexpected topology');
                            return _cb2(e);
                        }
                   });
                }
            ], arg: {}}, function (err, results) {
                if (err) {
                    LOG.info({err: err, results: results, topology: topology},
                             'still waiting for correct shard state');
                    return;
                } else {
                    clearInterval(intervalId);
                    return _cb();
                }

            }); }, 3000);

            setTimeout(function () {
                clearInterval(intervalId);
                return _cb(new verror.VError('shard did not flip in time'));
            }, TIMEOUT).unref();
        },
        function restartManatees(_, _cb) {
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
            var intervalId = setInterval(function () {vasync.pipeline({funcs: [
                function _getTopology(_2, _cb2) {
                    manatee_common.loadTopology(ZK_CLIENT,
                                                function (err, topology) {
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
                        manatee_common.pgStatus([_2.topology], _cb2);
                    } catch (e) {
                        LOG.info({err: e, topology: _2.topology});
                        return _cb2(e);
                    }
                },
                function _verifyTopology(_2, _cb2) {
                    _cb2 = once(_cb2);
                    topo = _2.topology;
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
                        LOG.info({err: e, topology: _2.topology},
                                 'unable to verify topology');
                        return _cb2(e);
                    }
                },
                function _verifySyncState(_2, _cb2) {
                    try {
                        var p = fs.readFileSync(MANATEES[_2.
                                                topology.primary.pgUrl].
                                                cookieLocation, 'utf8');
                        var s = fs.readFileSync(MANATEES[_2.topology.sync.
                                                pgUrl].cookieLocation, 'utf8');
                        var a = fs.readFileSync(MANATEES[_2.topology.async.
                                                pgUrl].cookieLocation, 'utf8');

                        assert.equal('primary', JSON.parse(p).role);
                        assert.equal('sync', JSON.parse(s).role);
                        assert.equal('async', JSON.parse(a).role);
                        return _cb2();
                    } catch (e) {
                        LOG.warn({err: e, topology: _2.topology},
                                 'unable to verify sync state');
                        return _cb2(e);
                    }
                }
            ], arg: {}}, function (err, results) {
                if (err) {
                    LOG.info({err: err, results: results},
                             'topology not correct');
                    return;
                }
                clearInterval(intervalId);
                return _cb();
            }); }, 3000);

            setTimeout(function () {
                clearInterval(intervalId);
                if (!_cb.called) {
                    LOG.fatal({topology: topo},
                              'new peer did not join shard in time');
                }
                return _cb(new verror.VError(
                    'new peer did not join shard in time'));
            }, TIMEOUT).unref();
        }
    ], arg: {}}, function (err, results) {
        if (err) {
            LOG.error({err: err, results: results});
            t.fail(err);
        }
        t.done();
    });
};

exports.primaryAsyncInstantaneousDeath = function (t) {
    var topo;
    vasync.pipeline({funcs: [
        function loadAndVerifyTopology(_, _cb) {
            getTopology(function (err, topology) {
                _.topology = topology;
                _.primaryPgUrl = _.topology.primary.pgUrl;
                _.syncPgUrl = _.topology.sync.pgUrl;
                _.asyncPgUrl = _.topology.async.pgUrl;
                return _cb(err);
            });
        },
        function killPrimaryAndAsync(_, _cb) {
            // hacky way to pick either async or primary first
            var seed = Math.round(Math.random());
            var barrier = vasync.barrier();
            barrier.on('drain', _cb);
            barrier.start(0);
            barrier.start(1);
            if (seed < 1) {
                MANATEES[_.primaryPgUrl].kill(function () {
                    barrier.done(0);
                });
                MANATEES[_.asyncPgUrl].kill(function () {
                    barrier.done(1);
                });
            } else {
                MANATEES[_.asyncPgUrl].kill(function () {
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
            var intervalId = setInterval(function () {vasync.pipeline({funcs: [
                function _loadTopology(_2, _cb2) {
                    manatee_common.loadTopology(ZK_CLIENT, function (err, top) {
                        if (err) {
                            return _cb2(err);
                        }
                        topology = top[SHARD_ID];
                        try {
                            assert.ok(topology, 'topology DNE');
                            assert.ok(topology.primary, 'primary DNE');
                            assert.equal(topology.sync, null, 'sync exists');
                            assert.equal(topology.async, null, 'async exists');

                            return _cb2();
                        } catch (e) {
                            LOG.info({err: e, topology: topology},
                                     'got unexpected topology');
                            return _cb2(e);
                        }
                   });
                }
            ], arg: {}}, function (err, results) {
                if (err) {
                    LOG.info({err: err, results: results, topology: topology},
                             'still waiting for correct shard state');
                    return;
                } else {
                    clearInterval(intervalId);
                    return _cb();
                }

            }); }, 3000);

            setTimeout(function () {
                clearInterval(intervalId);
                if (!_cb.called) {
                    LOG.fatal({topology: topology},
                              'shard did not flip in time');
                }
                return _cb(new verror.VError('shard did not flip in time'));
            }, TIMEOUT).unref();
        },
        function restartManatees(_, _cb) {
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
                MANATEES[_.asyncPgUrl].start(function (err) {
                    if (err) {
                        return _cb(err);
                    }
                    barrier.done(1);
                });
            } else {
                MANATEES[_.asyncPgUrl].start(function (err) {
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
            var intervalId = setInterval(function () {vasync.pipeline({funcs: [
                function _getTopology(_2, _cb2) {
                    manatee_common.loadTopology(ZK_CLIENT,
                                                function (err, topology) {
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
                        manatee_common.pgStatus([_2.topology], _cb2);
                    } catch (e) {
                        LOG.info({err: e, topology: _2.topology});
                        return _cb2(e);
                    }
                },
                function _verifyTopology(_2, _cb2) {
                    _cb2 = once(_cb2);
                    topo = _2.topology;
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
                        LOG.info({err: e, topology: _2.topology},
                                 'unable to verify topology');
                        return _cb2(e);
                    }
                },
                function _verifySyncState(_2, _cb2) {
                    try {
                        var p = fs.readFileSync(MANATEES[_2.
                                                topology.primary.pgUrl].
                                                cookieLocation, 'utf8');
                        var s = fs.readFileSync(MANATEES[_2.topology.sync.
                                                pgUrl].cookieLocation, 'utf8');
                        var a = fs.readFileSync(MANATEES[_2.topology.async.
                                                pgUrl].cookieLocation, 'utf8');

                        assert.equal('primary', JSON.parse(p).role);
                        assert.equal('sync', JSON.parse(s).role);
                        assert.equal('async', JSON.parse(a).role);
                        return _cb2();
                    } catch (e) {
                        LOG.warn({err: e, topology: _2.topology},
                                 'unable to verify sync state');
                        return _cb2(e);
                    }
                }
            ], arg: {}}, function (err, results) {
                if (err) {
                    LOG.info({err: err, results: results},
                             'topology not correct');
                    return;
                }
                clearInterval(intervalId);
                return _cb();
            }); }, 3000);

            setTimeout(function () {
                clearInterval(intervalId);
                if (!_cb.called) {
                    LOG.fatal({topology: topo},
                              'new peer did not join shard in time');
                }
                return _cb(new verror.VError(
                    'new peer did not join shard in time'));
            }, TIMEOUT).unref();
        }
    ], arg: {}}, function (err, results) {
        if (err) {
            LOG.error({err: err, results: results});
            t.fail(err);
        }
        t.done();
    });
};

exports.syncAsyncInstantaneousDeath = function (t) {
    vasync.pipeline({funcs: [
        function loadAndVerifyTopology(_, _cb) {
            getTopology(function (err, topology) {
                _.topology = topology;
                _.primaryPgUrl = _.topology.primary.pgUrl;
                _.syncPgUrl = _.topology.sync.pgUrl;
                _.asyncPgUrl = _.topology.async.pgUrl;
                return _cb(err);
            });
        },
        function killSyncAndSync(_, _cb) {
            // hacky way to pick either sync or primary first
            var seed = Math.round(Math.random());
            var barrier = vasync.barrier();
            barrier.on('drain', _cb);
            barrier.start(0);
            barrier.start(1);
            if (seed < 1) {
                MANATEES[_.syncPgUrl].kill(function () {
                    barrier.done(0);
                });
                MANATEES[_.asyncPgUrl].kill(function () {
                    barrier.done(1);
                });
            } else {
                MANATEES[_.asyncPgUrl].kill(function () {
                    barrier.done(1);
                });
                MANATEES[_.syncPgUrl].kill(function () {
                    barrier.done(0);
                });
            }
        },
        function getNewTopology(_, _cb) {
            _cb = once(_cb);
            var topology;
            var intervalId = setInterval(function () {vasync.pipeline({funcs: [
                function _loadTopology(_2, _cb2) {
                    manatee_common.loadTopology(ZK_CLIENT, function (err, top) {
                        if (err) {
                            return _cb2(err);
                        }
                        topology = top[SHARD_ID];
                        try {
                            assert.ok(topology, 'topology DNE');
                            assert.ok(topology.primary, 'primary DNE');
                            assert.equal(topology.sync, null, 'sync exists');
                            assert.equal(topology.async, null, 'async exists');

                            return _cb2();
                        } catch (e) {
                            LOG.info({err: e, topology: topology},
                                     'got unexpected topology');
                            return _cb2(e);
                        }
                   });
                }
            ], arg: {}}, function (err, results) {
                if (err) {
                    LOG.info({err: err, results: results, topology: topology},
                             'still waiting for correct shard state');
                    return;
                } else {
                    clearInterval(intervalId);
                    return _cb();
                }

            }); }, 3000);

            setTimeout(function () {
                clearInterval(intervalId);
                return _cb(new verror.VError('shard did not flip in time'));
            }, TIMEOUT).unref();
        },
        function restartManatees(_, _cb) {
            _cb = once(_cb);
            // hacky way to pick either sync or primary first
            var seed = Math.round(Math.random());
            var barrier = vasync.barrier();
            barrier.on('drain', _cb);
            barrier.start(0);
            barrier.start(1);
            if (seed < 1) {
                MANATEES[_.syncPgUrl].start(function (err) {
                    if (err) {
                        return _cb(err);
                    }
                    barrier.done(0);
                });
                MANATEES[_.asyncPgUrl].start(function (err) {
                    if (err) {
                        return _cb(err);
                    }
                    barrier.done(1);
                });
            } else {
                MANATEES[_.asyncPgUrl].start(function (err) {
                    if (err) {
                        return _cb(err);
                    }
                    barrier.done(1);
                });
                MANATEES[_.syncPgUrl].start(function (err) {
                    if (err) {
                        return _cb(err);
                    }
                    barrier.done(0);
                });
            }
        },
        function checkTopology2(_, _cb) {
            _cb = once(_cb);
            var intervalId = setInterval(function () {vasync.pipeline({funcs: [
                function _getTopology(_2, _cb2) {
                    manatee_common.loadTopology(ZK_CLIENT,
                                                function (err, topology) {
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
                        manatee_common.pgStatus([_2.topology], _cb2);
                    } catch (e) {
                        LOG.info({err: e, topology: _2.topology});
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
                        LOG.info({err: e, topology: _2.topology},
                                 'unable to verify topology');
                        return _cb2(e);
                    }
                },
                function _verifySyncState(_2, _cb2) {
                    try {
                        var p = fs.readFileSync(MANATEES[_2.
                                                topology.primary.pgUrl].
                                                cookieLocation, 'utf8');
                        var s = fs.readFileSync(MANATEES[_2.topology.sync.
                                                pgUrl].cookieLocation, 'utf8');
                        var a = fs.readFileSync(MANATEES[_2.topology.async.
                                                pgUrl].cookieLocation, 'utf8');

                        assert.equal('primary', JSON.parse(p).role);
                        assert.equal('sync', JSON.parse(s).role);
                        assert.equal('async', JSON.parse(a).role);
                        return _cb2();
                    } catch (e) {
                        LOG.warn({err: e, topology: _2.topology},
                                 'unable to verify sync state');
                        return _cb2(e);
                    }
                }
            ], arg: {}}, function (err, results) {
                if (err) {
                    LOG.info({err: err, results: results},
                             'topology not correct');
                    return;
                }
                clearInterval(intervalId);
                return _cb();
            }); }, 3000);

            setTimeout(function () {
                clearInterval(intervalId);
                return _cb(new verror.VError(
                    'new peer did not join shard in time'));
            }, TIMEOUT).unref();
        }
    ], arg: {}}, function (err, results) {
        if (err) {
            LOG.error({err: err, results: results});
            t.fail(err);
        }
        t.done();
    });
};

exports.primaryDeathThenSyncDeath = function (t) {
    vasync.pipeline({funcs: [
        function loadAndVerifyTopology(_, _cb) {
            getTopology(function (err, topology) {
                _.topology = topology;
                _.primaryPgUrl = _.topology.primary.pgUrl;
                _.syncPgUrl = _.topology.sync.pgUrl;
                _.asyncPgUrl = _.topology.async.pgUrl;
                return _cb(err);
            });
        },
        function killPrimary(_, _cb) {
            MANATEES[_.primaryPgUrl].kill(_cb);
        },
        function getNewTopology(_, _cb) {
            _cb = once(_cb);
            var topology;
            var intervalId = setInterval(function () {vasync.pipeline({funcs: [
                function _loadTopology(_2, _cb2) {
                    manatee_common.loadTopology(ZK_CLIENT, function (err, top) {
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
                                             'primary death');

                            LOG.info({topology: topology}, 'got topology');
                            return _cb2();
                        } catch (e) {
                            LOG.info({err: e, topology: topology},
                                     'got unexpected topology');
                            return _cb2(e);
                        }
                   });
                },
                function _getPgStatus(_2, _cb2) {
                    try {
                        manatee_common.pgStatus([topology], _cb2);
                    } catch (e) {
                        LOG.info({err: e, topology: topology},
                                 'could not get pg status');
                        return _cb2(e);
                    }
                },
                function _verifyTopology(_2, _cb2) {
                    _2.topology = topology;
                    try {
                        /*
                         * here we only have to check the sync states of each
                         * of the nodes.  if the sync states are correct, then
                         * we know replication is working.
                         */
                        assert.ok(topology, 'shard topology DNE');
                        assert.ok(topology.primary, 'primary DNE');
                        assert.ok(topology.primary.repl, 'no sync repl state');
                        assert.equal(topology.primary.repl.sync_state,
                                'sync',
                                'replication type not synchronous');
                        assert.ok(topology.sync, 'sync DNE');
                        assert.equal(Object.keys(topology.sync.repl).length, 0,
                                'sync should not have replication state.');
                        assert.equal(topology.async, null, 'async exists');
                        return _cb2();
                    } catch (e) {
                        LOG.info({err: e, topology: topology},
                                 'unable to verify topology');
                        return _cb2(e);
                    }
                },
                function _verifySyncState(_2, _cb2) {
                    try {
                        var p = fs.readFileSync(MANATEES[_2.
                                                topology.primary.pgUrl].
                                                cookieLocation, 'utf8');
                        var s = fs.readFileSync(MANATEES[_2.topology.sync.
                                                pgUrl].cookieLocation, 'utf8');

                        assert.equal('primary', JSON.parse(p).role);
                        assert.equal('sync', JSON.parse(s).role);
                        return _cb2();
                    } catch (e) {
                        LOG.warn({err: e, topology: _2.topology},
                                 'unable to verify sync state');
                        return _cb2(e);
                    }
                }
            ], arg: {}}, function (err, results) {
                if (err) {
                    LOG.info({err: err, results: results, topology: topology},
                             'still waiting for correct shard state');
                    return;
                } else {
                    clearInterval(intervalId);
                    return _cb();
                }

            }); }, 3000);

            setTimeout(function () {
                clearInterval(intervalId);
                return _cb(new verror.VError('shard did not flip in time'));
            }, TIMEOUT).unref();
        },
        function killNewPrimary(_, _cb) {
            MANATEES[_.syncPgUrl].kill(_cb);
        },
        function getNewTopology2(_, _cb) {
            _cb = once(_cb);
            var topology;
            var intervalId = setInterval(function () {vasync.pipeline({funcs: [
                function _loadTopology(_2, _cb2) {
                    manatee_common.loadTopology(ZK_CLIENT, function (err, top) {
                        if (err) {
                            return _cb2(err);
                        }
                        topology = top[SHARD_ID];
                        try {
                            assert.ok(topology, 'topology DNE');
                            assert.ok(topology.primary, 'primary DNE');
                            assert.equal(topology.sync, null, 'sync exists');
                            assert.equal(topology.async, null, 'async exists');

                            return _cb2();
                        } catch (e) {
                            LOG.info({err: e, topology: topology},
                                     'got unexpected topology');
                            return _cb2(e);
                        }
                   });
                }
            ], arg: {}}, function (err, results) {
                if (err) {
                    LOG.info({err: err, results: results, topology: topology},
                             'still waiting for correct shard state');
                    return;
                } else {
                    clearInterval(intervalId);
                    return _cb();
                }

            }); }, 3000);

            setTimeout(function () {
                clearInterval(intervalId);
                return _cb(new verror.VError('shard did not flip in time'));
            }, TIMEOUT).unref();
        },
        function restartManatees(_, _cb) {
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
            var intervalId = setInterval(function () {vasync.pipeline({funcs: [
                function _getTopology(_2, _cb2) {
                    manatee_common.loadTopology(ZK_CLIENT,
                                                function (err, topology) {
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
                        manatee_common.pgStatus([_2.topology], _cb2);
                    } catch (e) {
                        LOG.info({err: e, topology: _2.topology});
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
                        assert.ok(_2.topology.primary.repl,
                                  'no sync repl state');
                        assert.equal(_2.topology.primary.repl.sync_state,
                                     'sync', 'no sync replication state.');
                        assert.ok(_2.topology.sync, 'sync DNE');
                        assert.equal(_2.topology.sync.repl.sync_state,
                                     'async', 'no async replication state');
                        assert.ok(_2.topology.async, 'async DNE');
                        assert.notEqual(_2.topology.primary.pgUrl,
                                        _2.primaryPgUrl,
                                        'primary should not be killed primary');
                        return _cb2();
                    } catch (e) {
                        LOG.info({err: e, topology: _2.topology});
                        return _cb2(e);
                    }
                },
                function _verifySyncState(_2, _cb2) {
                    try {
                        var p = fs.readFileSync(MANATEES[_2.
                                                topology.primary.pgUrl].
                                                cookieLocation, 'utf8');
                        var s = fs.readFileSync(MANATEES[_2.topology.sync.
                                                pgUrl].cookieLocation, 'utf8');
                        var a = fs.readFileSync(MANATEES[_2.topology.async.
                                                pgUrl].cookieLocation, 'utf8');

                        assert.equal('primary', JSON.parse(p).role);
                        assert.equal('sync', JSON.parse(s).role);
                        assert.equal('async', JSON.parse(a).role);
                        return _cb2();
                    } catch (e) {
                        LOG.warn({err: e, topology: _2.topology},
                                 'unable to verify sync state');
                        return _cb2(e);
                    }
                }
            ], arg: {}}, function (err, results) {
                if (err) {
                    LOG.info({err: err, results: results},
                             'topology not correct');
                    return;
                }
                clearInterval(intervalId);
                return _cb();
            }); }, 3000);

            setTimeout(function () {
                clearInterval(intervalId);
                return _cb(new verror.VError(
                    'new peer did not join shard in time'));
            }, TIMEOUT).unref();
        }
    ], arg: {}}, function (err, results) {
        if (err) {
            LOG.error({err: err, results: results});
            t.fail(err);
        }
        t.done();
    });
};

exports.primaryDeathThenAsyncDeath = function (t) {
    vasync.pipeline({funcs: [
        function loadAndVerifyTopology(_, _cb) {
            getTopology(function (err, topology) {
                _.topology = topology;
                _.primaryPgUrl = _.topology.primary.pgUrl;
                _.syncPgUrl = _.topology.sync.pgUrl;
                _.asyncPgUrl = _.topology.async.pgUrl;
                return _cb(err);
            });
        },
        function killPrimary(_, _cb) {
            MANATEES[_.primaryPgUrl].kill(_cb);
        },
        function getNewTopology(_, _cb) {
            _cb = once(_cb);
            var topology;
            var intervalId = setInterval(function () {vasync.pipeline({funcs: [
                function _loadTopology(_2, _cb2) {
                    manatee_common.loadTopology(ZK_CLIENT, function (err, top) {
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
                                             'primary death');

                            LOG.info({topology: topology}, 'got topology');
                            return _cb2();
                        } catch (e) {
                            LOG.info({err: e, topology: topology},
                                     'got unexpected topology');
                            return _cb2(e);
                        }
                   });
                },
                function _getPgStatus(_2, _cb2) {
                    try {
                        manatee_common.pgStatus([topology], _cb2);
                    } catch (e) {
                        LOG.info({err: e, topology: topology},
                                 'could not get pg status');
                        return _cb2(e);
                    }
                },
                function _verifyTopology(_2, _cb2) {
                    _2.topology = topology;
                    try {
                        /*
                         * here we only have to check the sync states of each
                         * of the nodes.  if the sync states are correct, then
                         * we know replication is working.
                         */
                        assert.ok(topology, 'shard topology DNE');
                        assert.ok(topology.primary, 'primary DNE');
                        assert.ok(topology.primary.repl, 'no sync repl state');
                        assert.equal(topology.primary.repl.sync_state,
                                'sync',
                                'replication type not synchronous');
                        assert.ok(topology.sync, 'sync DNE');
                        assert.equal(Object.keys(topology.sync.repl).length, 0,
                                'sync should not have replication state.');
                        assert.equal(topology.async, null, 'async exists');
                        return _cb2();
                    } catch (e) {
                        LOG.info({err: e, topology: topology},
                                 'unable to verify topology');
                        return _cb2(e);
                    }
                },
                function _verifySyncState(_2, _cb2) {
                    try {
                        var p = fs.readFileSync(MANATEES[_2.
                                                topology.primary.pgUrl].
                                                cookieLocation, 'utf8');
                        var s = fs.readFileSync(MANATEES[_2.topology.sync.
                                                pgUrl].cookieLocation, 'utf8');

                        assert.equal('primary', JSON.parse(p).role);
                        assert.equal('sync', JSON.parse(s).role);
                        return _cb2();
                    } catch (e) {
                        LOG.warn({err: e, topology: _2.topology},
                                 'unable to verify sync state');
                        return _cb2(e);
                    }
                }
            ], arg: {}}, function (err, results) {
                if (err) {
                    LOG.info({err: err, results: results, topology: topology},
                             'still waiting for correct shard state');
                    return;
                } else {
                    clearInterval(intervalId);
                    return _cb();
                }

            }); }, 3000);

            setTimeout(function () {
                clearInterval(intervalId);
                return _cb(new verror.VError('shard did not flip in time'));
            }, TIMEOUT).unref();
        },
        function killAsync(_, _cb) {
            MANATEES[_.asyncPgUrl].kill(_cb);
        },
        function getNewTopology2(_, _cb) {
            _cb = once(_cb);
            var topology;
            var intervalId = setInterval(function () {vasync.pipeline({funcs: [
                function _loadTopology(_2, _cb2) {
                    manatee_common.loadTopology(ZK_CLIENT, function (err, top) {
                        if (err) {
                            return _cb2(err);
                        }
                        topology = top[SHARD_ID];
                        try {
                            assert.ok(topology, 'topology DNE');
                            assert.ok(topology.primary, 'primary DNE');
                            assert.equal(topology.sync, null, 'sync exists');
                            assert.equal(topology.async, null, 'async exists');

                            return _cb2();
                        } catch (e) {
                            LOG.info({err: e, topology: topology},
                                     'got unexpected topology');
                            return _cb2(e);
                        }
                   });
                }
            ], arg: {}}, function (err, results) {
                if (err) {
                    LOG.info({err: err, results: results, topology: topology},
                             'still waiting for correct shard state');
                    return;
                } else {
                    clearInterval(intervalId);
                    return _cb();
                }

            }); }, 3000);

            setTimeout(function () {
                clearInterval(intervalId);
                return _cb(new verror.VError('shard did not flip in time'));
            }, TIMEOUT).unref();
        },
        function restartManatees(_, _cb) {
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
                MANATEES[_.asyncPgUrl].start(function (err) {
                    if (err) {
                        return _cb(err);
                    }
                    barrier.done(1);
                });
            } else {
                MANATEES[_.asyncPgUrl].start(function (err) {
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
            var intervalId = setInterval(function () {vasync.pipeline({funcs: [
                function _getTopology(_2, _cb2) {
                    manatee_common.loadTopology(ZK_CLIENT,
                                                function (err, topology) {
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
                        manatee_common.pgStatus([_2.topology], _cb2);
                    } catch (e) {
                        LOG.info({err: e, topology: _2.topology});
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
                        assert.ok(_2.topology.primary.repl,
                                  'no sync repl state');
                        assert.equal(_2.topology.primary.repl.sync_state,
                                     'sync', 'no sync replication state.');
                        assert.ok(_2.topology.sync, 'sync DNE');
                        assert.equal(_2.topology.sync.repl.sync_state,
                                     'async', 'no async replication state');
                        assert.ok(_2.topology.async, 'async DNE');
                        assert.notEqual(_2.topology.primary.pgUrl,
                                        _2.primaryPgUrl,
                                        'primary should not be killed primary');
                        return _cb2();
                    } catch (e) {
                        LOG.info({err: e, topology: _2.topology});
                        return _cb2(e);
                    }
                },
                function _verifySyncState(_2, _cb2) {
                    try {
                        var p = fs.readFileSync(MANATEES[_2.
                                                topology.primary.pgUrl].
                                                cookieLocation, 'utf8');
                        var s = fs.readFileSync(MANATEES[_2.topology.sync.
                                                pgUrl].cookieLocation, 'utf8');
                        var a = fs.readFileSync(MANATEES[_2.topology.async.
                                                pgUrl].cookieLocation, 'utf8');

                        assert.equal('primary', JSON.parse(p).role);
                        assert.equal('sync', JSON.parse(s).role);
                        assert.equal('async', JSON.parse(a).role);
                        return _cb2();
                    } catch (e) {
                        LOG.warn({err: e, topology: _2.topology},
                                 'unable to verify sync state');
                        return _cb2(e);
                    }
                }
            ], arg: {}}, function (err, results) {
                if (err) {
                    LOG.info({err: err, results: results},
                             'topology not correct');
                    return;
                }
                clearInterval(intervalId);
                return _cb();
            }); }, 3000);

            setTimeout(function () {
                clearInterval(intervalId);
                return _cb(new verror.VError(
                    'new peer did not join shard in time'));
            }, TIMEOUT).unref();
        }
    ], arg: {}}, function (err, results) {
        if (err) {
            LOG.error({err: err, results: results});
            t.fail(err);
        }
        t.done();
    });
};

exports.MANATEE_212_killSyncBeforeReplOnPrimary = function (t) {
    var topo;
    vasync.pipeline({funcs: [
        function loadAndVerifyTopology(_, _cb) {
            getTopology(function (err, topology) {
                _.topology = topology;
                _.primaryPgUrl = _.topology.primary.pgUrl;
                _.syncPgUrl = _.topology.sync.pgUrl;
                _.asyncPgUrl = _.topology.async.pgUrl;
                return _cb(err);
            });
        },
        function addSimulatedAsync(_, _cb) {
            // we can't really prevent a peer joining and slaving before it
            // leaves.  However, we can simulate a slave joining as the async by
            // just joining the underlying ZK election.
            _cb = once(_cb);
            _.zkClient = zk.createClient(ZK_URL);
            _.zkClient.on('error', function (err) {
                if (!_cb.called()) {
                    return _cb(err);
                }
            });
            _.zkClient.on('connected', function () {
                _.election = leader.createElection({
                    zk: _.zkClient,
                    path: SHARD_PATH + '/election',
                    pathPrefix: '0.0.0.0:45677:45678',
                    log: LOG
                }, function () {});
                _.election.vote(undefined, function () {
                    return _cb();
                });
                _.election.on('error', function (err) {
                    if (!_cb.called()) {
                        return _cb(err);
                    }
                });
            });

            _.zkClient.connect();

        },
        function killAsync(_, _cb) {
            MANATEES[_.asyncPgUrl].kill(_cb);
        },
        function killPrimary(_, _cb) {
            MANATEES[_.primaryPgUrl].kill(_cb);
        },
        function addAsync(_, _cb) {
            MANATEES[_.asyncPgUrl].start(_cb);
        },
        function removeSimulatedSync(_, _cb) {
            setTimeout(function () {
                _.election.leave();
                _.zkClient.close();
                return _cb();
            }, 20000);
        },
        function verifyAsyncIsSync(_, _cb) {
            _cb = once(_cb);
            var intervalId = setInterval(function () {vasync.pipeline({funcs: [
                function _getTopology(_2, _cb2) {
                    manatee_common.loadTopology(ZK_CLIENT,
                                                function (err, topology) {
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
                        manatee_common.pgStatus([_2.topology], _cb2);
                    } catch (e) {
                        LOG.info({err: e, topology: _2.topology});
                        return _cb2(e);
                    }
                },
                function _verifyTopology(_2, _cb2) {
                    _cb2 = once(_cb2);
                    topo = _2.topology;
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
                         assert.equal(_2.topology.async, null,
                                         'async exists');
                        return _cb2();
                    } catch (e) {
                        LOG.warn({err: e, topology: _2.topology},
                                 'unable to verify topology');
                        return _cb2(e);
                    }
                },
                function _verifySyncState(_2, _cb2) {
                    try {
                        var p = fs.readFileSync(MANATEES[_2.
                                                topology.primary.pgUrl].
                                                cookieLocation, 'utf8');
                        var s = fs.readFileSync(MANATEES[_2.topology.sync.
                                                pgUrl].cookieLocation, 'utf8');

                        assert.equal('primary', JSON.parse(p).role);
                        assert.equal('sync', JSON.parse(s).role);
                        return _cb2();
                    } catch (e) {
                        LOG.warn({err: e, topology: _2.topology},
                                 'unable to verify sync state');
                        return _cb2(e);
                    }
                }
            ], arg: {}}, function (err, results) {
                if (err) {
                    LOG.info({err: err, results: results},
                             'topology not correct');
                    return;
                }
                clearInterval(intervalId);
                return _cb();
            }); }, 3000);

            setTimeout(function () {
                clearInterval(intervalId);
                if (!_cb.called) {
                    LOG.fatal({topology: topo},
                              'new peer did not join shard in time');
                }
                return _cb(new verror.VError(
                    'new peer did not join shard in time'));
            }, TIMEOUT).unref();
        },
        function addOldPrimary(_, _cb) {
            MANATEES[_.primaryPgUrl].start(_cb);
        },
        function checkTopology(_, _cb) {
            _cb = once(_cb);
            var intervalId = setInterval(function () {vasync.pipeline({funcs: [
                function _getTopology(_2, _cb2) {
                    manatee_common.loadTopology(ZK_CLIENT,
                                                function (err, topology) {
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
                        manatee_common.pgStatus([_2.topology], _cb2);
                    } catch (e) {
                        LOG.info({err: e, topology: _2.topology});
                        return _cb2(e);
                    }
                },
                function _verifyTopology(_2, _cb2) {
                    _cb2 = once(_cb2);
                    topo = _2.topology;
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
                        LOG.info({err: e, topology: _2.topology},
                                 'unable to verify topology');
                        return _cb2(e);
                    }
                },
                function _verifySyncState(_2, _cb2) {
                    try {
                        var p = fs.readFileSync(MANATEES[_2.
                                                topology.primary.pgUrl].
                                                cookieLocation, 'utf8');
                        var s = fs.readFileSync(MANATEES[_2.topology.sync.
                                                pgUrl].cookieLocation, 'utf8');
                        var a = fs.readFileSync(MANATEES[_2.topology.async.
                                                pgUrl].cookieLocation, 'utf8');

                        assert.equal('primary', JSON.parse(p).role);
                        assert.equal('sync', JSON.parse(s).role);
                        assert.equal('async', JSON.parse(a).role);
                        return _cb2();
                    } catch (e) {
                        LOG.warn({err: e, topology: _2.topology},
                                 'unable to verify sync state');
                        return _cb2(e);
                    }
                }
            ], arg: {}}, function (err, results) {
                if (err) {
                    LOG.info({err: err, results: results},
                             'topology not correct');
                    return;
                }
                clearInterval(intervalId);
                return _cb();
            }); }, 3000);

            setTimeout(function () {
                clearInterval(intervalId);
                if (!_cb.called) {
                    LOG.fatal({topology: topo},
                              'new peer did not join shard in time');
                }
                return _cb(new verror.VError(
                    'new peer did not join shard in time'));
            }, TIMEOUT).unref();
        }
    ], arg: {}}, function (err, results) {
        if (err) {
            LOG.error({err: err, results: results});
            t.fail(err);
        }
        t.done();
    });
};

exports.MANATEE_212_killSyncBeforeRepl = function (t) {
    var topo;
    vasync.pipeline({funcs: [
        function loadAndVerifyTopology(_, _cb) {
            getTopology(function (err, topology) {
                _.topology = topology;
                _.primaryPgUrl = _.topology.primary.pgUrl;
                _.syncPgUrl = _.topology.sync.pgUrl;
                _.asyncPgUrl = _.topology.async.pgUrl;
                return _cb(err);
            });
        },
        function killAsyncSync(_, _cb) {
            vasync.parallel({funcs: [
                function async(_cb2) {
                    MANATEES[_.asyncPgUrl].kill(_cb2);
                },
                function sync(_cb2) {
                    MANATEES[_.syncPgUrl].kill(_cb2);
                }
            ], arg: {}}, function (err, results) {
                return _cb(err);
            });
        },
        function verifyPrimaryIsAlone(_, _cb) {
            _cb = once(_cb);
            var intervalId = setInterval(function () {vasync.pipeline({funcs: [
                function _getTopology(_2, _cb2) {
                    manatee_common.loadTopology(ZK_CLIENT,
                                                function (err, topology) {
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
                        manatee_common.pgStatus([_2.topology], _cb2);
                    } catch (e) {
                        LOG.info({err: e, topology: _2.topology});
                        return _cb2(e);
                    }
                },
                function _verifyTopology(_2, _cb2) {
                    _cb2 = once(_cb2);
                    topo = _2.topology;
                    try {
                         assert.ok(_2.topology, 'shard topology DNE');
                         assert.ok(_2.topology.primary, 'primary DNE');
                         assert.notEqual(_2.topology.sync, null, 'sync exists');
                         assert.notEqual(_2.topology.async, null,
                                         'async exists');
                        return _cb2();
                    } catch (e) {
                        LOG.info({err: e, topology: _2.topology},
                                 'unable to verify topology');
                        return _cb2(e);
                    }
                }
            ], arg: {}}, function (err, results) {
                if (err) {
                    LOG.info({err: err, results: results},
                             'topology not correct');
                    return;
                }
                clearInterval(intervalId);
                return _cb();
            }); }, 3000);

            setTimeout(function () {
                clearInterval(intervalId);
                if (!_cb.called) {
                    LOG.fatal({topology: topo}, 'primary is not alone');
                }
                return _cb(new verror.VError('primary is not alone'));
            }, TIMEOUT).unref();
        },
        function addSync(_, _cb) {
            // we can't really prevent a sync joining and slaving before it
            // leaves.  However, we can simulate a slave joining as the sync by
            // just joining the underlying ZK election.
            _cb = once(_cb);
            _.zkClient = zk.createClient(ZK_URL);
            _.zkClient.on('error', function (err) {
                if (!_cb.called()) {
                    return _cb(err);
                }
            });
            _.zkClient.on('connected', function () {
                _.election = leader.createElection({
                    zk: _.zkClient,
                    path: SHARD_PATH + '/election',
                    pathPrefix: '0.0.0.0:45677:45678',
                    log: LOG
                }, function () {});
                _.election.vote(undefined, function () {
                    return _cb();
                });
                _.election.on('error', function (err) {
                    if (!_cb.called()) {
                        return _cb(err);
                    }
                });
            });

            _.zkClient.connect();
        },
        function addAsync(_, _cb) {
            MANATEES[_.asyncPgUrl].start(_cb);
        },
        function removeSimulatedSync(_, _cb) {
            setTimeout(function () {
                _.election.leave();
                _.zkClient.close();
                return _cb();
            }, 10000);
        },
        function verifyAsyncIsSync(_, _cb) {
            _cb = once(_cb);
            var intervalId = setInterval(function () {vasync.pipeline({funcs: [
                function _getTopology(_2, _cb2) {
                    manatee_common.loadTopology(ZK_CLIENT,
                                                function (err, topology) {
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
                        manatee_common.pgStatus([_2.topology], _cb2);
                    } catch (e) {
                        LOG.info({err: e, topology: _2.topology});
                        return _cb2(e);
                    }
                },
                function _verifyTopology(_2, _cb2) {
                    _cb2 = once(_cb2);
                    topo = _2.topology;
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
                         assert.equal(_2.topology.async, null,
                                         'async exists');
                        return _cb2();
                    } catch (e) {
                        LOG.warn({err: e, topology: _2.topology},
                                 'unable to verify topology');
                        return _cb2(e);
                    }
                },
                function _verifySyncState(_2, _cb2) {
                    try {
                        var p = fs.readFileSync(MANATEES[_2.
                                                topology.primary.pgUrl].
                                                cookieLocation, 'utf8');
                        var s = fs.readFileSync(MANATEES[_2.topology.sync.
                                                pgUrl].cookieLocation, 'utf8');

                        assert.equal('primary', JSON.parse(p).role);
                        assert.equal('sync', JSON.parse(s).role);
                        return _cb2();
                    } catch (e) {
                        LOG.warn({err: e, topology: _2.topology},
                                 'unable to verify sync state');
                        return _cb2(e);
                    }
                }
            ], arg: {}}, function (err, results) {
                if (err) {
                    LOG.info({err: err, results: results},
                             'topology not correct');
                    return;
                }
                clearInterval(intervalId);
                return _cb();
            }); }, 3000);

            setTimeout(function () {
                clearInterval(intervalId);
                if (!_cb.called) {
                    LOG.fatal({topology: topo},
                              'new peer did not join shard in time');
                }
                return _cb(new verror.VError(
                    'new peer did not join shard in time'));
            }, TIMEOUT).unref();
        },
        function addOldSync(_, _cb) {
            MANATEES[_.syncPgUrl].start(_cb);
        },
        function checkTopology(_, _cb) {
            _cb = once(_cb);
            var intervalId = setInterval(function () {vasync.pipeline({funcs: [
                function _getTopology(_2, _cb2) {
                    manatee_common.loadTopology(ZK_CLIENT,
                                                function (err, topology) {
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
                        manatee_common.pgStatus([_2.topology], _cb2);
                    } catch (e) {
                        LOG.info({err: e, topology: _2.topology});
                        return _cb2(e);
                    }
                },
                function _verifyTopology(_2, _cb2) {
                    _cb2 = once(_cb2);
                    topo = _2.topology;
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
                        LOG.info({err: e, topology: _2.topology},
                                 'unable to verify topology');
                        return _cb2(e);
                    }
                },
                function _verifySyncState(_2, _cb2) {
                    try {
                        var p = fs.readFileSync(MANATEES[_2.
                                                topology.primary.pgUrl].
                                                cookieLocation, 'utf8');
                        var s = fs.readFileSync(MANATEES[_2.topology.sync.
                                                pgUrl].cookieLocation, 'utf8');
                        var a = fs.readFileSync(MANATEES[_2.topology.async.
                                                pgUrl].cookieLocation, 'utf8');

                        assert.equal('primary', JSON.parse(p).role);
                        assert.equal('sync', JSON.parse(s).role);
                        assert.equal('async', JSON.parse(a).role);
                        return _cb2();
                    } catch (e) {
                        LOG.warn({err: e, topology: _2.topology},
                                 'unable to verify sync state');
                        return _cb2(e);
                    }
                }
            ], arg: {}}, function (err, results) {
                if (err) {
                    LOG.info({err: err, results: results},
                             'topology not correct');
                    return;
                }
                clearInterval(intervalId);
                return _cb();
            }); }, 3000);

            setTimeout(function () {
                clearInterval(intervalId);
                if (!_cb.called) {
                    LOG.fatal({topology: topo},
                              'new peer did not join shard in time');
                }
                return _cb(new verror.VError(
                    'new peer did not join shard in time'));
            }, TIMEOUT).unref();
        }
    ], arg: {}}, function (err, results) {
        if (err) {
            LOG.error({err: err, results: results});
            t.fail(err);
        }
        t.done();
    });
};

exports.MANATEE_206_delete_prev_emphemeral_nodes = function (t) {
    vasync.pipeline({funcs: [
        function loadAndVerifyTopology(_, _cb) {
            getTopology(function (err, topology) {
                _.topology = topology;
                _.primaryPgUrl = _.topology.primary.pgUrl;
                _.syncPgUrl = _.topology.sync.pgUrl;
                _.asyncPgUrl = _.topology.async.pgUrl;
                return _cb(err);
            });
        },
        function killAsync(_, _cb) {
            MANATEES[_.asyncPgUrl].kill(_cb);
        },
        function restartAsync(_, _cb) {
            _cb = once(_cb);
            MANATEES[_.asyncPgUrl].start(function (err) {
                return _cb(err);
            });
        },
        function checkTopology2(_, _cb) {
            _cb = once(_cb);
            var intervalId = setInterval(function () {vasync.pipeline({funcs: [
                function _getTopology(_2, _cb2) {
                    manatee_common.loadTopology(ZK_CLIENT,
                                                function (err, topology) {
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
                        manatee_common.pgStatus([_2.topology], _cb2);
                    } catch (e) {
                        LOG.info({err: e, topology: _2.topology});
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
                        assert.ok(_2.topology.primary.repl,
                                  'no sync repl state');
                        assert.equal(_2.topology.primary.repl.sync_state,
                                     'sync', 'no sync replication state.');
                        assert.ok(_2.topology.sync, 'sync DNE');
                        assert.equal(_2.topology.sync.repl.sync_state,
                                     'async', 'no async replication state');
                        assert.ok(_2.topology.async, 'async DNE');
                        assert.notEqual(_2.topology.primary.pgUrl,
                                        _2.primaryPgUrl,
                                        'primary should not be killed primary');
                        return _cb2();
                    } catch (e) {
                        LOG.info({err: e, topology: _2.topology});
                        return _cb2(e);
                    }
                },
                function _verifySyncState(_2, _cb2) {
                    try {
                        var p = fs.readFileSync(MANATEES[_2.
                                                topology.primary.pgUrl].
                                                cookieLocation, 'utf8');
                        var s = fs.readFileSync(MANATEES[_2.topology.sync.
                                                pgUrl].cookieLocation, 'utf8');
                        var a = fs.readFileSync(MANATEES[_2.topology.async.
                                                pgUrl].cookieLocation, 'utf8');

                        assert.equal('primary', JSON.parse(p).role);
                        assert.equal('sync', JSON.parse(s).role);
                        assert.equal('async', JSON.parse(a).role);
                        return _cb2();
                    } catch (e) {
                        LOG.warn({err: e, topology: _2.topology},
                                 'unable to verify sync state');
                        return _cb2(e);
                    }
                }
            ], arg: {}}, function (err, results) {
                if (err) {
                    LOG.info({err: err, results: results},
                             'topology not correct');
                    return;
                }
                clearInterval(intervalId);
                return _cb();
            }); }, 3000);

            setTimeout(function () {
                clearInterval(intervalId);
                return _cb(new verror.VError(
                    'new peer did not join shard in time'));
            }, TIMEOUT).unref();
        }
    ], arg: {}}, function (err, results) {
        if (err) {
            LOG.error({err: err, results: results});
            t.fail(err);
        }
        t.done();
    });
};

exports.MANATEE_207_killASNoWait = function (t) {
    var topo;
    vasync.pipeline({funcs: [
        function loadAndVerifyTopology(_, _cb) {
            getTopology(function (err, topology) {
                _.topology = topology;
                _.primaryPgUrl = _.topology.primary.pgUrl;
                _.syncPgUrl = _.topology.sync.pgUrl;
                _.asyncPgUrl = _.topology.async.pgUrl;
                return _cb(err);
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
                MANATEES[_.asyncPgUrl].kill(function () {
                    barrier.done(0);
                });
                MANATEES[_.syncPgUrl].kill(function () {
                    barrier.done(1);
                });
            } else {
                MANATEES[_.syncPgUrl].kill(function () {
                    barrier.done(1);
                });
                MANATEES[_.asyncPgUrl].kill(function () {
                    barrier.done(0);
                });
            }
        },
        // do not wait and restart right away -- this causes MANATEE-207
        function restartManatees(_, _cb) {
            _cb = once(_cb);
            // hacky way to pick either sync or primary first
            var seed = Math.round(Math.random());
            var barrier = vasync.barrier();
            barrier.on('drain', _cb);
            barrier.start(0);
            barrier.start(1);
            if (seed < 1) {
                MANATEES[_.asyncPgUrl].start(function (err) {
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
                MANATEES[_.asyncPgUrl].start(function (err) {
                    if (err) {
                        return _cb(err);
                    }
                    barrier.done(0);
                });
            }
        },
        function checkTopology2(_, _cb) {
            _cb = once(_cb);
            var intervalId = setInterval(function () {vasync.pipeline({funcs: [
                function _getTopology(_2, _cb2) {
                    manatee_common.loadTopology(ZK_CLIENT,
                                                function (err, topology) {
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
                        manatee_common.pgStatus([_2.topology], _cb2);
                    } catch (e) {
                        LOG.info({err: e, topology: _2.topology});
                        return _cb2(e);
                    }
                },
                function _verifyTopology(_2, _cb2) {
                    _cb2 = once(_cb2);
                    topo = _2.topology;
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
                        LOG.info({err: e, topology: _2.topology},
                                 'unable to verify topology');
                        return _cb2(e);
                    }
                },
                function _verifySyncState(_2, _cb2) {
                    try {
                        var p = fs.readFileSync(MANATEES[_2.
                                                topology.primary.pgUrl].
                                                cookieLocation, 'utf8');
                        var s = fs.readFileSync(MANATEES[_2.topology.sync.
                                                pgUrl].cookieLocation, 'utf8');
                        var a = fs.readFileSync(MANATEES[_2.topology.async.
                                                pgUrl].cookieLocation, 'utf8');

                        assert.equal('primary', JSON.parse(p).role);
                        assert.equal('sync', JSON.parse(s).role);
                        assert.equal('async', JSON.parse(a).role);
                        return _cb2();
                    } catch (e) {
                        LOG.warn({err: e, topology: _2.topology},
                                 'unable to verify sync state');
                        return _cb2(e);
                    }
                }
            ], arg: {}}, function (err, results) {
                if (err) {
                    LOG.info({err: err, results: results},
                             'topology not correct');
                    return;
                }
                clearInterval(intervalId);
                return _cb();
            }); }, 3000);

            setTimeout(function () {
                clearInterval(intervalId);
                if (!_cb.called) {
                    LOG.fatal({topology: topo},
                              'new peer did not join shard in time');
                }
                return _cb(new verror.VError(
                    'new peer did not join shard in time'));
            }, TIMEOUT).unref();
        }
    ], arg: {}}, function (err, results) {
        if (err) {
            LOG.error({err: err, results: results});
            t.fail(err);
        }
        t.done();
    });
};

exports.MANATEE_207_killPANoWait = function (t) {
    var topo;
    vasync.pipeline({funcs: [
        function loadAndVerifyTopology(_, _cb) {
            getTopology(function (err, topology) {
                _.topology = topology;
                _.primaryPgUrl = _.topology.primary.pgUrl;
                _.syncPgUrl = _.topology.sync.pgUrl;
                _.asyncPgUrl = _.topology.async.pgUrl;
                return _cb(err);
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
                MANATEES[_.asyncPgUrl].kill(function () {
                    barrier.done(1);
                });
            } else {
                MANATEES[_.asyncPgUrl].kill(function () {
                    barrier.done(1);
                });
                MANATEES[_.primaryPgUrl].kill(function () {
                    barrier.done(0);
                });
            }
        },
        // do not wait and restart right away -- this causes MANATEE-207
        function restartManatees(_, _cb) {
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
                MANATEES[_.asyncPgUrl].start(function (err) {
                    if (err) {
                        return _cb(err);
                    }
                    barrier.done(1);
                });
            } else {
                MANATEES[_.asyncPgUrl].start(function (err) {
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
            var intervalId = setInterval(function () {vasync.pipeline({funcs: [
                function _getTopology(_2, _cb2) {
                    manatee_common.loadTopology(ZK_CLIENT,
                                                function (err, topology) {
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
                        manatee_common.pgStatus([_2.topology], _cb2);
                    } catch (e) {
                        LOG.info({err: e, topology: _2.topology});
                        return _cb2(e);
                    }
                },
                function _verifyTopology(_2, _cb2) {
                    _cb2 = once(_cb2);
                    topo = _2.topology;
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
                        LOG.info({err: e, topology: _2.topology},
                                 'unable to verify topology');
                        return _cb2(e);
                    }
                },
                function _verifySyncState(_2, _cb2) {
                    try {
                        var p = fs.readFileSync(MANATEES[_2.
                                                topology.primary.pgUrl].
                                                cookieLocation, 'utf8');
                        var s = fs.readFileSync(MANATEES[_2.topology.sync.
                                                pgUrl].cookieLocation, 'utf8');
                        var a = fs.readFileSync(MANATEES[_2.topology.async.
                                                pgUrl].cookieLocation, 'utf8');

                        assert.equal('primary', JSON.parse(p).role);
                        assert.equal('sync', JSON.parse(s).role);
                        assert.equal('async', JSON.parse(a).role);
                        return _cb2();
                    } catch (e) {
                        LOG.warn({err: e, topology: _2.topology},
                                 'unable to verify sync state');
                        return _cb2(e);
                    }
                }
            ], arg: {}}, function (err, results) {
                if (err) {
                    LOG.info({err: err, results: results},
                             'topology not correct');
                    return;
                }
                clearInterval(intervalId);
                return _cb();
            }); }, 3000);

            setTimeout(function () {
                clearInterval(intervalId);
                if (!_cb.called) {
                    LOG.fatal({topology: topo},
                              'new peer did not join shard in time');
                }
                return _cb(new verror.VError(
                    'new peer did not join shard in time'));
            }, TIMEOUT).unref();
        }
    ], arg: {}}, function (err, results) {
        if (err) {
            LOG.error({err: err, results: results});
            t.fail(err);
        }
        t.done();
    });
};

exports.MANATEE_207_killPSNoWait = function (t) {
    var topo;
    vasync.pipeline({funcs: [
        function loadAndVerifyTopology(_, _cb) {
            getTopology(function (err, topology) {
                _.topology = topology;
                _.primaryPgUrl = _.topology.primary.pgUrl;
                _.syncPgUrl = _.topology.sync.pgUrl;
                _.asyncPgUrl = _.topology.async.pgUrl;
                return _cb(err);
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
        // do not wait and restart right away -- this causes MANATEE-207
        function restartManatees(_, _cb) {
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
            var intervalId = setInterval(function () {vasync.pipeline({funcs: [
                function _getTopology(_2, _cb2) {
                    manatee_common.loadTopology(ZK_CLIENT,
                                                function (err, topology) {
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
                        manatee_common.pgStatus([_2.topology], _cb2);
                    } catch (e) {
                        LOG.info({err: e, topology: _2.topology});
                        return _cb2(e);
                    }
                },
                function _verifyTopology(_2, _cb2) {
                    _cb2 = once(_cb2);
                    topo = _2.topology;
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
                        LOG.info({err: e, topology: _2.topology},
                                 'unable to verify topology');
                        return _cb2(e);
                    }
                },
                function _verifySyncState(_2, _cb2) {
                    try {
                        var p = fs.readFileSync(MANATEES[_2.
                                                topology.primary.pgUrl].
                                                cookieLocation, 'utf8');
                        var s = fs.readFileSync(MANATEES[_2.topology.sync.
                                                pgUrl].cookieLocation, 'utf8');
                        var a = fs.readFileSync(MANATEES[_2.topology.async.
                                                pgUrl].cookieLocation, 'utf8');

                        assert.equal('primary', JSON.parse(p).role);
                        assert.equal('sync', JSON.parse(s).role);
                        assert.equal('async', JSON.parse(a).role);
                        return _cb2();
                    } catch (e) {
                        LOG.warn({err: e, topology: _2.topology},
                                 'unable to verify sync state');
                        return _cb2(e);
                    }
                }
            ], arg: {}}, function (err, results) {
                if (err) {
                    LOG.info({err: err, results: results},
                             'topology not correct');
                    return;
                }
                clearInterval(intervalId);
                return _cb();
            }); }, 3000);

            setTimeout(function () {
                clearInterval(intervalId);
                if (!_cb.called) {
                    LOG.fatal({topology: topo},
                              'new peer did not join shard in time');
                }
                return _cb(new verror.VError(
                    'new peer did not join shard in time'));
            }, TIMEOUT).unref();
        }
    ], arg: {}}, function (err, results) {
        if (err) {
            LOG.error({err: err, results: results});
            t.fail(err);
        }
        t.done();
    });
};

exports.MANATEE_207_killAllNoWait = function (t) {
    var topo;
    vasync.pipeline({funcs: [
        function loadAndVerifyTopology(_, _cb) {
            getTopology(function (err, topology) {
                _.topology = topology;
                _.primaryPgUrl = _.topology.primary.pgUrl;
                _.syncPgUrl = _.topology.sync.pgUrl;
                _.asyncPgUrl = _.topology.async.pgUrl;
                return _cb(err);
            });
        },
        function killEveryone(_, _cb) {
            vasync.parallel({funcs: [
                function async(_cb2) {
                    MANATEES[_.asyncPgUrl].kill(_cb2);
                },
                function sync(_cb2) {
                    MANATEES[_.syncPgUrl].kill(_cb2);
                },
                function primary(_cb2) {
                    MANATEES[_.primaryPgUrl].kill(_cb2);
                }
            ], arg: {}}, function (err, results) {
                return _cb(err);
            });
        },
        // do not wait and restart right away -- this causes MANATEE-207
        function restartManatees(_, _cb) {
            vasync.parallel({funcs: [
                function async(_cb2) {
                    MANATEES[_.asyncPgUrl].start(_cb2);
                },
                function sync(_cb2) {
                    MANATEES[_.syncPgUrl].start(_cb2);
                },
                function primary(_cb2) {
                    MANATEES[_.primaryPgUrl].start(_cb2);
                }
            ], arg: {}}, function (err, results) {
                return _cb(err);
            });
        },
        function checkTopology2(_, _cb) {
            _cb = once(_cb);
            var intervalId = setInterval(function () {vasync.pipeline({funcs: [
                function _getTopology(_2, _cb2) {
                    manatee_common.loadTopology(ZK_CLIENT,
                                                function (err, topology) {
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
                        manatee_common.pgStatus([_2.topology], _cb2);
                    } catch (e) {
                        LOG.info({err: e, topology: _2.topology});
                        return _cb2(e);
                    }
                },
                function _verifyTopology(_2, _cb2) {
                    _cb2 = once(_cb2);
                    topo = _2.topology;
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
                        LOG.info({err: e, topology: _2.topology},
                                 'unable to verify topology');
                        return _cb2(e);
                    }
                },
                function _verifySyncState(_2, _cb2) {
                    try {
                        var p = fs.readFileSync(MANATEES[_2.
                                                topology.primary.pgUrl].
                                                cookieLocation, 'utf8');
                        var s = fs.readFileSync(MANATEES[_2.topology.sync.
                                                pgUrl].cookieLocation, 'utf8');
                        var a = fs.readFileSync(MANATEES[_2.topology.async.
                                                pgUrl].cookieLocation, 'utf8');

                        assert.equal('primary', JSON.parse(p).role);
                        assert.equal('sync', JSON.parse(s).role);
                        assert.equal('async', JSON.parse(a).role);
                        return _cb2();
                    } catch (e) {
                        LOG.warn({err: e, topology: _2.topology},
                                 'unable to verify sync state');
                        return _cb2(e);
                    }
                }
            ], arg: {}}, function (err, results) {
                if (err) {
                    LOG.info({err: err, results: results},
                             'topology not correct');
                    return;
                }
                clearInterval(intervalId);
                return _cb();
            }); }, 3000);

            setTimeout(function () {
                clearInterval(intervalId);
                if (!_cb.called) {
                    LOG.fatal({topology: topo},
                              'new peer did not join shard in time');
                }
                return _cb(new verror.VError(
                    'new peer did not join shard in time'));
            }, TIMEOUT).unref();
        }
    ], arg: {}}, function (err, results) {
        if (err) {
            LOG.error({err: err, results: results});
            t.fail(err);
        }
        t.done();
    });
};

//exports.safeMode = function (t) {
    //vasync.pipeline({funcs: [
        //function _stopManatees(_, _cb) {
            //var barrier = vasync.barrier();
            //barrier.on('drain', function () {
                //return _cb();
            //});

            //Object.keys(MANATEES).forEach(function (m) {
                //var id = uuid.v4();
                //barrier.start(id);
                //MANATEES[m].kill(function () {
                    //barrier.done(id);
                //});
            //});
        //},
        //function loadAndVerifyTopology(_, _cb) {
            //getTopology(function (err, topology) {
                //_.topology = topology;
                //_.primaryPgUrl = _.topology.primary.pgUrl;
                //_.syncPgUrl = _.topology.sync.pgUrl;
                //_.asyncPgUrl = _.topology.async.pgUrl;
                //return _cb(err);
            //});
        //},
        //function
    //], arg: {}}, function (err, results) {
        //if (err) {
            //LOG.error({err: err, results: results});
            //t.fail(err);
        //}
        //t.done();
    //});
//};

exports.add4thManatee = function (t) {
    var n4 = uuid.v4();
    var n4Port = 40000;
    var n4Opts = {
        zfsDataset: PARENT_ZFS_DS + '/' + n4,
        zfsPort: n4Port,
        mountPoint: FS_PATH_PREFIX + '/' + n4,
        backupPort: ++n4Port,
        postgresPort: ++n4Port,
        cookieLocation: FS_PATH_PREFIX + '/' + n4 + '_metadata' + '/cookie',
        backupServerPort: ++n4Port,
        configLocation: FS_PATH_PREFIX + '/' + n4 + '_metadata' + '/config',
        metadataDir: FS_PATH_PREFIX + '/' + n4 + '_metadata',
        shardPath: SHARD_PATH,
        postgresUserId: PG_UID,
        log: LOG
    };
    var n4Url;
    vasync.pipeline({funcs: [
        function loadAndVerifyTopology(_, _cb) {
            getTopology(function (err, topology) {
                _.topology = topology;
                _.primaryPgUrl = _.topology.primary.pgUrl;
                _.syncPgUrl = _.topology.sync.pgUrl;
                _.asyncPgUrl = _.topology.async.pgUrl;
                return _cb(err);
            });
        },
        function _startN4(_, _cb) {
            var manatee = new Manatee(n4Opts, function (err) {
                if (err) {
                    LOG.warn({err: err}, 'could not start manatee');
                    return (_cb);
                }

                MANATEES[manatee.pgUrl] = manatee;
                n4Url = manatee.pgUrl;
                return _cb();
            });
        },
        function checkTopology(_, _cb) {
            _cb = once(_cb);
            var intervalId = setInterval(function () {vasync.pipeline({funcs: [
                function _getTopology(_2, _cb2) {
                    manatee_common.loadTopology(ZK_CLIENT,
                                                function (err, topology) {
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
                        manatee_common.pgStatus([_2.topology], _cb2);
                    } catch (e) {
                        LOG.info({err: e, topology: _2.topology});
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
                        assert.ok(_2.topology.primary.repl,
                                  'no sync repl state');
                        assert.equal(_2.topology.primary.repl.sync_state,
                                     'sync', 'no sync replication state.');
                        assert.ok(_2.topology.sync, 'sync DNE');
                        assert.equal(_2.topology.sync.repl.sync_state,
                                     'async', 'no async replication state');
                        assert.ok(_2.topology.async, 'async DNE');
                        assert.equal(_2.topology.async.repl.sync_state,
                                     'async', 'no async replication state');
                        assert.ok(_2.topology.async1, 'async1 DNE');
                        return _cb2();
                    } catch (e) {
                        LOG.info({err: e, topology: _2.topology});
                        return _cb2(e);
                    }
                },
                function _verifySyncState(_2, _cb2) {
                    try {
                        var p = fs.readFileSync(MANATEES[_2.
                                                topology.primary.pgUrl].
                                                cookieLocation, 'utf8');
                        var s = fs.readFileSync(MANATEES[_2.topology.sync.
                                                pgUrl].cookieLocation, 'utf8');
                        var a = fs.readFileSync(MANATEES[_2.topology.async.
                                                pgUrl].cookieLocation, 'utf8');
                        var a1 = fs.readFileSync(MANATEES[_2.topology.async1.
                                                pgUrl].cookieLocation, 'utf8');

                        assert.equal('primary', JSON.parse(p).role);
                        assert.equal('sync', JSON.parse(s).role);
                        assert.equal('async', JSON.parse(a).role);
                        assert.equal('async', JSON.parse(a1).role);
                        return _cb2();
                    } catch (e) {
                        LOG.warn({err: e, topology: _2.topology},
                                 'unable to verify sync state');
                        return _cb2(e);
                    }
                }
            ], arg: {}}, function (err, results) {
                if (err) {
                    LOG.info({err: err, results: results},
                             'topology not correct');
                    return;
                }
                clearInterval(intervalId);
                return _cb();
            }); }, 3000);

            setTimeout(function () {
                clearInterval(intervalId);
                return _cb(new verror.VError(
                    'new peer did not join shard in time'));
            }, TIMEOUT).unref();
        },
        function _killN4(_, _cb) {
            MANATEES[n4Url].kill(function (err) {
                delete MANATEES[n4Url];
                return _cb(err);
            });
        },
        function checkTopology2(_, _cb) {
            _cb = once(_cb);
            var intervalId = setInterval(function () {vasync.pipeline({funcs: [
                function _getTopology(_2, _cb2) {
                    manatee_common.loadTopology(ZK_CLIENT,
                                                function (err, topology) {
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
                        manatee_common.pgStatus([_2.topology], _cb2);
                    } catch (e) {
                        LOG.info({err: e, topology: _2.topology});
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
                        assert.ok(_2.topology.primary.repl,
                                  'no sync repl state');
                        assert.equal(_2.topology.primary.repl.sync_state,
                                     'sync', 'no sync replication state.');
                        assert.ok(_2.topology.sync, 'sync DNE');
                        assert.equal(_2.topology.sync.repl.sync_state,
                                     'async', 'no async replication state');
                        assert.ok(_2.topology.async, 'async DNE');
                        return _cb2();
                    } catch (e) {
                        LOG.info({err: e, topology: _2.topology});
                        return _cb2(e);
                    }
                },
                function _verifySyncState(_2, _cb2) {
                    try {
                        var p = fs.readFileSync(MANATEES[_2.
                                                topology.primary.pgUrl].
                                                cookieLocation, 'utf8');
                        var s = fs.readFileSync(MANATEES[_2.topology.sync.
                                                pgUrl].cookieLocation, 'utf8');
                        var a = fs.readFileSync(MANATEES[_2.topology.async.
                                                pgUrl].cookieLocation, 'utf8');

                        assert.equal('primary', JSON.parse(p).role);
                        assert.equal('sync', JSON.parse(s).role);
                        assert.equal('async', JSON.parse(a).role);
                        return _cb2();
                    } catch (e) {
                        LOG.warn({err: e, topology: _2.topology},
                                 'unable to verify sync state');
                        return _cb2(e);
                    }
                }
            ], arg: {}}, function (err, results) {
                if (err) {
                    LOG.info({err: err, results: results},
                             'topology not correct');
                    return;
                }
                clearInterval(intervalId);
                return _cb();
            }); }, 3000);

            setTimeout(function () {
                clearInterval(intervalId);
                return _cb(new verror.VError(
                    'new peer did not join shard in time'));
            }, TIMEOUT).unref();
        }
    ], arg: {}}, function (err, results) {
        if (err) {
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
                    LOG.info({err: err}, 'unable to destroy zfs dataset');
                });
            }, 2000);

            setTimeout(function () {
                clearInterval(intervalId);
                return _cb(new verror.VError(
                    'timed out trying to destroy dataset'));
            }, TIMEOUT).unref();
        },
        function _cleanupZK(_, _cb) {
            ZK_CLIENT.rmr('/manatee', _cb);
        },
        function _closeZK(_, _cb) {
            ZK_CLIENT.once('close', _cb);
            ZK_CLIENT.close();
        }
    ], arg: {}}, function (err, results) {
        LOG.info({err: err, results: err ? results : null}, 'finished after()');
        t.done();
    });
};

/*
 * Private helpers.
 */

function getTopology(callback) {
    callback = once(callback);
    LOG.info('entering getTopology');
    var error;
    var _ = {};
    var intervalId = setInterval(manatee_common.loadTopology(ZK_CLIENT,
                                    function (err, topology) {
        if (err) {
            error = err;
            return;
        }
        _.topology = topology[SHARD_ID];
        try {
            assert.ok(_.topology);
            assert.ok(_.topology.primary);
            assert.ok(_.topology.sync);
            assert.ok(_.topology.async);
            assert.ok(_.topology.primary.pgUrl);
            assert.ok(_.topology.sync.pgUrl);
            assert.ok(_.topology.async.pgUrl);
        } catch (e) {
            error = e;
            return;
        }
        _.asyncPgUrl = _.topology.async.pgUrl;
        _.syncPgUrl = _.topology.sync.pgUrl;
        _.primaryPgUrl = _.topology.primary.pgUrl;
        LOG.info({topology: topology}, 'got topology');

        manatee_common.pgStatus([_.topology], function (err2) {
            LOG.info({err2: err, topology: topology}, 'got topology with repl');
            if (err2) {
                error = err;
                return;
            }

            try {
                assert.ok(_.topology.primary.repl);
                assert.ok(_.topology.sync.repl);
                assert.equal(_.topology.primary.repl.sync_state, 'sync');
                assert.equal(_.topology.sync.repl.sync_state, 'async');
            } catch (e) {
                error = e;
                return;
            }

            clearInterval(intervalId);
            return callback(null, _.topology);
        });
    }), 3000);

    setTimeout(function () {
        if (!callback.called) {
            clearInterval(intervalId);
            LOG.warn({error: error, topology: _.topology},
                      'unable to veryify topology');
            return callback(error, _.topology);
        }
    }, TIMEOUT).unref();
}
