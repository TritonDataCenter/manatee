var assert = require('assert-plus');
var bunyan = require('bunyan');
var ConfParser = require('../lib/confParser');
var fs = require('fs');
var exec = require('child_process').exec;
var path = require('path');
var manatee = require('../bin/manatee_common');
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
        metadataDir: FS_PATH_PREFIX + '/' + n1 + '_metadata'
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
        metadataDir: FS_PATH_PREFIX + '/' + n2 + '_metadata'
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
        metadataDir: FS_PATH_PREFIX + '/' + n3 + '_metadata'
    };

    vasync.pipeline({funcs: [
        function _createZkClient(_, _cb) {
            manatee.createZkClient({
                zk: ZK_URL,
                shard: SHARD_PATH
            }, function (err, zk) {
                ZK_CLIENT = zk;

                return _cb(err);
            });
        },
        function _startN1(_, _cb) {
            startInstance(n1Opts, function (err, manatee) {
                LOG.info({err: err}, 'prepared instance');
                MANATEES.n1 = manatee;
                return _cb();
            });
        },
        function _timeout(_, _cb) {
            setTimeout(_cb, 10000);
        },
        function _startN2(_, _cb) {
            startInstance(n2Opts, function (err, manatee) {
                LOG.info({err: err}, 'prepared instance');
                MANATEES.n2 = manatee;
                return _cb();
            });
        },
        function _timeout2(_, _cb) {
            setTimeout(_cb, 10000);
        },
        function _startN3(_, _cb) {
            startInstance(n3Opts, function (err, manatee) {
                LOG.info({err: err}, 'prepared instance');
                MANATEES.n3 = manatee;
                return _cb();
            });
        },
        function _timeout3(_, _cb) {
            setTimeout(_cb, 10000);
        },
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
            manatee.loadTopology(ZK_CLIENT, function (err, topology) {
                _.topology = topology[SHARD_ID];
                if (err) {
                    return _cb(err);
                }

                return _cb();
            });
        },
        function getPgStatus(_, _cb) {
            manatee.pgStatus([_.topology], _cb);
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
            manatee.loadTopology(ZK_CLIENT, function (err, topology) {
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
            killManatee(_.primaryPgUrl, _cb);
        },
        function waitForFlip(_, _cb) {
            setTimeout(_cb, 10000);
        },
        function getNewTopology(_, _cb) {
            manatee.loadTopology(ZK_CLIENT, function (err, topology) {
                if (err) {
                    return _cb(err);
                }
                _.topology = topology[SHARD_ID];
                assert.ok(_.topology, 'topology DNE');
                assert.ok(_.topology.primary, 'primary DNE');
                assert.ok(_.topology.sync, 'sync DNE');
                assert.equal(_.topology.async, null,
                            'async should not exist after primary death');
                LOG.info({topology: topology}, 'got topology');
                return _cb();
            });
        },
        function getPgStatus(_, _cb) {
            manatee.pgStatus([_.topology], _cb);
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
            /*
             * empty repl fields look like this: repl: {}. So we have to check
             * the key length in order to figure out that it is an empty/
             * object.
             */
            t.equal(Object.keys(_.topology.sync.repl).length, 0,
                    'sync should not have replication state.');
            return _cb();
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
                Object.keys(MANATEES[m]).forEach(function (p) {
                    // ignore metadata, since it's not a child process.
                    if (p === 'metadata') {
                        return;
                    }
                    barrier.start(m+p);
                    console.log('setting close', m, p, MANATEES[m][p].pid);
                    MANATEES[m][p].on('close', function () {
                        console.log('killed', m, p);
                        barrier.done(m+p);
                    });
                    console.log('killing', m, p, MANATEES[m][p].pid);
                    MANATEES[m][p].kill('SIGKILL');
                });
            });
        },
        function _destroyZfsDataset(_, _cb) {
            console.log('destroying ds');
            exec('zfs destroy -r ' + PARENT_ZFS_DS, _cb);
        },
        function _removeMetadata(_, _cb) {
            exec('rm -rf ' + FS_PATH_PREFIX, _cb);
        }
    ], arg: {}}, function (err, results) {
        console.log('finished killing', err);
        console.log(err, results);
        LOG.info({err: err, results: results}, 'finished after()');
        t.done();
    });
};

/*
 * Helpers
 */

function spawnComponents(opts, cb) {
    var SPAWN_SITTER_OPTS = ['-l', 'child', '-o', 'noorphan', 'sudo', '-u',
        'postgres', '../build/node/bin/node', '../sitter.js', '-v', '-f',
        opts.sitterCfg || './etc/sitter.json'];
    var SPAWN_BS_OPTS = ['-l', 'child', '-o', 'noorphan', 'sudo', '-u',
        'postgres', '../build/node/bin/node', '../backupserver.js', '-v', '-f',
        opts.bsCfg || './etc/backupserver.json'];
    var SPAWN_SS_OPTS = ['-l', 'child', '-o', 'noorphan', 'sudo', '-u',
        'postgres', '../build/node/bin/node', '../snapshotter.js', '-v', '-f',
        opts.ssCfg || './etc/snapshotter.json'];

    var manatee = {};

    manatee.sitter = spawn('/usr/bin/ctrun', SPAWN_SITTER_OPTS);
    manatee.backupServer = spawn('/usr/bin/ctrun', SPAWN_BS_OPTS);
    manatee.snapshotter = spawn('/usr/bin/ctrun', SPAWN_SS_OPTS);
    manatee.metadata = {
        opts: opts,
        pgUrl: opts.cfgObj.sitter.postgresMgrCfg.url
    };

    //if (opts.enableOutput) {
        manatee.sitter.stdout.on('data', function (data) {
            //console.log(data.toString());
        });

        manatee.sitter.stderr.on('data', function (data) {
            //console.log(data.toString());
        });

        manatee.snapshotter.stdout.on('data', function (data) {
            //console.log(data.toString());
        });

        manatee.snapshotter.stderr.on('data', function (data) {
            //console.log(data.toString());
        });

        manatee.backupServer.stdout.on('data', function (data) {
            //console.log(data.toString());
        });

        manatee.backupServer.stderr.on('data', function (data) {
            //console.log(data.toString());
        });
    //}

    return cb(null, manatee);
}

/**
 * @callback startInstance-cb
 * @param {Error} err
 * @param {object} manatee
 */

/**
 * prepare a manatee instance such that it can run.
 *
 * @param {object} opts
 * @param {string} opts.zfsDataset The child zfs datset name.
 * @param {number} opts.zfsPort
 * @param {number} opts.heartbeatServerPort The heartbeatServer port.
 * @param {string} opts.mountPoint The zfs dataset mount point.
 * @param {number} opts.backupPort
 * @param {number} opts.postgresPort
 * @param {string} opts.metadataDir
 * configs for this instance.
 *
 * @param {startinstance-cb} cb
 */
function startInstance(opts, cb) {
    assert.object(opts, 'opts');
    assert.string(opts.zfsDataset, 'opts.zfsDataset');
    assert.number(opts.zfsPort, 'opts.zfsPort');
    assert.number(opts.heartbeatServerPort, 'opts.heartbeatServerPort');
    assert.string(opts.mountPoint, 'opts.mountPoint');
    assert.number(opts.backupPort, 'opts.backupPort');
    assert.number(opts.postgresPort, 'opts.postgresPort');
    assert.string(opts.metadataDir, 'opts.metadataDir');

    LOG.info('instance opts', opts);

    var snapshotDir = opts.mountPoint + '/.zfs/snapshot';
    var pgUrl = getPostgresUrl(MY_IP, opts.postgresPort, 'postgres');
    var configLocation = opts.metadataDir + '/config';
    var postgresConf = configLocation+ '/postgres.conf';
    var cookieLocation = opts.metadataDir + '/sync_cookie';
    var manatee = null;

    vasync.pipeline({funcs: [
        function _createParentZfsDataset(_, _cb) {
            exec('zfs create ' + PARENT_ZFS_DS, function (err, stdout, stderr) {
                return _cb();
            });
        },
        function _createZfsChildDataset(_, _cb) {
            exec('zfs create ' + opts.zfsDataset, function (err, stdout, stderr)
            {
                LOG.info({
                    err: err,
                    ds: opts.zfsDataset
                }, 'created zfs dataset');
                return _cb();
            });
        },
        function _createMountDir(_, _cb) {
            shelljs.mkdir('-p', opts.mountPoint);
            return _cb();
        },
        function _setZfsMountPoint(_, _cb) {
            var cmd = 'zfs set mountpoint=' + opts.mountPoint + ' ' +
                opts.zfsDataset;
            exec(cmd, function (err) {
                return _cb(err);
            });
        },
        function _createPgDataDir(_, _cb) {
            shelljs.mkdir('-p', opts.mountPoint + '/data');
            return _cb();
        },
        function _createConfigDir(_, _cb) {
            shelljs.mkdir('-p', configLocation + '/data');
            return _cb();
        },
        function _chownMetadataDir(_, _cb) {
            exec('chown -R postgres ' + opts.metadataDir, _cb);
        },
        function _enableSnapshotDir(_, _cb) {
            var cmd = 'zfs set snapdir=visible ' + opts.zfsDataset;
            exec(cmd, _cb);
        },
        function _updatePostgresConfig(_, _cb) {
            ConfParser.read('./etc/postgres.integ.conf', function (err, conf) {
                if (err) {
                    return _cb(new verror.VError(err));
                }

                ConfParser.set(conf, 'port', opts.postgresPort);

                ConfParser.write(postgresConf, conf, _cb);
            });
        },
        function _updateSitterConfig(_, _cb) {
            var cfg = JSON.parse(fs.readFileSync(SITTER_CFG));
            cfg.backupPort = opts.backupPort;
            cfg.postgresPort = opts.postgresPort;
            cfg.heartbeatServerPort = opts.heartbeatServerPort;
            cfg.shardPath = SHARD_PATH;
            cfg.ip = MY_IP;

            cfg.heartbeatClientCfg.url = MY_IP;
            cfg.heartbeatClientCfg.postgresUrl = pgUrl;

            cfg.heartbeatServerCfg.port = opts.heartbeatServerPort;

            cfg.postgresMgrCfg.dataDir = opts.mountPoint + '/data';
            cfg.postgresMgrCfg.snapShotterCfg.dataset = opts.zfsDataset;
            cfg.postgresMgrCfg.snapShotterCfg.snapshotDir = snapshotDir;
            cfg.postgresMgrCfg.snapShotterCfg.pgUrl = pgUrl;
            cfg.postgresMgrCfg.syncStateCheckerCfg.cookieLocation =
                cookieLocation;
            cfg.postgresMgrCfg.url = pgUrl;
            cfg.postgresMgrCfg.postgresConf = postgresConf;
            cfg.postgresMgrCfg.zfsClientCfg.dataset = opts.zfsDataset;
            cfg.postgresMgrCfg.zfsClientCfg.parentDataset =
                path.dirname(opts.zfsDataset);
            cfg.postgresMgrCfg.zfsClientCfg.snapshotDir = snapshotDir;
            cfg.postgresMgrCfg.zfsClientCfg.zfsPort = opts.zfsPort;
            cfg.postgresMgrCfg.zfsClientCfg.mountpoint = opts.mountPoint;
            _.sitterCfg = cfg;
            return _cb();
        },
        function _updateBsConfig(_, _cb) {
            var cfg = JSON.parse(fs.readFileSync(BS_CFG));
            cfg.backupServerCfg.port = opts.backupPort;
            cfg.backupSenderCfg.dataset = opts.zfsDataset;
            cfg.backupSenderCfg.snapshotDir = snapshotDir;
            _.bsCfg = cfg;
            return _cb();
        },
        function _updateSsConfig(_, _cb) {
            var cfg = JSON.parse(fs.readFileSync(SS_CFG));
            cfg.dataset = opts.zfsDataset;
            cfg.snapshotDir = snapshotDir;
            cfg.pgUrl = pgUrl;

            _.ssCfg = cfg;
            return _cb();
        },
        function _writeConfig(_, _cb) {
            shelljs.mkdir('-p', configLocation);
            fs.writeFileSync(configLocation + '/sitter.cfg',
                             JSON.stringify(_.sitterCfg));
            fs.writeFileSync(configLocation + '/ss.cfg',
                             JSON.stringify(_.ssCfg));
            fs.writeFileSync(configLocation + '/bs.cfg',
                             JSON.stringify(_.bsCfg));
            return _cb();
        },
        function _spawnComponents(_, _cb) {
            spawnComponents({
                sitterCfg: configLocation + '/sitter.cfg',
                ssCfg: configLocation + '/ss.cfg',
                bsCfg: configLocation + '/bs.cfg',
                cfgObj: {
                    ss: _.ssCfg,
                    bs: _.bsCfg,
                    sitter: _.sitterCfg
                }
            }, function (err, _manatee) {
                manatee = _manatee;
                return _cb(err);
            });
        }
    ], arg: {}}, function (err, results) {
        LOG.info({err: err, results: err ? results : null},
                 'finished starting manatee');
        return cb(err, manatee);
    });
}

function killManatee(pgUrl, cb) {
    vasync.pipeline({funcs: [
        function _findManatee(_, _cb) {
            _cb = once(_cb);

            var barrier = vasync.barrier();
            barrier.on('drain', function () {
                return _cb(new verror.VError('could not find manatee: ' +
                                             pgUrl));
            });

            Object.keys(MANATEES).forEach(function (m) {
                barrier.start(m);
                if (MANATEES[m].metadata.pgUrl === pgUrl) {
                    _.manatee = MANATEES[m];

                    return _cb();
                } else {
                    barrier.done(m);
                }
            });

        },
        function _killManatee(_, _cb) {
            var barrier = vasync.barrier();
            barrier.start('sitter');
            barrier.start('snapshotter');
            barrier.start('backupServer');
            barrier.on('drain', function () {
                return _cb();
            });

            _.manatee.sitter.once('close', function () {
                LOG.info('killed sitter');
                barrier.done('sitter');
            });

            _.manatee.snapshotter.once('close', function () {
                LOG.info('killed snapshotter');
                barrier.done('snapshotter');
            });

            _.manatee.backupServer.once('close', function () {
                LOG.info('killed backupServer');
                barrier.done('backupServer');
            });

            LOG.info('killing sitter');
            _.manatee.sitter.kill('SIGKILL');
            LOG.info('killing snapshotter');
            _.manatee.snapshotter.kill('SIGKILL');
            LOG.info('killing backupserver');
            _.manatee.backupServer.kill('SIGKILL');
        }
    ], arg:{}}, function (err, results) {
        return cb(err, results);
    });
}

function getPostgresUrl(ip, port, db) {
    return 'tcp://postgres@' + ip + ':' + port + '/' + db;
}
