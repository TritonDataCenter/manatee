var assert = require('assert-plus');
var bunyan = require('bunyan');
var ConfParser = require('../lib/confParser');
var fs = require('fs');
var exec = require('child_process').exec;
var path = require('path');
var spawn = require('child_process').spawn;
var shelljs = require('shelljs');
var uuid = require('node-uuid');
var vasync = require('vasync');
var verror = require('verror');

var FS_PATH_PREFIX = process.env.FS_PATH_PREFIX || '/var/tmp/manatee_tests';
var ZK_INSTANCE = process.env.ZK_INSTANCE || 'localhost:2181';
var PARENT_ZFS_DS = process.env.PARENT_ZFS_DS;
var SHARD_PATH = '/' + uuid.v4();
var SITTER_CFG = './etc/sitter.json';
var BS_CFG = './etc/backupserver.json';
var SS_CFG = './etc/snapshotter.json';
var MY_IP = '0.0.0.0';

var LOG = bunyan.createLogger({
    level: (process.env.LOG_LEVEL || 'info'),
    name: 'manatee-integ-tests',
    serializers: {
        err: bunyan.stdSerializers.err
    },
    // always turn source to true, manatee isn't in the data path
    src: true
});

var MANATEES = {};

function spawnComponents(opts, cb) {
    var SPAWN_SITTER_OPTS = ['-u', 'postgres', '../build/node/bin/node',
        '../sitter.js', '-v', '-f', opts.sitterCfg || './etc/sitter.json'];
    var SPAWN_BS_OPTS = ['-u', 'postgres', '../build/node/bin/node',
        '../backupserver.js', '-v', '-f',
        opts.bsCfg || './etc/backupserver.json'];
    var SPAWN_SS_OPTS = ['-u', 'postgres', '../build/node/bin/node',
        '../snapshotter.js', '-v', '-f',
        opts.ssCfg || './etc/snapshotter.json'];

    var manatee = {
        sitter: null,
        snapshotter: null,
        backupServer: null
    };

    manatee.sitter = spawn('sudo', SPAWN_SITTER_OPTS);
    manatee.backupServer = spawn('sudo', SPAWN_BS_OPTS);
    manatee.snapshotter = spawn('sudo', SPAWN_SS_OPTS);

    manatee.sitter.stdout.on('data', function (data) {
        console.log(data.toString());
    });

    manatee.sitter.stderr.on('data', function (data) {
        console.log(data.toString());
    });

    manatee.snapshotter.stdout.on('data', function (data) {
        console.log(data.toString());
    });

    manatee.snapshotter.stderr.on('data', function (data) {
        console.log(data.toString());
    });

    manatee.backupServer.stdout.on('data', function (data) {
        console.log(data.toString());
    });

    manatee.backupServer.stderr.on('data', function (data) {
        console.log(data.toString());
    });

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
                LOG.info({err: err}, 'created parent zfs dataset');
                return _cb();
            });
        },
        function _createZfsChildDataset(_, _cb) {
            exec('zfs create ' + opts.zfsDataset, function (err, stdout, stderr)
            {
                LOG.info({err: err}, 'created zfs dataset');
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
                //return _cb(new verror.VError(err));
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
                bsCfg: configLocation + '/bs.cfg'
            }, function (err, _manatee) {
                manatee = _manatee;
                return _cb(err);
            });
        }
    ], arg: {}}, function (err, results) {
        return cb(err, manatee);
    });
}

function getPostgresUrl(ip, port, db) {
    return 'tcp://postgres@' + ip + ':' + port + '/' + db;
}

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

    startInstance(n1Opts, function (err, manatee) {
        LOG.info({err: err}, 'prepared instance');
        MANATEES.n1 = manatee;
        setTimeout(t.done, 7000);
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
                    barrier.start(m+p);
                    MANATEES[m][p].kill('SIGKILL');
                    barrier.done(m+p);
                });
            });
        },
        function _destroyZfsDataset(_, _cb) {
            exec('zfs destroy -r ' + PARENT_ZFS_DS, _cb);
        },
        function _removeMetadata(_, _cb) {
            exec('rm -rf ' + FS_PATH_PREFIX, _cb);
        }
    ], arg: {}}, function (err, results) {
        LOG.info({err: err, results: results}, 'finished after()');
        t.done();
    });
};
