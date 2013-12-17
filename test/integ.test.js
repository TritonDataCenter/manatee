var fs = require('fs');
var exec = require('child_process').exec;
var spawn = require('child_process').spawn;
var uuid = require('node-uuid');
var vasync = require('vasync');

var ZK_INSTANCE = process.env.ZK_INSTANCE || 'localhost:2181';
var PARENT_ZFS_DS = process.env.PARENT_ZFS_DS;
var SHARD_PATH = uuid.v4();
var SITTER_CFG = './etc/sitter.json';
var BS_CFG = './etc/backupserver.json';
var SS_CFG = './etc/snapshotter.json';
var MY_IP = '0.0.0.0';

function startManatee(opts, cb) {
    /* BEGIN JSSTYLED */
    var SPAWN_SITTER_OPTS = ['-u', 'postgres', '../build/node/bin/node', '../sitter.js', '-v', '-f', opts.sitterCfg || './etc/sitter.json'];
    var SPAWN_BS_OPTS = ['-u', 'postgres', '../build/node/bin/node', '../backupserver.js', '-v', '-f', opts.bsCfg || './etc/backupserver.json'];
    var SPAWN_SS_OPTS = ['-u', 'postgres', '../build/node/bin/node', '../snapshotter.js', '-v', '-f', opts.ssCfg || './etc/snapshotter.json'];
    /* END JSSTYLED */

    var manatee = {
        sitter: null,
        snapshotter: null,
        backupServer: null
    };

    manatee.sitter = spawn('sudo', SPAWN_SITTER_OPTS);
    manatee.snapshotter = spawn('sudo', SPAWN_BS_OPTS);
    manatee.backupServer = spawn('sudo', SPAWN_SS_OPTS);

    return cb(manatee);
}

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
 * @param {number} opts.heartbeatServerPort
 * @param {string} opts.cookieLocation
 * @param {number} opts.backupServerPort
 */
function prepareInstance(opts, cb) {
    var parentDataset, dataset = null;
    var snapshotDir = opts.mountPoint + '/.zfs/snapshots';
    var pgUrl = getPostgresUrl(MY_IP, opts.postgresPort, 'postgres');
    vasync.pipeline({funcs: [
        function _getZfsParentDataset(_, _cb) {
            if (PARENT_ZFS_DS) {
                parentDataset = PARENT_ZFS_DS;
                return _cb();
            }
            exec('zfs list', function (err, stdout, stderr) {
                if (err) {
                    return _cb(err);
                }
                var datasets = stdout.split('\n');
                // Ignore the last entry in the array, hence length - 2,
                // because the last dataset is appended with another \n, making
                // the last entry in the array null.
                parentDataset = datasets[dataset.length - 2].split('\t')[0];
                dataset = parentDataset + '/' + opts.zfsDataset;
                return _cb();
            });
        },
        function _createZfsChildDataset(_, _cb) {
            exec('zfs create ' + dataset, function (err, stdout, stderr) {
                return _cb(err);
            });
        },
        function _updateSitterConfig(_, _cb) {
            var cfg = JSON.parse(fs.readFileSync(SITTER_CFG));
            cfg.backupPort = opts.backupPort;
            cfg.postgresPort = opts.postgresPort;
            cfg.shardPath = SHARD_PATH;
            cfg.ip = MY_IP;

            cfg.heartbeatClientCfg.url = MY_IP;

            cfg.heartbeatServerCfg.port = opts.heartbeatServerPort;

            cfg.postgresMgrCfg.datadir = opts.mountPoint + '/data';
            cfg.postgresMgrCfg.snapshotterCfg.dataset = dataset;
            cfg.postgresMgrCfg.snapshotterCfg.snapshotDir = snapshotDir;
            cfg.postgresMgrCfg.snapshotterCfg.pgUrl = pgUrl;
            cfg.postgresMgrCfg.syncStateCheckerCfg.cookieLocation =
                opts.cookieLocation;
            cfg.postgresMgrCfg.url = pgUrl;
            cfg.postgresMgrCfg.zfsClientCfg.dataset = dataset;
            cfg.postgresMgrCfg.zfsClientCfg.parentDataset = parentDataset;
            cfg.postgresMgrCfg.zfsClientCfg.snapshotDir = snapshotDir;
            cfg.postgresMgrCfg.zfsClientCfg.zfsPort = opts.zfsPort;
            cfg.postgresMgrCfg.zfsClientCfg.Mountpoint = opts.mountPoint;
            return _cb();
        },
        function _updateBsConfig(_, _cb) {
            var cfg = JSON.parse(fs.readFileSync(BS_CFG));
            cfg.backupServerCfg.port = opts.backupServerPort;
            cfg.backupSenderCfg.dataset = dataset;
            cfg.backupSenderCfg.snapshotDir = snapshotDir;
            return _cb();
        },
        function _updateSsConfig(_, _cb) {
            var cfg = JSON.parse(fs.readFileSync(SS_CFG));
            cfg.dataset = dataset;
            cfg.snapshotDir = snapshotDir;
            cfg.pgUrl = pgUrl;
        },
        function _startManatee(_, _cb) {

        }
    ], arg: {}}, function (err) {

    });
}

function getPostgresUrl(ip, port, db) {
    return 'tcp://postgres@' + ip + ':' + port + '/' + db;
}
