// Copyright (c) 2012, Joyent, Inc. All rights reserved.
var SnapShotter = require('../lib/snapShotter');
var ZfsClient = require('../lib/zfsClient');
var Logger = require('bunyan');
var tap = require('tap');
var test = require('tap').test;
var spawn = require('../lib/shellSpawner').spawn;
var uuid = require('node-uuid');

var log = new Logger({
  name: 'backupServer-test',
  src: true,
  level: 'trace'
});

var snapShotter = new SnapShotter({
        log: log,
        dataset: 'zones/d9b67e33-b331-4111-88fc-1f743832eb9f/data/manatee',
        snapshotDir: '/manatee/pg/.zfs/snapshot/',
        pollInterval: 3600000,
        snapshotNumber: 5,
        pgUrl: 'tcp://postgres@10.99.99.34:5432/postgres',
        startupDelay: 90000
});
var zfsClientCfg = {
        log: log,
        parentDataset: 'zones/d9b67e33-b331-4111-88fc-1f743832eb9f/data',
        dataset: 'zones/d9b67e33-b331-4111-88fc-1f743832eb9f/data/manatee',
        snapshotDir: '/manatee/pg/.zfs/snapshot/',
        zfsHost: '10.99.99.34',
        zfsPort: 1234,
        serverUrl: 'http://10.99.99.12:12345',
        snapShotter: snapShotter,
        pollInterval: 1000,
        zfsRecvPath: '/opt/smartdc/manatee/bin/zfs_recv',
        mountpoint: '/manatee/pg'
};

var zfsClient = new ZfsClient(zfsClientCfg);
var DATASET = 'zones/d9b67e33-b331-4111-88fc-1f743832eb9f/data/manatee';
var BACKUP_SNAPSHOT;

test('reset state', function (t) {
        /* JSSTYLED */
        spawn('pfexec zfs promote zones/d9b67e33-b331-4111-88fc-1f743832eb9f/data/manatee', log, function (err) {
                t.end();
        });
});

test('reset state', function (t) {
        /* JSSTYLED */
        spawn('pfexec zfs create zones/d9b67e33-b331-4111-88fc-1f743832eb9f/data/manatee', log, function (err) {
                t.end();
        });
});

test('test backup', function (t) {
        zfsClient.backupCurrentDataset(function (err, backupSnapshot) {
                t.notOk(err);
                t.ok(backupSnapshot);
                BACKUP_SNAPSHOT = backupSnapshot;
                log.info('got backup dataset %s' + backupSnapshot);
                t.end();
        });
});

test('restoreBackup', function (t) {
        zfsClient.restoreDataset(BACKUP_SNAPSHOT, DATASET, function (err) {
                t.notOk(err);
                t.end();
        });
});

test('restore-from-remote', function (t) {
        zfsClient.restore(function (err, backupDataset) {
                t.notOk(err);
                t.ok(backupDataset);
                t.end();
        });
});

tap.tearDown(function () {
        process.exit(tap.output.results.fail);
});
