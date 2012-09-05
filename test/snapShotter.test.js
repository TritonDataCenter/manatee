// Copyright (c) 2012, Joyent, Inc. All rights reserved.
var SnapShotter = require('../lib/snapShotter');
var Logger = require('bunyan');
var tap = require('tap');
var common = require('../lib/common');
var test = require('tap').test;
var Shard = require('../lib/shard');
var ZooKeeper = require('zookeeper');
var uuid = require('node-uuid');

var log = new Logger({
        name: 'backupServer-test',
        src: true,
        level: 'trace'
});

var snapShotter = new SnapShotter({
        log: log,
        dataset: 'zones/a6354a44-6d08-40fb-b4fe-7f58100d6d14/data',
        snapshotDir: '/zones/a6354a44-6d08-40fb-b4fe-7f58100d6d14/data/.zfs/snapshot/',
        pollInterval: 5000,
        snapshotNumber: 5,
        pgUrl: 'tcp://yunong@10.99.99.17:5432/postgres'
});

snapShotter.on('err', function(err) {
        log.error('got error from snapshotter', err);
});

snapShotter.start(function() {
        log.info('started snapshotter');
});
