// Copyright (c) 2012, Joyent, Inc. All rights reserved.
var Logger = require('bunyan');
var tap = require('tap');
var test = require('tap').test;
var Shard = require('../lib/shard');
var uuid = require('node-uuid');

var LOG = new Logger({
        name: 'shard-test',
        src: true,
        level: 'info'
});

var dataset = 'zones/' + '66dc0dd4-e1cd-4064-9dbd-df67395dc0fe' + '/data';
var dataDir = '/' + dataset + '/pg';
var snapshotDir = '/' + dataset + '/.zfs/snapshot/';
var currHost = '10.99.99.118';

var shardCfg = {
        log: LOG,
        shardPath: '/yunong',
        zkCfg: {
                pollInterval: 200,
                log: LOG,
                servers: [ {
                        host: (process.env.ZK_HOST || '10.99.99.11'),
                        port: (process.env.ZK_PORT || 2181)
                }],
                timeout: 1000
        },
        postgresManCfg: {
                log: LOG,
                pgCtlPath: '/opt/smartdc/manatee/bin/pgsql/bin/pg_ctl',
                postgresPath: '/opt/smartdc/manatee/bin/pgsql/bin/postgres',
                pgInitDbPath: '/opt/smartdc/manatee/bin/pgsql/bin/initdb',
                hbaConf: '/root/manatee/cfg/pg_hba.conf',
                postgresConf: '/root/manatee/cfg/postgresql.conf',
                recoveryConf: '/root/manatee/cfg/recovery.conf',
                dbUser: 'postgres',
                dataDir: dataDir,
                url: 'tcp://postgres@' + currHost + ':5432/postgres',
                healthChkInterval: 1000,
                opsTimeout: 2000,
                backupClientCfg: {
                        log: LOG,
                        dataset: dataset,
                        snapshotDir: snapshotDir,
                        zfsHost: currHost || '10.99.99.12',
                        zfsPort: 1234,
                        pollInterval: 1000,
                        zfsRecvPath: '/opt/smartdc/manatee/bin/zfs_recv'
                },
                snapShotterCfg: {
                        dataset: dataset,
                        snapshotDir: snapshotDir,
                        pollInterval: 5000,
                        snapshotNumber: 5,
                        pgUrl: 'tcp://postgres@' + currHost + ':5432/postgres',
                        log: LOG
                }
        },
        heartbeaterCfg: {
                log: LOG,
                port: 12344
        },
        backupPort: 12345,
        postgresPort: 5432,
        url: currHost || '10.99.99.12',
        heartbeatPort: 12344,
        heartbeatInterval: 200
};

var shard = new Shard(shardCfg);

shard.on('connect', function () {
        shard.init();
});
