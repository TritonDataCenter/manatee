// Copyright (c) 2012, Joyent, Inc. All rights reserved.
var Logger = require('bunyan');
var shelljs = require('shelljs');
var spawn = require('child_process').spawn;
var tap = require('tap');
var test = require('tap').test;
var PostgresMan = require('../lib/postgresMan2');
var uuid = require('node-uuid');

var LOG = new Logger({
        name: 'postgresMan-test',
        src: true,
        level: 'trace'
});

var POSTGRES_MAN_CFG = {
        log: LOG,
        pgCtlPath: '/opt/smartdc/manatee/bin/pgsql/bin/pg_ctl',
        postgresPath: '/opt/smartdc/manatee/bin/pgsql/bin/postgres',
        pgInitDbPath: '/opt/smartdc/manatee/bin/pgsql/bin/initdb',
        hbaConf: '/root/manatee/cfg/pg_hba.conf',
        postgresConf: '/root/manatee/cfg/postgresql.conf',
        recoveryConf: '/root/manatee/cfg/recovery.conf',
        dbUser: 'postgres',
        dataDir: '/zones/4da04395-d51b-4b5d-966b-1a8e662245c4/data/pg',
        url: 'tcp://postgres@10.99.99.12:5432/postgres',
        healthChkInterval: 1000,
        opsTimeout: 2000,
        backupClientCfg: {
                log: LOG,
                dataset: 'zones/4da04395-d51b-4b5d-966b-1a8e662245c4/data',
                snapshotDir: '/zones/4da04395-d51b-4b5d-966b-1a8e662245c4/data/.zfs/snapshot/',
                zfsHost: '10.99.99.12',
                zfsPort: 1234,
                pollInterval: 100,
                zfsRecvPath: '/opt/smartdc/manatee/bin/zfs_recv'
        }
};

var PMAN = null;

var PRIMARY_URL = 'tcp://postgres@10.99.99.118:5432/postgres';
var BACKUP_URL = 'http://10.99.99.118:12345';

test('init', function(t) {
        PMAN = new PostgresMan(POSTGRES_MAN_CFG);
        t.ok(PMAN);
        t.end();
});

//test('standby', function(t) {
        //PMAN.standby(PRIMARY_URL, BACKUP_URL, function(err) {
                //if(err) {
                        //t.fail(err);
                        //t.end();
                //}

                //t.end();
        //});
//});

test('primary', function(t) {
        PMAN.primary(null, function(err) {
                if(err) {
                        t.fail(err);
                        t.end();
                }

                t.end();
        });
});

tap.tearDown(function() {
        process.exit(tap.output.results.fail);
});
