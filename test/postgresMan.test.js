var Logger = require('bunyan');
var shelljs = require('shelljs');
var spawn = require('child_process').spawn;
var tap = require('tap');
var test = require('tap').test;
var PostgresMan = require('../lib/postgresMan');
var uuid = require('node-uuid');

var LOG = new Logger({
  name: 'postgresMan-test',
  src: true,
  level: 'trace'
});

var log = new Logger({
  name: 'postgresMan-test',
  src: true,
  level: 'trace'
});

var LOG_FILE = '/tmp/pg/' + uuid();
var DATA_DIR = '/tmp/pg/' + uuid();
var POSTGRES_MAN;
var PRIMARY_URL = 'tcp://yunong@localhost:5432/test';
var STANDBY_URL = 'tcp://yunong@localhost:5433/test';

test('killall postgres instances', function(t) {
  shelljs.mkdir('-p', '/tmp/pg');
  shelljs.rm('-rf', '/tmp/pg/*');
  spawn('killall', ['-KILL', 'postgres']);
  t.end();
});

test('setup postgres', function(t) {
  var postgresManCfg = {
    log: LOG,
    pgCtlPath: '/opt/local/bin/pg_ctl',
    pgCreateDbPath: 'createdb',
    pgInitDbPath: 'initdb',
    postgresPath: 'postgres',
    hbaConf: './test_conf/pg_hba.conf',
    postgresConf: './test_conf/postgresql.conf',
    recoveryConf: './test_conf/recovery.conf',
    dbName: 'test',
    dbUser: 'yunong',
    dataDir: DATA_DIR,
    logFile: '/tmp/pg.log',
    url: 'tcp://yunong@localhost:5432/postgres',
    healthChkInterval: 1000,
    opsTimeout: 2000,
    backupClientCfg:{
      log: LOG,
      dataset: 'YOUR DAD',
      snapshotDir: '/tmp/foo',
      zfsHost: '10.0.0.0',
      zfsPort: 1234,
      pollInterval: 100,
      zfsRecvPath: './zfs_recv'
    }
  };
  POSTGRES_MAN = new PostgresMan(postgresManCfg);

  t.ok(POSTGRES_MAN, 'instantiate postgresman');
  t.end();
});

test('healthcheck on start', function(t) {
  // pg can't start since initdb wasn't called
  POSTGRES_MAN.start(POSTGRES_MAN, function(err, process) {
    t.ok(err);
    t.end();
  });
});

//test('initdb', function(t) {
  //POSTGRES_MAN.initDb(POSTGRES_MAN, function(err) {
    //if (err) {
      //t.fail(err);
      //t.end();
    //}
    //t.end();
  //});
//});

//test('primary', function(t) {
  //POSTGRES_MAN.primary([], function(err) {
    //if (err) {
      //t.fail(err);
      //t.end();
    //}
    //t.end();
  //});
//});

test('standby', function(t) {
  var pMan = new PostgresMan({
    log: LOG,
    pgCtlPath: '/opt/local/bin/pg_ctl',
    pgCreateDbPath: 'createdb',
    pgInitDbPath: 'initdb',
    postgresPath: 'postgres',
    hbaConf: './test_conf/pg_hba.conf',
    postgresConf: './test_conf/postgresql.conf',
    recoveryConf: './test_conf/recovery.conf',
    dbName: 'test',
    dbUser: 'yunong',
    dataDir: DATA_DIR,
    logFile: '/tmp/pg.log',
    url: 'tcp://yunong@localhost:5432/postgres',
    healthChkInterval: 1000,
    opsTimeout: 2000,
    backupClientCfg:{
      log: LOG,
      dataset: 'YOUR DAD',
      snapshotDir: '/tmp/foo',
      zfsHost: '10.0.0.0',
      zfsPort: 1234,
      pollInterval: 100,
      zfsRecvPath: './zfs_recv'
    }
  });

  pMan.standby(PRIMARY_URL, 'foobar', function() {
    t.end();
  });
});

//test('initialize postgres', function(t) {
  //POSTGRES_MAN.initDb(POSTGRES_MAN, function(err) {
    //if (err) {
       //t.fail(err);
       //t.end();
    //}
    //POSTGRES_MAN.stat(function(stat, err) {
      //if (err) {
        //t.fail(err);
        //t.end();
      //}
      //t.equal(stat, 1);
      //t.end();
    //});
  //});
//});

//test('init already initialized postgres', function(t) {
  //POSTGRES_MAN.initDb(POSTGRES_MAN, function(err) {
    //t.end();
  //});
//});

//test('stop postgres', function(t) {
  //POSTGRES_MAN.shutdown(function(err) {
    //t.end();
  //});
//});

//test('check no pg running', function(t) {
  //POSTGRES_MAN.stat(function(stat, err) {
    //t.equal(stat, 1);
    //t.end();
  //});
//});

//test('start postgres', function(t) {
  //POSTGRES_MAN.start(function(err) {
    //if (err) {
      //t.fail(err);
      //t.end();
    //}

    //POSTGRES_MAN.stat(function(stat, err) {
      //if (err) {
        //t.fail(err);
        //t.end();
      //}
      //t.equal(stat, 0);
      //POSTGRES_MAN.health(function(err) {
        //if (err) {
          //t.fail(err);
          //t.end();
        //}
        //POSTGRES_MAN.xlogLocation(function(err) {
          //if (err) {
            //t.fail(err);
            //t.end();
          //}
          //t.end();
        //})
      //})
    //});
  //});
//});

//test('createdb', function(t) {
  //POSTGRES_MAN.createDb(uuid(), function(err) {
    //if (err) {
      //t.fail(err);
      //t.end();
    //}

    //t.end();
  //});
//});

//test('restart postgres', function(t) {
  //POSTGRES_MAN.restart(function(err) {
    //if (err) {
      //t.fail(err);
      //t.end();
    //}

    //POSTGRES_MAN.stat(function(stat, err) {
      //if (err) {
        //t.fail(err);
        //t.end();
      //}
      //t.equal(stat, 0);
      //t.end();
    //});
  //});
//});

tap.tearDown(function() {
  process.exit(tap.output.results.fail);
});
