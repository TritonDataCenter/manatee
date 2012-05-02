var Logger = require('bunyan');
var shelljs = require('shelljs');
var spawn = require('child_process').spawn;
var tap = require('tap');
var test = require('tap').test;
var PostgresMan = require('../lib/postgresMan');
var uuid = require('node-uuid');

var log = new Logger({
  name: 'postgresMan-test',
  src: true,
  level: 'trace'
});

var LOG_FILE = '/tmp/pg/' + uuid();
var DATA_DIR = '/tmp/pg/' + uuid();
var POSTGRES_MAN;

test('killall postgres instances', function(t) {
  shelljs.mkdir('-p', '/tmp/pg');
  shelljs.rm('-rf', '/tmp/pg/*');
  spawn('killall', ['-KILL', 'postgres']);
  t.end();
});

test('setup postgres', function(t) {
  POSTGRES_MAN = new PostgresMan({
    log: log,
    pgCtlPath: '/usr/local/pgsql/bin/pg_ctl',
    pgInitDbPath: '/usr/local/pgsql/bin/initdb',
    pgHbaPath: './test_conf/pg_hba.conf',
    dbName: 'test',
    dataDir: DATA_DIR,
    logFile: LOG_FILE,
    url: 'tcp://yunong@localhost:5432/test'
  });

  t.ok(POSTGRES_MAN, 'instantiate postgresman');
  t.end();
});

test('initialize postgres', function(t) {
  POSTGRES_MAN.initDb(function(err) {
    if (err) {
       t.fail(err);
       t.end();
    }
    POSTGRES_MAN.stat(function(stat, err) {
      if (err) {
        t.fail(err);
        t.end();
      }
      t.equal(stat, 1);
      t.end();
    });
  });
});

test('init already initialized postgres', function(t) {
  POSTGRES_MAN.initDb(function(err) {
    t.end();
  });
});

test('stop postgres', function(t) {
  POSTGRES_MAN.shutdown(function(err) {
    t.end();
  });
});

test('check no pg running', function(t) {
  POSTGRES_MAN.stat(function(stat, err) {
    t.equal(stat, 1);
    t.end();
  });
});

test('start postgres', function(t) {
  POSTGRES_MAN.start(function(err) {
    if (err) {
      t.fail(err);
      t.end();
    }

    POSTGRES_MAN.stat(function(stat, err) {
      if (err) {
        t.fail(err);
        t.end();
      }
      t.equal(stat, 0);
      POSTGRES_MAN.health(function(err) {
        if (err) {
          t.fail(err);
          t.end();
        }
        POSTGRES_MAN.xlogLocation(function(err) {
          if (err) {
            t.fail(err);
            t.end();
          }
          t.end();
        })
      })
    });
  });
});

test('createdb', function(t) {
  POSTGRES_MAN.createDb(uuid(), function(err) {
    if (err) {
      t.fail(err);
      t.end();
    }

    t.end();
  });
});

test('restart postgres', function(t) {
  POSTGRES_MAN.restart(function(err) {
    if (err) {
      t.fail(err);
      t.end();
    }

    POSTGRES_MAN.stat(function(stat, err) {
      if (err) {
        t.fail(err);
        t.end();
      }
      t.equal(stat, 0);
      t.end();
    });
  });
});

tap.tearDown(function() {
  process.exit(tap.output.results.fail);
});
