var Logger = require('bunyan');
var tap = require('tap');
var test = require('tap').test;
var PostgresMan = require('../lib/postgresMan');
var uuid = require('node-uuid');

var log = new Logger({
  name: 'postgresMan-test',
  src: true,
  level: 'trace'
});

var LOG_FILE = '/tmp/' + uuid();
var DATA_DIR = '/tmp/' + uuid();

var POSTGRES_MAN;

test('setup postgres', function(t) {
  POSTGRES_MAN = new PostgresMan({
    log: log,
    pgCtlPath: '/usr/local/pgsql/bin/pg_ctl',
    pgInitDbPath: '/usr/local/pgsql/bin/initdb',
    pgHbaPath: './test_conf/pg_hba.conf',
    dbName: 'test',
    dataDir: DATA_DIR,
    logFile: LOG_FILE
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

    t.end();
  });
});

test('init already initialized postgres', function(t) {
  POSTGRES_MAN.initDb(function(err) {
    t.ok(err);
    t.end();
  });
});

test('stop postgres', function(t) {
  POSTGRES_MAN.stop(function(err) {
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
      t.end();
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

test('stop postgres', function(t) {
  POSTGRES_MAN.stop(function(err) {
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

tap.tearDown(function() {
  process.exit(tap.output.results.fail);
});
