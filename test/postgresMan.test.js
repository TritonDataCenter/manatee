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
var DATA_DIR = '/usr/local/pgsql/primary';

var POSTGRES_MAN;

test('setup postgres', function(t) {
  POSTGRES_MAN = new PostgresMan({
    log: log,
    pgPath: '/usr/local/pgsql/bin//pg_ctl',
    dataDir: DATA_DIR,
    logFile: LOG_FILE
  });

  t.ok(POSTGRES_MAN, 'instantiate postgresman');
  t.end();
});

test('stop postgres', function(t) {
  POSTGRES_MAN.stop(function(err) {
    t.end();
  });
});

test('check no pg running', function(t) {
  POSTGRES_MAN.stat(function(err) {
    log.error(err);
    t.ok(err, 'stat postgres');
    t.end();
  });
});

test('start postgres', function(t) {
  POSTGRES_MAN.start(function(err) {
    if (err) {
      t.fail(err);
      t.end();
    }

    POSTGRES_MAN.stat(function(err) {
      if (err) {
        t.fail(err);
        t.end();
      }
      t.end();
    });
  });
});

test('start postgres', function(t) {
  POSTGRES_MAN.restart(function(err) {
    if (err) {
      t.fail(err);
      t.end();
    }

    POSTGRES_MAN.stat(function(err) {
      if (err) {
        t.fail(err);
        t.end();
      }
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
    t.end();
  });
});

tap.tearDown(function() {
  process.exit(tap.output.results.fail);
});
