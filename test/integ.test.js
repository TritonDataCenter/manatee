var Logger = require('bunyan');
var shelljs = require('shelljs');
var spawn = require('child_process').spawn;
var tap = require('tap');
var common = require('../lib/common');
var test = require('tap').test;
var PostgresMan = require('../lib/postgresMan');
var Shard = require('../lib/shard');
var ZooKeeper = require('zookeeper');
var uuid = require('node-uuid');
var Daemon = require('../lib/daemon');
var confparser = require('../lib/confParser');

var SHARD_ID = uuid();

var REGISTRAR_PATH = '/' + uuid() + 'registrar';

var MEMBERS = 'toto, dorothy, lion';

var ZK_CFG = {
 connect: 'localhost:2181',
 timeout: 200000,
 debug_level: ZooKeeper.ZOO_LOG_LEVEL_WARNING,
 host_order_deterministic: false,
 data_as_buffer: false
};

var log = new Logger({
  name: 'scallop-test',
  src: true,
  level: 'trace'
});

var dbName = 'test';

var URL = 'tcp://yunong@localhost:5432/' + dbName;
var URL_2 = 'tcp://yunong@localhost:5433/' + dbName;
var URL_3 = 'tcp://yunong@localhost:5434/' + dbName;

var BASE_PATH;

var POSTGRES_PRIMARY_PATH = '/tmp/' + uuid();
var POSTGRES_SYNC_PATH = '/tmp/' + uuid();
var POSTGRES_ASYNC_PATH = '/tmp/' + uuid();

var PG_CTL_PATH = '/usr/local/pgsql/bin/pg_ctl';

var CONFIG_PATH = '/tmp/' + uuid() + '/';

var PG_HBA_TEMPATE = './test_conf/pg_hba.conf';
var RECOVERY_TEMPLATE = './test_conf/recovery.conf';
var POSTGRESQL_TEMPLATE = './test_conf/postgresql.conf';
var POSTGRESQL_TEMPLATE_2 = './test_conf/postgresql2.conf';
var POSTGRESQL_TEMPLATE_3 = './test_conf/postgresql3.conf';

var postgresManCfg = {
  log: log,
  pgCtlPath: '/usr/local/pgsql/bin/pg_ctl',
  pgInitDbPath: '/usr/local/pgsql/bin/initdb',
  pgHbaPath: PG_HBA_TEMPATE,
  dataDir: '/tmp/pg/primary/',
  logFile: '/tmp/pg/primary.log',
  dbName: 'test',
  url: URL
};

var postgresManCfg_2 = {
  log: log,
  pgCtlPath: '/usr/local/pgsql/bin/pg_ctl',
  pgInitDbPath: '/usr/local/pgsql/bin/initdb',
  pgHbaPath: PG_HBA_TEMPATE,
  dataDir: '/tmp/pg/sync/',
  //dataDir: '/tmp/' + uuid(),
  logFile: '/tmp/pg/sync.log',
  dbName: 'test',
  url: URL_2
  //logFile: '/tmp/' + uuid()
};

var postgresManCfg_3 = {
  log: log,
  pgCtlPath: '/usr/local/pgsql/bin/pg_ctl',
  pgInitDbPath: '/usr/local/pgsql/bin/initdb',
  pgHbaPath: PG_HBA_TEMPATE,
  dataDir: '/tmp/pg/async/',
  logFile: '/tmp/pg/async.log',
  dbName: 'test',
  url: URL_3
};
var DAEMON;
var DAEMON2;
var DAEMON3;

var zk;

test('killall postgres instances', function(t) {
  shelljs.mkdir('-p', '/tmp/pg');
  shelljs.rm('-rf', '/tmp/pg/*');
  spawn('killall', ['-KILL', 'postgres']);
  t.end();
});

test('setup-persistent-znode', function(t) {
  zk = new ZooKeeper(ZK_CFG);
  zk.connect(function(err) {
    if (err) {
      t.fail(err);
      t.end();
    }

    zk.a_create('/' + SHARD_ID, null, ZooKeeper.ZOO_PERSISTENT,
                function(rc, error, path) {
      if (rc != 0) {
        t.fail(rc, err);
      }
      log.info('successfully created a znode', path);

      BASE_PATH = path;
      t.end();
    });
  });
});

test('setup-registrar', function(t) {
  // create /REGISTRAR_PATH
  zk.a_create(REGISTRAR_PATH, null, ZooKeeper.ZOO_PERSISTENT,
    function(rc, msg, path) {

    if (rc !== 0) {
      console.log(rc, msg, path);
      t.fail();
      t.end();
    }

    log.info('successfully created a znode', path);
    // create /REGISTRAR_PATH/SHARD_ID
    zk.a_create(REGISTRAR_PATH + '/' + SHARD_ID, null,
                ZooKeeper.ZOO_PERSISTENT, function(rc, msg, path) {
      if (rc !== 0) {
        console.log(rc, msg, path);
        t.fail();
        t.end();
      }
      log.info('succesfully created registrar node', path);
      t.end();
    });
  });
});

test('daemon-init primary', function(t) {
  DAEMON = new Daemon({
    url: URL,
    zkCfg: ZK_CFG,
    shardId: SHARD_ID,
    log: log,
    postgresqlPath: postgresManCfg.dataDir + '/postgresql.conf',
    recoveryPath: postgresManCfg.dataDir + '/recovery.conf',
    registrarPath: REGISTRAR_PATH,
    recoveryTemplate: RECOVERY_TEMPLATE,
    postgresqlTemplate: POSTGRESQL_TEMPLATE,
    postgresCfg: postgresManCfg
  });


  DAEMON.init(function(err) {
    console.log(err);
    if (err) {
      t.fail(err);
      t.end();
    }
    // 0 == primary
    t.equal(DAEMON.mode, 0, 'in primary mode');

    DAEMON.postgresMan.stat(function(stat, err) {
      if (err) {
        t.fail(err);
        t.end();
      }
      t.equal(stat, 0);
      DAEMON.postgresMan.health(function(err) {
        if (err) {
          t.fail(err);
          t.end();
        }
        DAEMON.postgresMan.xlogLocation(function(err) {
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

// for the sake of unit tests, we just copy the data dir over
test('backup secondary', function(t) {
  var primaryPath = postgresManCfg.dataDir + '/*';
  var secondaryPath = postgresManCfg_2.dataDir;
  shelljs.mkdir('-p', secondaryPath);
  log.info('copying primary to secondary', primaryPath, secondaryPath);
  shelljs.cp('-r', primaryPath, secondaryPath);
  shelljs.rm(postgresManCfg_2.dataDir+'/postmaster.pid');
  shelljs.cp('-f', POSTGRESQL_TEMPLATE_2,
    postgresManCfg_2.dataDir + '/postgresql.conf');
  spawn('chmod', ['700', secondaryPath]);
  t.end();
});

test('daemon-init standby', function(t) {
  DAEMON = new Daemon({
    url: URL_2,
    zkCfg: ZK_CFG,
    shardId: SHARD_ID,
    log: log,
    postgresqlPath: postgresManCfg_2.dataDir + '/postgresql.conf',
    recoveryPath: postgresManCfg_2.dataDir + '/recovery.conf',
    registrarPath: REGISTRAR_PATH,
    recoveryTemplate: RECOVERY_TEMPLATE,
    postgresqlTemplate: POSTGRESQL_TEMPLATE_2,
    postgresCfg: postgresManCfg_2
  });


  DAEMON.init(function(err) {
    console.log(err);
    if (err) {
      t.fail(err);
      t.end();
    }
    // 1 == standby
    t.equal(DAEMON.mode, 1, 'in standby mode');

    DAEMON.postgresMan.stat(function(stat, err) {
      if (err) {
        t.fail(err);
        t.end();
      }
      t.equal(stat, 0);
      DAEMON.postgresMan.health(function(err) {
        if (err) {
          t.fail(err);
          t.end();
        }
        DAEMON.postgresMan.xlogReceiveLocation(function(err) {
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

// for the sake of unit tests, we just copy the data dir over
test('backup async', function(t) {
  var primaryPath = postgresManCfg.dataDir + '/*';
  var secondaryPath = postgresManCfg_3.dataDir;
  shelljs.mkdir('-p', secondaryPath);
  log.info('copying primary to secondary', primaryPath, secondaryPath);
  shelljs.cp('-r', primaryPath, secondaryPath);
  shelljs.rm(postgresManCfg_3.dataDir+'/postmaster.pid');
  shelljs.cp('-f', POSTGRESQL_TEMPLATE_3,
    postgresManCfg_3.dataDir + '/postgresql.conf');
  spawn('chmod', ['700', secondaryPath]);
  t.end();
});

test('daemon-init async', function(t) {
  DAEMON = new Daemon({
    url: URL_3,
    zkCfg: ZK_CFG,
    shardId: SHARD_ID,
    log: log,
    postgresqlPath: postgresManCfg_3.dataDir + '/postgresql.conf',
    recoveryPath: postgresManCfg_3.dataDir + '/recovery.conf',
    registrarPath: REGISTRAR_PATH,
    recoveryTemplate: RECOVERY_TEMPLATE,
    postgresqlTemplate: POSTGRESQL_TEMPLATE_3,
    postgresCfg: postgresManCfg_3
  });


  DAEMON.init(function(err) {
    console.log(err);
    if (err) {
      t.fail(err);
      t.end();
    }
    // 1 == standby
    t.equal(DAEMON.mode, 1, 'in standby mode');

    DAEMON.postgresMan.stat(function(stat, err) {
      if (err) {
        t.fail(err);
        t.end();
      }
      t.equal(stat, 0);
      DAEMON.postgresMan.health(function(err) {
        if (err) {
          t.fail(err);
          t.end();
        }
        DAEMON.postgresMan.xlogReceiveLocation(function(err) {
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

tap.tearDown(function() {
  process.exit(tap.output.results.fail);
});
