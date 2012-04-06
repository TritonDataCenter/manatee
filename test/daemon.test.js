var Logger = require('bunyan');
var tap = require('tap');
var common = require('../lib/common');
var test = require('tap').test;
var Shard = require('../lib/shard');
var ZooKeeper = require('zookeeper');
var uuid = require('node-uuid');
var Daemon = require('../lib/daemon');
var confparser = require('../lib/confParser');
var fs = require('fs'),
    mkdirOrig = fs.mkdir,
    mkdirSyncOrig = fs.mkdirSync,
    osSep = process.platform === 'win32' ? '\\' : '/';

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

var URL = 'localhost:5432';

var BASE_PATH;

var CONFIG_PATH = '/tmp/' + uuid() + '/';

var RECOVERY_PATH = CONFIG_PATH + 'recovery.conf';

var POSTGRESQL_PATH = CONFIG_PATH + 'postgresql.conf';

var RECOVERY_TEMPLATE = './test_conf/recovery.conf';

var POSTGRESQL_TEMPLATE = './test_conf/postgresql.conf';

var DAEMON = new Daemon({
  url: URL,
  zkCfg: ZK_CFG,
  shardId: SHARD_ID,
  registrarPath: REGISTRAR_PATH,
  log: log,
  configPath: CONFIG_PATH,
  recoveryPath: RECOVERY_PATH,
  postgresqlPath: POSTGRESQL_PATH,
  recoveryTemplate: RECOVERY_TEMPLATE,
  postgresqlTemplate: POSTGRESQL_TEMPLATE
});

var DAEMON2;

var DAEMON3;

var zk;

function mkdir_p(path, mode, callback, position) {
  var parts = require('path').normalize(path).split(osSep);

  mode = mode || process.umask();
  position = position || 0;

  if (position >= parts.length) {
    return callback();
  }

  var directory = parts.slice(0, position + 1).join(osSep) || osSep;
  fs.stat(directory, function(err) {
    if (err === null) {
      mkdir_p(path, mode, callback, position + 1);
    } else {
      mkdirOrig(directory, mode, function(err) {
        if (err && err.errno != 17) {
          return callback(err);
        } else {
          mkdir_p(path, mode, callback, position + 1);
        }
      });
    }
  });
}

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

test('setup directories', function(t) {
  mkdir_p(CONFIG_PATH, 0777, function(err) {
    if (err) {
      t.fail(err);
      t.end();
    }
    t.end();
  });
});

test('daemon-primary', function(t) {
  DAEMON.shard = {
    sync: {
      url: 'sync'
    },
    async: {
      url: 'async'
    }
  };

  DAEMON.primary(function(err) {
    if (err) {
      t.fail(err);
      t.end();
    }
    confparser.read(POSTGRESQL_PATH, function(err, conf) {
      if (err) {
        t.fail(err);
        t.end();
      }
      t.equals(conf.synchronous_standby_names, '\'sync, async\'');
      t.end();
    });
  });
});

test('daemon-primary only peer, go to readonly mode', function(t) {
  DAEMON.shard = {};

  DAEMON.primary(function(err) {
    if (err) {
      t.fail(err);
      t.end();
    }
    confparser.read(POSTGRESQL_PATH, function(err, conf) {
      if (err) {
        t.fail(err);
        t.end();
      }
      t.equals(conf.default_transaction_read_only, 'on');
      t.end();
    });
  });

});

test('daemon-standby', function(t) {
  DAEMON.shard = {
    primary: {url: 'postgresql://1.1.1.1:5432'}
  };

  DAEMON.appName = 'sync';
  DAEMON.user = 'yunong';
  DAEMON.standby(function(err) {
    if (err) {
      t.fail(err);
      t.end();
    }
    confparser.read(RECOVERY_PATH, function(err, conf) {
      if (err) {
        t.fail(err);
        t.end();
      }
      t.equals(conf.primary_conninfo, '\'host=1.1.1.1 port=5432 user=yunong ' +
               'application_name=sync\'');
      log.info('ending standby test');
      DAEMON = null;
      t.end();
    });
  });
});

test('daemon-init readonly', function(t) {
  DAEMON = new Daemon({
    url: URL,
    zkCfg: ZK_CFG,
    shardId: SHARD_ID,
    log: log,
    configPath: CONFIG_PATH,
    recoveryPath: RECOVERY_PATH,
    registrarPath: REGISTRAR_PATH,
    postgresqlPath: POSTGRESQL_PATH,
    recoveryTemplate: RECOVERY_TEMPLATE,
    postgresqlTemplate: POSTGRESQL_TEMPLATE
  });

  DAEMON.init(function(err) {
    console.log(err);
    if (err) {
      t.fail(err);
      t.end();
    }
    // 2 == readonly
    t.equal(DAEMON.mode, 2);
    t.end();
  });
});

test('daemon-init standby', function(t) {
  var cfgPath = '/tmp/' + uuid() + '/';
  var daemon = 0;

  mkdir_p(cfgPath, 0777, function(err) {
    if (err) {
      t.fail(err);
      t.end();
    }
    DAEMON2 = new Daemon({
      url: URL,
      zkCfg: ZK_CFG,
      shardId: SHARD_ID,
      registrarPath: REGISTRAR_PATH,
      log: log,
      configPath: cfgPath,
      recoveryPath: cfgPath + 'recovery.conf',
      postgresqlPath: cfgPath + 'postgresql.conf',
      recoveryTemplate: RECOVERY_TEMPLATE,
      postgresqlTemplate: POSTGRESQL_TEMPLATE
    });

    DAEMON2.init(function(err) {
      if (err) {
        t.fail(err);
        t.end();
      }
      // 1 == standby
      t.equal(DAEMON2.mode, 1);
      daemon++;
      if (daemon === 2) {
        t.end();
      }
    });
  });

  DAEMON.shard.once('init', function(shard) {
    console.log(shard);
    // 0 == primary
    t.equal(DAEMON.mode, 0);
    daemon++;
    if (daemon === 2) {
      t.end();
    }
  });
});

test('daemon-init async', function(t) {
  var cfgPath = '/tmp/' + uuid() + '/';
  var daemon = 0;
  mkdir_p(cfgPath, 0777, function(err) {
    if (err) {
      t.fail(err);
      t.end();
    }
    DAEMON3 = new Daemon({
      url: URL,
      zkCfg: ZK_CFG,
      shardId: SHARD_ID,
      registrarPath: REGISTRAR_PATH,
      log: log,
      configPath: cfgPath,
      recoveryPath: cfgPath + 'recovery.conf',
      postgresqlPath: cfgPath + 'postgresql.conf',
      recoveryTemplate: RECOVERY_TEMPLATE,
      postgresqlTemplate: POSTGRESQL_TEMPLATE
    });

    DAEMON3.init(function(err) {
      if (err) {
        t.fail(err);
        t.end();
      }
      // 1 == standby
      daemon++;
      t.equal(DAEMON3.mode, 1);
      // 2 == async
      t.equal(DAEMON3.shard.role, 2);
      if (daemon === 3) {
        t.end();
      }
    });
  });

  DAEMON2.shard.once('init', function() {
    // 1 == standby
    t.equal(DAEMON2.mode, 1);
    daemon++;
    if (daemon === 3) {
      t.end();
    }
  });

  DAEMON.shard.once('init', function() {
    // 0 == primary
    t.equal(DAEMON.mode, 0);
    daemon++;
    if (daemon === 3) {
      t.end();
    }
  });
});

tap.tearDown(function() {
  process.exit(tap.output.results.fail);
});
