var Logger = require('bunyan');
var tap = require('tap');
var test = require('tap').test;
var Shard = require('../lib/shard');
var ZooKeeper = require('zookeeper');
var uuid = require('node-uuid');

var LOG = new Logger({
  name: 'shard-test',
  src: true,
  level: 'trace'
});
var REGISTRAR_PATH = '/' + uuid() + 'registrar';
var SHARD_ID = uuid();
var SHARD_PATH;

var PRIMARY;
var PRIMARY_URL = 'tcp://wizard@localhost:5432/oz'
var SYNC;
var ASYNC;

var ZK;
var ZK_CFG = {
 connect: 'localhost:2181',
 timeout: 200000,
 debug_level: ZooKeeper.ZOO_LOG_LEVEL_WARNING,
 host_order_deterministic: false,
 data_as_buffer: false
};

test('create shard znode', function(t) {
  ZK = new ZooKeeper(ZK_CFG);
  ZK.connect(function(err) {
    if (err) {
      t.fail(err);
      t.end();
    }

    ZK.a_create('/' + SHARD_ID, null, ZooKeeper.ZOO_PERSISTENT,
                function(rc, msg, path)
    {
      if (rc !== 0) {
        console.log(rc, msg, path);
        t.fail();
        t.end();
      }

      LOG.info('successfully created a znode', path);
      SHARD_PATH = path;
      t.end();
    });
  });
});

test('setup-registrar', function(t) {
  // create /REGISTRAR_PATH
  ZK.a_create(REGISTRAR_PATH, null, ZooKeeper.ZOO_PERSISTENT,
    function(rc, msg, path) {

    if (rc !== 0) {
      console.log(rc, msg, path);
      t.fail();
      t.end();
    }
    LOG.info('successfully created a znode', path);
    t.end();
  });
});

test('new peer in empty shard', function(t) {
  PRIMARY = new Shard({
    log: LOG,
    shardPath: SHARD_PATH,
    shardId: SHARD_ID,
    registrarPath: REGISTRAR_PATH + '/' + SHARD_ID,
    registrarPathPrefix: REGISTRAR_PATH,
    zkCfg: ZK_CFG,
    url: 'tcp://wizard@localhost:12345/oz'
  });

  PRIMARY.once('primary', function() {
    t.ok(PRIMARY.standbys);
    t.equal(PRIMARY.standbys.length, 0);
    var shardInfo = {
      primary: PRIMARY.url
    };
    PRIMARY.writeRegistrar(shardInfo, function(err) {
      if (err) {
        t.fail(err);
        t.end();
      }
      t.end();
    });
  });
  PRIMARY.once('err', function(err) {
    t.fail(err);
    t.end();
  });
  PRIMARY.init();
});

test('add another peer to shard', function(t) {
  SYNC = new Shard({
    log: LOG,
    shardPath: SHARD_PATH,
    shardId: SHARD_ID,
    registrarPath: REGISTRAR_PATH + '/' + SHARD_ID,
    registrarPathPrefix: REGISTRAR_PATH,
    zkCfg: ZK_CFG,
    url: 'tcp://dorothy@localhost:12345/oz'
  });

  var count = 0;

  SYNC.once('standby', function() {
    t.ok(SYNC);
    t.ok(SYNC.shardInfo);
    if (++count === 2) {
      t.end();
    }
  });
  SYNC.on('err', function(err) {
    t.fail(err);
    t.end();
  });

  PRIMARY.once('standby-change', function(standbys) {
    t.ok(standbys);
    t.equal(standbys.length, 1);
    t.equal(standbys[0], 'tcp://dorothy@localhost:12345/oz');
    // update the registrar
    var shardInfo = {
      primary: 'tcp://wizard@localhost:12345/oz',
      sync: standbys[0]
    }

    PRIMARY.writeRegistrar(shardInfo, function(err) {
      if (err) {
        t.fail(err);
        t.end();
      }

      t.ok(PRIMARY.shardInfo);
      t.equals(JSON.stringify(PRIMARY.shardInfo), JSON.stringify(shardInfo));
      if (++count === 2) {
        t.end();
      }
    });

  });

  SYNC.init();
});

test('add another peer to the shard', function(t) {
  ASYNC = new Shard({
    log: LOG,
    shardPath: SHARD_PATH,
    shardId: SHARD_ID,
    registrarPath: REGISTRAR_PATH + '/' + SHARD_ID,
    registrarPathPrefix: REGISTRAR_PATH,
    zkCfg: ZK_CFG,
    url: 'tcp://toto@localhost:12345/oz'
  });
  var count = 0;
  ASYNC.once('standby', function() {
    t.ok(ASYNC);
    t.ok(ASYNC.shardInfo);
    if (++count === 2) {
      t.end();
    }
  });
  ASYNC.once('err', function(err) {
    t.fail(err);
    t.end();
  });

  PRIMARY.once('standby-change', function(standbys) {
    t.ok(standbys);
    t.equal(standbys.length, 2);
    t.equal(standbys[1], 'tcp://dorothy@localhost:12345/oz');
    t.equal(standbys[0], 'tcp://toto@localhost:12345/oz');
    // update the registrar
    var shardInfo = {
      primary: 'tcp://wizard@localhost:12345/oz',
      sync: 'tcp://dorothy@localhost:12345/oz',
      async: 'tcp://toto@localhost:12345/oz'
    }

    PRIMARY.writeRegistrar(shardInfo, function(err) {
      if (err) {
        t.fail(err);
        t.end();
      }

      t.ok(PRIMARY.shardInfo);
      t.equals(JSON.stringify(PRIMARY.shardInfo), JSON.stringify(shardInfo));
      if (++count === 2) {
        t.end();
      }
    });
  });

  ASYNC.init();
});

test('primary dies', function(t) {
  SYNC.once('primary', function(standbys) {
    t.ok(standbys);
    t.equal(standbys[0], 'tcp://toto@localhost:12345/oz');
    SYNC.writeRegistrar({
      primary: 'tcp://dorothy@localhost:12345/oz',
      sync: 'tcp://toto@localhost:12345/oz'
    }, function(){});
  });
  ASYNC.once('primary-change', function(shardInfo) {
    t.ok(shardInfo);
    t.equal(shardInfo.primary, 'tcp://dorothy@localhost:12345/oz');
    t.equal(shardInfo.sync, 'tcp://toto@localhost:12345/oz');
    t.end();
  });
  PRIMARY.session.close();
});

test('primary dies', function(t) {
  PRIMARY = ASYNC;
  PRIMARY.once('primary', function(standbys) {
    t.ok(standbys);
    t.equal(standbys.length, 0);
    PRIMARY.writeRegistrar({
      primary: 'tcp://toto@localhost:12345/oz'
    }, function(err){
      if (err) {
        t.fail(err);
        t.end();
      }
      t.end();
    });
  });
  LOG.info('closing sync sessions');
  SYNC.session.close();
});

test('add sync peer back to shard', function(t) {
  ASYNC = null;
  SYNC = new Shard({
    log: LOG,
    shardPath: SHARD_PATH,
    shardId: SHARD_ID,
    registrarPath: REGISTRAR_PATH + '/' + SHARD_ID,
    registrarPathPrefix: REGISTRAR_PATH,
    zkCfg: ZK_CFG,
    url: 'tcp://dorothy@localhost:12345/oz'
  });

  var count = 0;

  SYNC.once('standby', function() {
    t.ok(SYNC);
    t.ok(SYNC.shardInfo);
    if (++count === 2) {
      t.end();
    }
  });
  SYNC.on('err', function(err) {
    t.fail(err);
    t.end();
  });

  PRIMARY.once('standby-change', function(standbys) {
    t.ok(standbys);
    LOG.info(standbys);
    t.equal(standbys.length, 1);
    t.equal(standbys[0], 'tcp://dorothy@localhost:12345/oz');
    // update the registrar
    var shardInfo = {
      primary: 'tcp://toto@localhost:12345/oz',
      sync: 'tcp://dorothy@localhost:12345/oz'
    }

    PRIMARY.writeRegistrar(shardInfo, function(err) {
      if (err) {
        t.fail(err);
        t.end();
      }

      t.ok(PRIMARY.shardInfo);
      t.equals(JSON.stringify(PRIMARY.shardInfo), JSON.stringify(shardInfo));
      if (++count === 2) {
        t.end();
      }
    });

  });

  SYNC.init();
});

test('add async peer back to the shard', function(t) {
  ASYNC = new Shard({
    log: LOG,
    shardPath: SHARD_PATH,
    shardId: SHARD_ID,
    registrarPath: REGISTRAR_PATH + '/' + SHARD_ID,
    registrarPathPrefix: REGISTRAR_PATH,
    zkCfg: ZK_CFG,
    url: 'tcp://wizard@localhost:12345/oz'
  });
  var count = 0;
  ASYNC.once('standby', function() {
    t.ok(ASYNC);
    t.ok(ASYNC.shardInfo);
    if (++count === 2) {
      t.end();
    }
  });
  ASYNC.once('err', function(err) {
    t.fail(err);
    t.end();
  });

  PRIMARY.once('standby-change', function(standbys) {
    t.ok(standbys);
    t.equal(standbys.length, 2);
    LOG.info(standbys);
    t.equal(standbys[1], 'tcp://dorothy@localhost:12345/oz');
    t.equal(standbys[0], 'tcp://wizard@localhost:12345/oz');
    // update the registrar
    var shardInfo = {
      async: 'tcp://wizard@localhost:12345/oz',
      sync: 'tcp://dorothy@localhost:12345/oz',
      primary: 'tcp://toto@localhost:12345/oz'
    }

    PRIMARY.writeRegistrar(shardInfo, function(err) {
      if (err) {
        t.fail(err);
        t.end();
      }

      t.ok(PRIMARY.shardInfo);
      t.equals(JSON.stringify(PRIMARY.shardInfo), JSON.stringify(shardInfo));
      if (++count === 2) {
        t.end();
      }
    });
  });

  ASYNC.init();
});

tap.tearDown(function() {
  process.exit(tap.output.results.fail);
});
