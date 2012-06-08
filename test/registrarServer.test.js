var Logger = require('bunyan');
var RegistrarServer = require('../lib/registrarServer');
var tap = require('tap');
var test = require('tap').test;
var uuid = require('node-uuid');
var ZooKeeper = require('zookeeper');

var numberOfInitialShards = 3;

var REGISTRAR_PATH = '/' + uuid() + 'registrar';

var REGISTRAR_SERVER;

var LOG = new Logger({
  name: 'registrarServer-test',
  src: true,
  level: 'trace'
});

var SHARDS = [];

var ZK;

var ZK_2;

var ZK_CFG = {
 connect: 'localhost:2181',
 timeout: 200000,
 debug_level: ZooKeeper.ZOO_LOG_LEVEL_WARNING,
 host_order_deterministic: false,
 data_as_buffer: false
};

test('create a registrar server without registrar znode', function(t) {
  var registrar = new RegistrarServer({
    log: LOG,
    port: 12345,
    zkCfg: ZK_CFG,
    registrarPath: REGISTRAR_PATH
  });

  registrar.once('err', function(err) {
    t.ok(err);
    t.equal(err.rc, -101);
    t.end();
  });

  registrar.once('update', function(shards) {
    t.fail('should not update when no znode is created');
    t.end()
  });

  registrar.init();
});

test('setup-registrar-znode', function(t) {
  LOG.info('setting up ZK');
  ZK = new ZooKeeper(ZK_CFG);
  ZK.connect(function(err) {
    if (err) {
      t.fail(err);
      t.end();
    }

    ZK.a_create(REGISTRAR_PATH, null, ZooKeeper.ZOO_PERSISTENT,
      function(rc, msg, path)
    {
      LOG.info('creating registrar path');

      if (rc !== 0) {
        console.log(rc, msg, path);
        t.fail();
        t.end();
      }

      LOG.info('successfully created a registrar base path', path);
      t.end();
    });
  });
});

test('create a registrar server with no shards', function(t) {
  var registrar = new RegistrarServer({
    log: LOG,
    port: 12345,
    zkCfg: ZK_CFG,
    registrarPath: REGISTRAR_PATH
  });

  registrar.once('err', function(err) {
    t.fail(err);
    t.end();
  });

  registrar.once('update', function(shards) {
    t.ok(shards);
    t.equal(Object.keys(shards).length, 0);
    t.end();
  });

  registrar.init();
});

test('put-some-shards-into-the-registrar', function(t) {
  var completed = 0;

  for (var i = 0; i < numberOfInitialShards; i++) {
    var shard = {
      primary: 'dorothy',
      sync: 'tin-man',
      async: 'toto'
    };

    SHARDS[i] = {
      shardId: uuid(),
      data: JSON.stringify(shard)
    }

    ZK.a_create(REGISTRAR_PATH + '/' + SHARDS[i].shardId,
      JSON.stringify(shard), ZooKeeper.ZOO_EPHEMERAL, function(rc, msg, path)
    {
      if (rc !== 0) {
        t.fail();
        t.end();
      }

      completed++;

      if (completed === numberOfInitialShards) {
        t.end();
      }
    });
  }
});

test('create the registrar server', function(t) {
  REGISTRAR_SERVER = new RegistrarServer({
    log: LOG,
    port: 1234,
    zkCfg: ZK_CFG,
    registrarPath: REGISTRAR_PATH
  });

  REGISTRAR_SERVER.once('err', function(err) {
    t.fail(err);
    t.end();
  });

  REGISTRAR_SERVER.once('update', function(shards) {
    t.ok(shards);
    SHARDS.forEach(function(shard) {
      t.ok(shards[shard.shardId]);
    });
    t.end();
  });

  REGISTRAR_SERVER.init();
});

test('add a shard to the registrar', function(t) {
  var shardId = uuid();
  var data = JSON.stringify({
    primary: 'wicked-witch-of-the-west'
  });

  SHARDS[numberOfInitialShards + 1] = {
    shardId: shardId,
    data: data
  };

  REGISTRAR_SERVER.once('err', function(err) {
    t.fail(err);
    t.end();
  });

  REGISTRAR_SERVER.once('update', function(shards) {
    t.ok(shards);
    t.equal(Object.keys(shards).length, 4);
    t.equal(JSON.stringify(shards[shardId]), data);
    t.end();
  });

  ZK_2 = new ZooKeeper(ZK_CFG);
  ZK_2.connect(function(err) {
    if (err) {
      t.fail(err);
      t.end();
    }

    ZK_2.a_create(REGISTRAR_PATH + '/' + shardId, data, ZooKeeper.ZOO_EPHEMERAL,
      function(rc, msg, path)
    {
      if (rc !== 0) {
        t.fail();
        t.end();
      }
    });
  });
});

test('change a shard in registrar', function(t) {
  REGISTRAR_SERVER.once('err', function(err) {
    t.fail(err);
    t.end();
  });

  REGISTRAR_SERVER.once('update', function(shards) {
    t.ok(shards);
    t.equal(Object.keys(shards).length, 4);
    t.equal(JSON.stringify(shards[shardId]), data);
    t.end();
  });

  var shardId = SHARDS[0].shardId;
  var data = JSON.stringify({
    primary: 'the wizard',
    sync: 'glinda',
    async: 'scarecrow'
  });

  LOG.info('changing shardId', shardId);
  ZK.a_set(REGISTRAR_PATH + '/' + shardId, data, -1, function(rc, msg, stat) {
    LOG.info('got response', rc, msg, stat);
    if (rc !== 0) {
      t.fail();
      t.end();
    }
  });
});

/**
 * Add a timeout such that the cluster can settle after the change of a shard
 */
test('timeout', function(t) {
  setTimeout(function(){ t.end(); }, 500);
});

test('remove a shard from the registrar', function(t) {
  REGISTRAR_SERVER.once('err', function(err) {
    t.fail(err);
    t.end();
  });

  REGISTRAR_SERVER.once('update', function(shards) {
    t.ok(shards);
    t.equal(Object.keys(shards).length, 3);
    var deletedShardId = SHARDS[numberOfInitialShards + 1];
    t.notok(shards[deletedShardId]);
    t.end();
  });

  // disconnect should remove the shard from the registrar
  ZK_2.close();
});

tap.tearDown(function() {
  process.exit(tap.output.results.fail);
});
