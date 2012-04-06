var Logger = require('bunyan');
var tap = require('tap');
var common = require('../lib/common');
var test = require('tap').test;
var Registrar = require('../lib/registrar');
var Shard = require('../lib/shard');
var ZooKeeper = require('zookeeper');
var uuid = require('node-uuid');

var SHARD_ID = uuid();

var MEMBERS = 'toto, dorothy, lion';

var ZK_CFG = {
 connect: 'localhost:2181',
 timeout: 200000,
 debug_level: ZooKeeper.ZOO_LOG_LEVEL_WARNING,
 host_order_deterministic: false,
 data_as_buffer: false
};

var URL = 'localhost';
var BASE_PATH;
var REGISTRAR_PATH = '/' + uuid() + 'registrar';
var EPATH;
var ZNODE_TYPE = ZooKeeper.ZOO_SEQUENCE | ZooKeeper.ZOO_EPHEMERAL;
var log = new Logger({
  name: 'scallop-test',
  src: true,
  level: 'trace'
});

var REGISTRAR;

var PRIMARY;
var SYNC;
var ASYNC;

var zk;
var zks = [];
var shards = [];

test('test sortShards', function(t) {
  var shards = ['fdca03ad-6a08-42f8-b611-004a67a51d2b-0000000000',
      'bf8f743d-9771-44d8-ba04-6de1edcb82fa-0000000006',
      'fdca03ad-6a08-42f8-b611-004a67a51d2b-0000000002',
      'fdca03ad-6a08-42f8-b611-004a67a51d2b-0000000001',
      '80f6b369-a73b-41d6-b71a-7945df6bb2c3-0000000004',
      'fdca03ad-6a08-42f8-b611-004a67a51d2b-0000000003',
      '70eae696-e9d5-483e-b0b4-f143c1ecc073-0000000005'];

  var sortedShards = ['fdca03ad-6a08-42f8-b611-004a67a51d2b-0000000003',
      'bf8f743d-9771-44d8-ba04-6de1edcb82fa-0000000006',
      '80f6b369-a73b-41d6-b71a-7945df6bb2c3-0000000004',
      '70eae696-e9d5-483e-b0b4-f143c1ecc073-0000000005'];

  var r = new Registrar(null, null, log);

  r.sortShards(shards, function(shardArray) {
    for (var i = 0; i < 4; i++) {
      t.equals(sortedShards[i], shardArray[i]);
    }
    t.end();
  });
});

test('test sortShards', function(t) {
  var shards = [];

  for (var i = 0; i < 100; i++) {
    for (var j = 1000; j < 1100; j++) {
      shards.push(i + '-' + j);
    }
  }
  var r = new Registrar(null, null, log);

  r.sortShards(shards, function(shardArray) {
    t.equals(100, shardArray.length);
    for (var i = 0; i < 100; i++) {
      t.equals(shardArray[i], i + '-' + 1099);
    }
    t.end();
  });
});

test('setup-shard-base-znode', function(t) {
  zk = new ZooKeeper(ZK_CFG);
  zk.connect(function(err) {
    if (err) {
      t.fail(err);
      t.end();
    }

    zk.a_create('/' + SHARD_ID, null, ZooKeeper.ZOO_PERSISTENT,
                function(rc, msg, path) {

      if (rc !== 0) {
        console.log(rc, msg, path);
        t.fail();
        t.end();
      }

      log.info('successfully created a znode', path);
      BASE_PATH = path;
      t.end();
    });
  });
});

test('setup-registrar-znode', function(t) {
  // create /REGISTRAR_PATH
  zk.a_create(REGISTRAR_PATH, null, ZooKeeper.ZOO_PERSISTENT,
    function(rc, msg, path) {

    if (rc !== 0) {
      console.log(rc, msg, path);
      t.fail();
      t.end();
    }

    log.info('successfully created a registrar base path', path);
    t.end();
  });
});

test('setup-registrar-client', function(t) {
  REGISTRAR = new Registrar(REGISTRAR_PATH, ZK_CFG, log);
  REGISTRAR.once('init', function(map) {
    console.log('registered registrar client');
    t.ok(map);
    t.end();
  });

  REGISTRAR.once('error', function(err) {
    t.fail(err);
    t.end();
  });

  REGISTRAR.init();
});

test('setup-shard', function(t) {
  var count = 0;

  for (i = 0; i < 3; i++) {
    var shard = new Shard(BASE_PATH, REGISTRAR_PATH, ZK_CFG, i, log);
    shards.push(shard);
    shard.once('error', function(err) {
      t.fail();
      t.end();
    });
    shard.once('init', function() {
      count++;
      if (count === 3) {
        //wait a couple seconds for shards to converge.
        setTimeout(function() { t.end(); }, 200);
      }
    });
    shard.init();
  }
});

test('steady state', function(t) {
  shards.forEach(function(shard) {
    switch (shard.role) {
      case 0:
        PRIMARY = shard;
        break;
      case 1:
        SYNC = shard;
        break;
      case 2:
        ASYNC = shard;
        break;
      default:
        t.fail();
        t.end();
        break;
    }
  });
  t.end();
});

test('check registrar', function(t) {
  t.ok(REGISTRAR.shardMap);

  count = 0;
  log.info(REGISTRAR.shardMap);
  for (var key in REGISTRAR.shardMap) {
    count++;
    var shard = REGISTRAR.shardMap[key];
    log.info(shard);
    t.ok(shard.primary);
    t.equals(shard.primary, PRIMARY.primary.url);
    t.ok(shard.sync);
    t.equals(shard.sync, PRIMARY.sync.url);
    t.ok(shard.async);
    t.equals(shard.async, PRIMARY.async.url);
  }

  if (count > 1) {
    t.fail('more than 1 shard in registrar');
    t.end();
  }

  if (count === 1) {
    t.end();
  }
});

test('sync dies', function(t) {
  initCount = 0;

  PRIMARY.once('init', function(shard) {
    initCount++;
    if (initCount == 2) {
      t.end();
    }
  });

  ASYNC.once('init', function(shard) {
    SYNC = shard;
    ASYNC = null;
    initCount++;
    if (initCount == 2) {
      t.end();
    }
  });

  SYNC.disconnect();
});

test('wait for shard to converge', function(t) {
  setTimeout(function() { t.end(); }, 200);
});

test('check registrar', function(t) {
  t.ok(REGISTRAR.shardMap);

  count = 0;
  log.info(REGISTRAR.shardMap);
  for (var key in REGISTRAR.shardMap) {
    count++;
    var shard = REGISTRAR.shardMap[key];
    log.info(shard);
    t.ok(shard.primary);
    t.equals(shard.primary, PRIMARY.primary.url);
    t.ok(shard.sync);
    t.equals(shard.sync, PRIMARY.sync.url);
    t.notOk(shard.async);
  }

  if (count > 1) {
    t.fail('more than 1 shard in registrar');
    t.end();
  }

  if (count === 1) {
    t.end();
  }
});

test('add a peer', function(t) {
  ASYNC = new Shard(BASE_PATH, REGISTRAR_PATH, ZK_CFG, 4, log);
  var initCount = 0;
  PRIMARY.once('init', function(shard) {
    initCount++;
    if (initCount == 3) {
      t.end();
    }
  });

  SYNC.once('init', function(shard) {
    initCount++;
    if (initCount == 3) {
      t.end();
    }
  });

  ASYNC.once('init', function(shard) {
    initCount++;
    if (initCount == 3) {
      t.end();
    }
  });

  ASYNC.init();
});

test('wait for shard to converge', function(t) {
  setTimeout(function() { t.end(); }, 200);
});

test('check registrar', function(t) {
  t.ok(REGISTRAR.shardMap);

  count = 0;
  log.info(REGISTRAR.shardMap);
  for (var key in REGISTRAR.shardMap) {
    count++;
    var shard = REGISTRAR.shardMap[key];
    log.info(shard);
    t.ok(shard.primary);
    t.equals(shard.primary, PRIMARY.primary.url);
    t.ok(shard.sync);
    t.equals(shard.sync, PRIMARY.sync.url);
    t.ok(shard.async);
    t.equals(shard.async, PRIMARY.async.url);
  }

  if (count > 1) {
    t.fail('more than 1 shard in registrar');
    t.end();
  }

  if (count === 1) {
    t.end();
  }
});

test('async dies', function(t) {
  initCount = 0;

  PRIMARY.once('init', function(shard) {
    t.equals(shard.role, 0);
    t.equals(shard.primary.path, PRIMARY.myPath);
    initCount++;
    if (initCount == 2) {
      t.end();
    }
  });

  SYNC.once('init', function(shard) {
    t.equals(shard.role, 1);
    initCount++;
    if (initCount == 2) {
      t.end();
    }
  });

  ASYNC.disconnect();
});

test('wait for shard to converge', function(t) {
  setTimeout(function() { t.end(); }, 200);
});

test('check registrar', function(t) {
  t.ok(REGISTRAR.shardMap);

  count = 0;
  log.info(REGISTRAR.shardMap);
  for (var key in REGISTRAR.shardMap) {
    count++;
    var shard = REGISTRAR.shardMap[key];
    log.info(shard);
    t.ok(shard.primary);
    t.equals(shard.primary, PRIMARY.primary.url);
    t.ok(shard.sync);
    t.equals(shard.sync, PRIMARY.sync.url);
    t.notOk(shard.async);
  }

  if (count > 1) {
    t.fail('more than 1 shard in registrar');
    t.end();
  }

  if (count === 1) {
    t.end();
  }
});

test('setup-shards', function(t) {
  var count = 0;

  for (i = 0; i < 3; i++) {
    var path = '/' + uuid();
    // create the persistent path for the shard
    zk.a_create(path, null, ZooKeeper.ZOO_PERSISTENT, function(rc, msg, path) {
      if (rc !== 0) {
        console.log(rc, msg, path);
        t.fail();
        t.end();
      }
      // create the shard
      var shard = new Shard(path, REGISTRAR_PATH, ZK_CFG, i, log);
      shard.once('error', function(err) {
        log.error(err);
        t.fail(err);
        t.end();
      });
      shard.once('init', function() {
        count++;
        if (count === 3) {
          //wait a couple seconds for shards to converge.
          setTimeout(function() { t.end(); }, 200);
        }
      });
      shard.init();
    });
  }
});

test('check registrar', function(t) {
  t.ok(REGISTRAR.shardMap);

  count = 0;
  log.info(REGISTRAR.shardMap);
  for (var key in REGISTRAR.shardMap) {
    count++;
    var shard = REGISTRAR.shardMap[key];
    log.info(shard);
    t.ok(shard.primary);
  }

  if (count > 4) {
    t.fail('more than 4 shards in registrar');
    t.end();
  }

  if (count === 4) {
    t.end();
  }
});

test('delete a shard', function(t) {
  PRIMARY.disconnect();
  SYNC.disconnect();
  //wait a couple seconds for shards to converge.
  setTimeout(function() { t.end(); }, 200);
});

test('check registrar', function(t) {
  t.ok(REGISTRAR.shardMap);
  count = 0;
  log.info(REGISTRAR.shardMap);
  for (var key in REGISTRAR.shardMap) {
    count++;
    var shard = REGISTRAR.shardMap[key];
    log.info(shard);
    t.ok(shard.primary);
  }

  if (count > 3) {
    t.fail('more than 3 shards in registrar');
    t.end();
  }

  if (count === 3) {
    t.end();
  }
});

tap.tearDown(function() {
  process.exit(tap.output.results.fail);
});
