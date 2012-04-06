var Logger = require('bunyan');
var tap = require('tap');
var common = require('../lib/common');
var test = require('tap').test;
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

var PRIMARY;
var SYNC;
var ASYNC;

var zk;
var zks = [];
var shards = [];
test('setup-local', function(t) {

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
      t.end();
    });
  });
});

test('setup-shards', function(t) {
  zk = new ZooKeeper(ZK_CFG);
  var count = 0;

  EPATH = '/' + SHARD_ID + '/shard-';
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
    t.equals(shard.role,
      parseInt(shard.myPath.substring(shard.myPath.length - 1), 10));
    t.equal(shard.primary.path.substring(shard.primary.path.length - 1), '0');
    t.equal(shard.sync.path.substring(shard.sync.path.length - 1), '1');
    t.equal(shard.async.path.substring(shard.async.path.length - 1), '2');

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

test('primary dies', function(t) {
  var initCount = 0;
  SYNC.once('init', function(shard) {
    console.log(SYNC);
    t.equals(shard.role, 0);
    PRIMARY = shard;
    initCount++;
    if (initCount == 2) {
      t.end();
    }
  });

  ASYNC.once('init', function(shard) {
    console.log(ASYNC);
    t.equals(shard.role, 1);
    SYNC = shard;
    ASYNC = null;
    initCount++;
    if (initCount == 2) {
      t.end();
    }
  });

  PRIMARY.disconnect();
});

test('add a peer', function(t) {
  ASYNC = new Shard(BASE_PATH, REGISTRAR_PATH, ZK_CFG, 4, log);
  var initCount = 0;
  PRIMARY.once('init', function(shard) {
    console.log(SYNC);
    t.equals(shard.role, 0);
    t.equals(shard.primary.path, PRIMARY.myPath);
    t.equals(shard.sync.path, SYNC.myPath);
    t.equals(shard.async.path, ASYNC.myPath);
    initCount++;
    if (initCount == 3) {
      t.end();
    }
  });

  SYNC.once('init', function(shard) {
    console.log(ASYNC);
    t.equals(shard.role, 1);
    t.equals(shard.primary.path, PRIMARY.myPath);
    t.equals(shard.sync.path, SYNC.myPath);
    t.equals(shard.async.path, ASYNC.myPath);
    initCount++;
    if (initCount == 3) {
      t.end();
    }
  });

  ASYNC.once('init', function(shard) {
    t.equals(shard.role, 2);
    t.equals(shard.primary.path, PRIMARY.myPath);
    t.equals(shard.sync.path, SYNC.myPath);
    t.equals(shard.async.path, ASYNC.myPath);
    initCount++;
    if (initCount == 3) {
      t.end();
    }
  });

  ASYNC.init();
});

test('sync dies', function(t) {
  initCount = 0;

  PRIMARY.once('init', function(shard) {
    t.equals(shard.role, 0);
    t.equals(shard.primary.path, PRIMARY.myPath);
    initCount++;
    if (initCount == 2) {
      t.end();
    }
  });

  ASYNC.once('init', function(shard) {
    t.equals(shard.role, 1);
    SYNC = shard;
    ASYNC = null;
    initCount++;
    if (initCount == 2) {
      t.end();
    }
  });

  SYNC.disconnect();
});

test('add a peer', function(t) {
  ASYNC = new Shard(BASE_PATH, REGISTRAR_PATH, ZK_CFG, 4, log);
  var initCount = 0;
  PRIMARY.once('init', function(shard) {
    console.log(SYNC);
    t.equals(shard.role, 0);
    t.equals(shard.primary.path, PRIMARY.myPath);
    t.equals(shard.sync.path, SYNC.myPath);
    t.equals(shard.async.path, ASYNC.myPath);
    initCount++;
    if (initCount == 3) {
      t.end();
    }
  });

  SYNC.once('init', function(shard) {
    console.log(ASYNC);
    t.equals(shard.role, 1);
    t.equals(shard.primary.path, PRIMARY.myPath);
    t.equals(shard.sync.path, SYNC.myPath);
    t.equals(shard.async.path, ASYNC.myPath);
    initCount++;
    if (initCount == 3) {
      t.end();
    }
  });

  ASYNC.once('init', function(shard) {
    t.equals(shard.role, 2);
    t.equals(shard.primary.path, PRIMARY.myPath);
    t.equals(shard.sync.path, SYNC.myPath);
    t.equals(shard.async.path, ASYNC.myPath);
    initCount++;
    if (initCount == 3) {
      t.end();
    }
  });

  ASYNC.init();
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

test('add a peer', function(t) {
  ASYNC = new Shard(BASE_PATH, REGISTRAR_PATH, ZK_CFG, 4, log);
  var initCount = 0;
  PRIMARY.once('init', function(shard) {
    console.log(SYNC);
    t.equals(shard.role, 0);
    t.equals(shard.primary.path, PRIMARY.myPath);
    t.equals(shard.sync.path, SYNC.myPath);
    t.equals(shard.async.path, ASYNC.myPath);
    initCount++;
    if (initCount == 3) {
      t.end();
    }
  });

  SYNC.once('init', function(shard) {
    console.log(ASYNC);
    t.equals(shard.role, 1);
    t.equals(shard.primary.path, PRIMARY.myPath);
    t.equals(shard.sync.path, SYNC.myPath);
    t.equals(shard.async.path, ASYNC.myPath);
    initCount++;
    if (initCount == 3) {
      t.end();
    }
  });

  ASYNC.once('init', function(shard) {
    t.equals(shard.role, 2);
    t.equals(shard.primary.path, PRIMARY.myPath);
    t.equals(shard.sync.path, SYNC.myPath);
    t.equals(shard.async.path, ASYNC.myPath);
    initCount++;
    if (initCount == 3) {
      t.end();
    }
  });

  ASYNC.init();
});

test('primary and sync dies', function(t) {
  ASYNC.once('init', function(shard) {
    t.equals(shard.role, 0);
    t.equals(shard.sync, null);
    t.equals(shard.async, null);
    PRIMARY = shard;
    SYNC = null;
    ASYNC = null;
    t.end();
  });

  PRIMARY.disconnect();
  SYNC.disconnect();
});

test('add a peer', function(t) {
  SYNC = new Shard(BASE_PATH, REGISTRAR_PATH, ZK_CFG, 4, log);
  var initCount = 0;
  PRIMARY.once('init', function(shard) {
    console.log(SYNC);
    t.equals(shard.role, 0);
    t.equals(shard.primary.path, PRIMARY.myPath);
    t.equals(shard.sync.path, SYNC.myPath);
    initCount++;
    if (initCount == 2) {
      t.end();
    }
  });

  SYNC.once('init', function(shard) {
    console.log(ASYNC);
    t.equals(shard.role, 1);
    t.equals(shard.primary.path, PRIMARY.myPath);
    t.equals(shard.sync.path, SYNC.myPath);
    initCount++;
    if (initCount == 2) {
      t.end();
    }
  });

  SYNC.init();
});

test('add a peer', function(t) {
  ASYNC = new Shard(BASE_PATH, REGISTRAR_PATH, ZK_CFG, 4, log);
  var initCount = 0;
  PRIMARY.once('init', function(shard) {
    console.log(SYNC);
    t.equals(shard.role, 0);
    t.equals(shard.primary.path, PRIMARY.myPath);
    t.equals(shard.sync.path, SYNC.myPath);
    t.equals(shard.async.path, ASYNC.myPath);
    initCount++;
    if (initCount == 3) {
      t.end();
    }
  });

  SYNC.once('init', function(shard) {
    console.log(ASYNC);
    t.equals(shard.role, 1);
    t.equals(shard.primary.path, PRIMARY.myPath);
    t.equals(shard.sync.path, SYNC.myPath);
    t.equals(shard.async.path, ASYNC.myPath);
    initCount++;
    if (initCount == 3) {
      t.end();
    }
  });

  ASYNC.once('init', function(shard) {
    t.equals(shard.role, 2);
    t.equals(shard.primary.path, PRIMARY.myPath);
    t.equals(shard.sync.path, SYNC.myPath);
    t.equals(shard.async.path, ASYNC.myPath);
    initCount++;
    if (initCount == 3) {
      t.end();
    }
  });

  ASYNC.init();
});

test('primary and async dies', function(t) {
  SYNC.once('init', function(shard) {
    t.equals(shard.role, 0);
    t.equals(shard.sync, null);
    t.equals(shard.async, null);
    PRIMARY = shard;
    SYNC = null;
    ASYNC = null;
    t.end();
  });

  PRIMARY.disconnect();
  ASYNC.disconnect();
});

test('add a peer', function(t) {
  SYNC = new Shard(BASE_PATH, REGISTRAR_PATH, ZK_CFG, 4, log);
  var initCount = 0;
  PRIMARY.once('init', function(shard) {
    console.log(SYNC);
    t.equals(shard.role, 0);
    t.equals(shard.primary.path, PRIMARY.myPath);
    t.equals(shard.sync.path, SYNC.myPath);
    initCount++;
    if (initCount == 2) {
      t.end();
    }
  });

  SYNC.once('init', function(shard) {
    console.log(ASYNC);
    t.equals(shard.role, 1);
    t.equals(shard.primary.path, PRIMARY.myPath);
    t.equals(shard.sync.path, SYNC.myPath);
    initCount++;
    if (initCount == 2) {
      t.end();
    }
  });

  SYNC.init();
});

test('add a peer', function(t) {
  ASYNC = new Shard(BASE_PATH, REGISTRAR_PATH, ZK_CFG, 4, log);
  var initCount = 0;
  PRIMARY.once('init', function(shard) {
    console.log(SYNC);
    t.equals(shard.role, 0);
    t.equals(shard.primary.path, PRIMARY.myPath);
    t.equals(shard.sync.path, SYNC.myPath);
    t.equals(shard.async.path, ASYNC.myPath);
    initCount++;
    if (initCount == 3) {
      t.end();
    }
  });

  SYNC.once('init', function(shard) {
    console.log(ASYNC);
    t.equals(shard.role, 1);
    t.equals(shard.primary.path, PRIMARY.myPath);
    t.equals(shard.sync.path, SYNC.myPath);
    t.equals(shard.async.path, ASYNC.myPath);
    initCount++;
    if (initCount == 3) {
      t.end();
    }
  });

  ASYNC.once('init', function(shard) {
    t.equals(shard.role, 2);
    t.equals(shard.primary.path, PRIMARY.myPath);
    t.equals(shard.sync.path, SYNC.myPath);
    t.equals(shard.async.path, ASYNC.myPath);
    initCount++;
    if (initCount == 3) {
      t.end();
    }
  });

  ASYNC.init();
});

test('sync and async dies', function(t) {
  PRIMARY.once('init', function(shard) {
    t.equals(shard.role, 0);
    t.equals(shard.sync, null);
    t.equals(shard.async, null);
    PRIMARY = shard;
    SYNC = null;
    ASYNC = null;
    t.end();
  });

  SYNC.disconnect();
  ASYNC.disconnect();
});

test('add a peer', function(t) {
  SYNC = new Shard(BASE_PATH, REGISTRAR_PATH, ZK_CFG, 4, log);
  var initCount = 0;
  PRIMARY.once('init', function(shard) {
    console.log(SYNC);
    t.equals(shard.role, 0);
    t.equals(shard.primary.path, PRIMARY.myPath);
    t.equals(shard.sync.path, SYNC.myPath);
    initCount++;
    if (initCount == 2) {
      t.end();
    }
  });

  SYNC.once('init', function(shard) {
    console.log(ASYNC);
    t.equals(shard.role, 1);
    t.equals(shard.primary.path, PRIMARY.myPath);
    t.equals(shard.sync.path, SYNC.myPath);
    initCount++;
    if (initCount == 2) {
      t.end();
    }
  });

  SYNC.init();
});

test('add a peer', function(t) {
  ASYNC = new Shard(BASE_PATH, REGISTRAR_PATH, ZK_CFG, 4, log);
  var initCount = 0;
  PRIMARY.once('init', function(shard) {
    console.log(SYNC);
    t.equals(shard.role, 0);
    t.equals(shard.primary.path, PRIMARY.myPath);
    t.equals(shard.sync.path, SYNC.myPath);
    t.equals(shard.async.path, ASYNC.myPath);
    initCount++;
    if (initCount == 3) {
      t.end();
    }
  });

  SYNC.once('init', function(shard) {
    console.log(ASYNC);
    t.equals(shard.role, 1);
    t.equals(shard.primary.path, PRIMARY.myPath);
    t.equals(shard.sync.path, SYNC.myPath);
    t.equals(shard.async.path, ASYNC.myPath);
    initCount++;
    if (initCount == 3) {
      t.end();
    }
  });

  ASYNC.once('init', function(shard) {
    t.equals(shard.role, 2);
    t.equals(shard.primary.path, PRIMARY.myPath);
    t.equals(shard.sync.path, SYNC.myPath);
    t.equals(shard.async.path, ASYNC.myPath);
    initCount++;
    if (initCount == 3) {
      t.end();
    }
  });

  ASYNC.init();
});

test('add a fourth peer', function(t) {
  var fourth = new Shard(BASE_PATH, REGISTRAR_PATH, ZK_CFG, 'fooo', log);
  var count = 0;
  PRIMARY.on('error', function() {
    count++;
    if (count == 4) {
      t.end();
    }
  });

  SYNC.on('error', function() {
    count++;
    if (count == 4) {
      t.end();
    }
  });

  ASYNC.on('error', function() {
    count++;
    if (count == 4) {
      t.end();
    }
  });

  fourth.on('error', function() {
    count++;
    if (count == 4) {
      t.end();
    }
  });

  fourth.init();
});

tap.tearDown(function() {
  process.exit(tap.output.results.fail);
});
