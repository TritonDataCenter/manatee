// Copyright (c) 2012, Joyent, Inc. All rights reserved.
var Logger = require('bunyan');
var tap = require('tap');
var test = require('tap').test;
var Peer = require('../lib/peer').Peer;
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
var PATH;
var ZNODE_TYPE = ZooKeeper.ZOO_SEQUENTIAL | ZooKeeper.ZOO_EPHEMERAL;
var log = new Logger({
  name: 'scallop-test',
  src: true,
  level: 'trace'
});

var zk;

test('setup-local', function(t) {
  log.info('test');
  zk = new ZooKeeper(ZK_CFG);
  zk.connect(function(err) {
    if (err) {
      t.fail(err);
    }

    zk.a_create('/' + SHARD_ID, URL, ZNODE_TYPE,
                function(rc, error, path) {

      console.info(rc, error, path);
      if (rc != 0) {
        t.fail(rc, err);
      }

      console.info('successfully created a znode', path);
      PATH = path;
      t.end();
    });
  });
});

test('peer', function(t) {
  console.log('in peer test', PATH);
  var p = new Peer(PATH, zk, log, function(err) {
    if (err) {
      t.fail(err);
      t.end();
    }
    t.equal(p.url, URL);
    t.end();
  });
});

tap.tearDown(function() {
  process.exit(tap.output.results.fail);

});
