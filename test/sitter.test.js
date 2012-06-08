var Sitter = require('../lib/sitter');
var Logger = require('bunyan');
var uuid = require('node-uuid');
var ZooKeeper = require('zookeeper');

var SHARD_PATH = '/shard/1';
var REGISTRAR_PATH = '/registrar';
var SHARD_ID = '1';
var MY_URL = 'tcp://yunong@10.99.99.19:5432/test';
var DATA_DIR = '/zones/2bb193e3-4c59-4e25-817a-23ce40abf8d5/data/yunong/';
var SNAPSHOT_DIR =
  '/zones/2bb193e3-4c59-4e25-817a-23ce40abf8d5/data/.zfs/snapshot';
var DATA_SET = 'zones/2bb193e3-4c59-4e25-817a-23ce40abf8d5/data';
var MY_IP = '10.99.99.19';

var ZK_CFG = {
 connect: '10.99.99.17:2181',
 timeout: 200,
 debug_level: ZooKeeper.ZOO_LOG_LEVEL_WARNING,
 host_order_deterministic: false,
 data_as_buffer: false
};

var LOG = new Logger({
  name: 'sitter-test',
  src: true,
  level: 'trace'
});

var postgresManCfg = {
 log: LOG,
 pgCtlPath: 'pg_ctl',
 pgCreateDbPath: 'createdb',
 pgInitDbPath: 'initdb',
 hbaConf: './test_conf/pg_hba.conf',
 postgresConf: './test_conf/postgresql.conf',
 recoveryConf: './test_conf/recovery.conf',
 dbName: 'test',
 dbUser: 'yunong',
 dataDir: DATA_DIR,
 logFile: '/tmp/' + uuid(),
 url: MY_URL,
 backupClientCfg:{
   log: LOG,
   dataset: DATA_SET,
   snapshotDir: SNAPSHOT_DIR,
   zfsHost: MY_IP,
   zfsPort: 1234,
   pollInterval: 100,
   zfsRecvPath: './zfs_recv'
 }
};

var shardCfg = {
  log: LOG,
  shardPath: SHARD_PATH,
  shardId: SHARD_ID,
  registrarPath: REGISTRAR_PATH + '/' + SHARD_ID,
  registrarPathPrefix: REGISTRAR_PATH,
  zkCfg: ZK_CFG,
  url: MY_URL,
};

var sitter = new Sitter({
  log: LOG,
  backupUrl: 'http://' + MY_URL + ':8080',
  shardCfg: shardCfg,
  postgresManCfg: postgresManCfg
});

sitter.init();
