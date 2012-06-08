var BackupClient = require('../lib/backupClient');
var Logger = require('bunyan');
var tap = require('tap');
var common = require('../lib/common');
var test = require('tap').test;
var Shard = require('../lib/shard');
var ZooKeeper = require('zookeeper');
var uuid = require('node-uuid');

var log = new Logger({
  name: 'backupServer-test',
  src: true,
  level: 'trace'
});

var backupClient = new BackupClient({
  log: log,
  dataset: 'zones/2bb193e3-4c59-4e25-817a-23ce40abf8d5/data',
  serverUrl: 'http://10.99.99.17:8080',
  snapshotDir: '/zones/2bb193e3-4c59-4e25-817a-23ce40abf8d5/data/.zfs/snapshot',
  zfsHost: '10.99.99.19',
  zfsPort: 1234,
  pollInterval: 500,
  zfsRecvPath: './zfs_recv'
});

backupClient.restore(function(err) {
  log.info('finished backup', err);
});
