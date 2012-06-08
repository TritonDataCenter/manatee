var BackupServer = require('../lib/backupServer');
var BackupSender = require('../lib/backupSender');
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

var server = new BackupServer({
  log: log,
  port: 8080
});

var BackupSender = new BackupSender({
  log: log,
  zfsSendPath: '/root/manatee/test/zfs_send',
  dataset: 'zones/a6354a44-6d08-40fb-b4fe-7f58100d6d14/data',
  snapshotDir: '/zones/a6354a44-6d08-40fb-b4fe-7f58100d6d14/data/.zfs/snapshot/',
  queue: server.queue
});


server.init();

//test('start server', function(t) {
  //var server = new BackupServer({
    //log: log,
    //port: 8080
  //});
//});
