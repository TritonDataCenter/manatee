// Copyright (c) 2012, Joyent, Inc. All rights reserved.
var HeartbeatClient = require('../lib/heartbeatClient');
var HeartbeatServer = require('../lib/heartbeatServer');
var Logger = require('bunyan');
var tap = require('tap');
var test = require('tap').test;
var uuid = require('node-uuid');

var log = new Logger({
        name: 'heartbeat-test',
        src: true,
        level: 'info'
});

var server = new HeartbeatServer({
  log: log,
  port: 12222
});

server.init();

var heartbeatClient = new HeartbeatClient({
        log: log,
        heartbeatInterval: 200,
        url: 'http://foo/baz',
        primaryUrl: 'http://0.0.0.0:12222'
});

setInterval(function () {
        heartbeatClient.postHeartbeat(function (err) {});
}, 300);
