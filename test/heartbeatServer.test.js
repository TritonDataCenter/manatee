// Copyright (c) 2012, Joyent, Inc. All rights reserved.
var HeartbeatServer = require('../lib/heartbeatServer');
var Logger = require('bunyan');
var tap = require('tap');
var test = require('tap').test;
var uuid = require('node-uuid');

var log = new Logger({
  name: 'primaryServer-test',
  src: true,
  level: 'info'
});

var server = new HeartbeatServer({
  log: log,
  port: 12222
});

server.init();

//test('start server', function(t) {
  //var server = new HeartbeatServer({
    //log: log,
    //port: 8080
  //});
//});
