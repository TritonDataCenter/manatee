// Copyright (c) 2012, Joyent, Inc. All rights reserved.
var HeartbeatServer = require('../lib/heartbeatServer');
var bunyan = require('bunyan');
var restify = require('restify');
var tap = require('tap');
var test = require('tap').test;
var uuid = require('node-uuid');

var log = new bunyan({
    name: 'primaryServer-test',
    src: true,
    level: 'info'
});

var server = new HeartbeatServer({
    log: log,
    port: 12222,
    expirationTime: 10000,
    pollInterval: 1000
});

server.init();

