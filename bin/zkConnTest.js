#!/usr/bin/env node
// -*- mode: js -*-
/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2014, Joyent, Inc.
 */
var once = require('once');
var vasync = require('vasync');
var zkClient = require('joyent-zookeeper-client');

var connStr = process.argv[2];
var timeout = process.argv[3] ? parseInt(process.argv[3], 10) : 0;

if (!connStr) {
    console.error('usage: ' + process.argv.join(' ') + ' <zk conn string> ' +
                  '[shutdown in seconds]');
    process.exit(1);
}

console.log('Connecting to: ' + connStr);

var opts = {
    "sessionTimeout": 60000,
    "spinDelay": 1000,
    "retries": 60
}

var zk = zkClient.createClient(connStr, opts);

//Creator says this is "Java Style"
zk.on('state', function (s) {
    //Just log it.  The other events are called.
    console.log(s, 'zk: new state (' + zk.getState().getName() + ')');
});

//Client is connected and ready. This fires whenever the client is
// disconnected and reconnected (more than just the first time).
zk.on('connected', function () {
    console.log(zk.getSessionId(), 'zk: connected');
});

//Client is connected to a readonly server.
zk.on('connectedReadOnly', function () {
    console.log('zk: connected read only');
});

//The connection between client and server is dropped.
zk.on('disconnected', function () {
    console.log('zk: disconnected');
});

//The client session is expired.
zk.on('expired', function () {
    console.log('zk: session expired, reiniting.');
});

//Failed to authenticate with the server.
zk.on('authenticationFailed', function () {
    console.log('zk: auth failed');
});

//Not even sure if this is really an error that would be emitted...
zk.on('error', function (err) {
    console.log({err: err}, 'zk: unexpected error, reiniting');
});

zk.connect();

if (timeout !== 0) {
    console.log('zk: closing zk in ' + timeout + ' seconds');
    setTimeout(function () {
        console.log('zk: closing zk');
        zk.close();
    }, timeout * 1000);
}
