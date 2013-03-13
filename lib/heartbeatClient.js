// Copyright (c) 2012, Joyent, Inc. All rights reserved.
var EventEmitter = require('events').EventEmitter;
var assert = require('assert-plus');
var http = require('http');
var once = require('once');
var restify = require('restify');
var shelljs = require('shelljs');
var spawn = require('child_process').spawn;
var url = require('url');
var util = require('util');


// MANATEE-83 set maxSockets so node gets out of restify's way
http.globalAgent.maxSockets = 500;

function HeartbeatClient(options) {
    assert.object(options, 'options');
    assert.object(options.log, 'options.log');
    assert.string(options.url, 'options.url');
    assert.string(options.primaryUrl, 'options.primaryUrl');
    assert.number(options.heartbeatInterval, 'options.heartbeatInterval');
    assert.number(options.connectTimeout, 'options.connectTimeout');
    assert.number(options.requestTimeout, 'options.requestTimeout');
    assert.object(options.retry, 'options.retry');

    EventEmitter.call(this);

    var self = this;

    this.log = options.log.child({component: 'HeartbeatClient'}, true);

    /**
     * The primary server's url. http://10.0.0.0:1234
     */
    this.primaryUrl = options.primaryUrl;

    this.url = options.url;

    this.requestTimeout = options.requestTimeout;

    this.retry = options.retry;

    var parsedUrl = url.parse(self.primaryUrl);
    if (!parsedUrl.host || !parsedUrl.port) {
        throw new Error('primaryUrl ' + self.primaryUrl +
                        ' does not contain host or port');
    }

    this.client = restify.createClient({
        log: self.log.child({component: 'HeartbeatClient'}, true),
        url: self.primaryUrl,
        retry: self.retry,
        connectTimeout: options.connectTimeout,
        headers: {
            accept: 'text/plain'
        }
    });

    this.heartbeatInterval = options.heartbeatInterval;
    this.intervalId = null;

    this.log.trace('initializing HeartbeatClient with options', options);
}

module.exports = HeartbeatClient;
util.inherits(HeartbeatClient, EventEmitter);

/**
 * Posts a heartbeat request to the primary peer in the shard
 */
function _postHeartbeat(self, callback) {
    var log = self.log;
    var opts = {
        path: '/heartbeat',
        headers: {
            'content-type': 'application/x-www-form-urlencoded'
        }
    };
    callback = once(callback);

    log.trace({opts: opts}, 'sending heartbeat request');

    self.client.post(opts, function (connectErr, req) {
        log.trace({err: connectErr, req: req}, 'returned from connect');
        if (connectErr) {
            self.log.warn({err: connectErr}, 'unable to connect to leader');
            return callback(connectErr);
        }

        // MANATEE-83 set timeout in case requests get black-holed.
        var timeoutId = setTimeout(function() {
            log.warn({req: req}, 'request timed out');
            req.abort();
            return callback(new Error('request timed out'));
        }, self.requestTimeout);

        req.end('host=' + self.url);

        req.on('result', function (err, res) {
            log.trace({err: err, res: res}, 'got heartbeatResult');
            clearTimeout(timeoutId);
            if (err) {
                log.warn({err: err}, 'unable to post request to leader');
                return callback(err);
            }

            res.once('end', function () {
                return callback();
            });
            return (undefined);
        });
        return (undefined);
    });

    return (undefined);
}

HeartbeatClient.prototype.start = function start(callback) {
    var self = this;
    var log = self.log;
    log.trace({
        url: self.primaryUrl,
        intervalId: self.intervalId
    }, 'starting heartbeat');

    if (self.intervalId !== null) {
        log.warn({
            url: self.primaryUrl,
            intervalId: self.intervalId
        }, 'heartbeat is already running');
    } else {
        self.intervalId = setInterval(function() {
            _postHeartbeat(self, function() {});
        }, self.heartbeatInterval);
    }

    return callback();
};

HeartbeatClient.prototype.stop = function stop(callback) {
    var self = this;
    var log = self.log;

    log.trace({
        url: self.primaryUrl,
        intervalId: self.intervalId
    }, 'stopping heartbeat');

    if (self.intervalId) {
        clearInterval(self.intervalId);
        self.intervalId = null;
    } else {
        log.warn({
            url: self.primaryUrl
        }, 'heartbeat is already stopped');
    }

    if (typeof (callback) === 'function') {
        return callback();
    }

    return true;
};
