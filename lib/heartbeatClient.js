// Copyright (c) 2012, Joyent, Inc. All rights reserved.
var assert = require('assert-plus');
var EventEmitter = require('events').EventEmitter;
var restify = require('restify');
var shelljs = require('shelljs');
var spawn = require('child_process').spawn;
var util = require('util');

function HeartbeatClient(options) {
        assert.object(options, 'options');
        assert.object(options.log, 'options.log');
        assert.string(options.url, 'options.url');
        assert.string(options.primaryUrl, 'options.primaryUrl');
        assert.number(options.heartbeatInterval, 'options.heartbeatInterval');

        EventEmitter.call(this);

        var self = this;

        this.log = options.log;

        /**
        * The primary server's url. http://10.0.0.0:1234
        */
        this.primaryUrl = options.primaryUrl;

        this.url = options.url;

        this.client = restify.createJsonClient({
                url: self.primaryUrl,
                version: '*',
                log: options.log
        });

        this.heartbeatInterval = self.heartbeatInterval;
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
        var request = {
                host: self.url
        };

        self.log.trace({
                request: request
        }, 'sending heartbeat request');

        self.client.post('/heartbeat', request, function(err, req, res, obj) {
                if (err) {
                        log.trace({err: err},
                               'error posting heartbeat request');
                        return callback(err);
                }

                self.log.trace({
                        response: obj
                }, 'successfully posted heartbeat request');

                return callback(null);
        });
        return true;
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
                }, self._heartbeatInterval);
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
