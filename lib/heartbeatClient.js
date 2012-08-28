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

        this.log.info('initializing HeartbeatClient with options', options);
}

module.exports = HeartbeatClient;
util.inherits(HeartbeatClient, EventEmitter);

/**
* Posts a heartbeat request to the primary peer in the shard
*/
HeartbeatClient.prototype.postHeartbeat = function postHeartbeat(callback) {
        var self = this;
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
};
