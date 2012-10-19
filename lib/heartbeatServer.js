// Copyright (c) 2012, Joyent, Inc. All rights reserved.
var assert = require('assert-plus');
var BackupQueue = require('./backupQueue');
var BackupSender = require('./backupSender');
var EventEmitter = require('events').EventEmitter;
var restify = require('restify');
var util = require('util');
var uuid = require('node-uuid');

/**
* REST server that takes postgres heartbeat requests
* API:
* POST /heartbeat/
*/
function HeartbeatServer(options) {
        assert.object(options, 'options');
        assert.object(options.log, 'options.log');
        assert.number(options.port, 'options.port');
        if (options.expirationTime) {
                assert.number(options.expirationTime, 'options.expirationTime');
        }
        if (options.heartbeatPollInterval) {
                assert.number(options.heartbeatPollInterval,
                              'options.heartbeatPollInterval');
        }

        EventEmitter.call(this);

        this.log = options.log;
        var log = this.log;
        log.info('new primary server with options', options);

        this.port = options.port;
        this.server = restify.createServer({
                log: log
        });

        this.expirationTime = options.expirationTime || 1 * 1000;

        this.heartbeatPollInterval = options.heartbeatPollInterval || 200;

        this.heartbeat = {};

        var self = this;
        setInterval(function(){
                _checkHeartbeats(self);
        }, this.heartbeatPollInterval);
}

module.exports = HeartbeatServer;
util.inherits(HeartbeatServer, EventEmitter);

HeartbeatServer.prototype.init = function init() {
        var self = this;
        var server = this.server;
        var log = self.log;

        server.use(restify.queryParser());
        server.use(restify.bodyParser());

        server.post('/heartbeat/', postHeartbeat);
        server.on('uncaughtException', function(req, res, route, error) {
                res.send(error);
                self.emit('error', error);
        });

        server.listen(self.port, function() {
                log.info('backup server started');
        });

        function postHeartbeat(req, res, next) {
                var params = req.params;
                if (!params.host) {
                        return next(new restify.MissingParameterError(
                                'host parameter required'));
                }

                if (!self.heartbeat[params.host]) {
                        self.heartbeat = {};
                        self.heartbeat[params.host] = Date.now();
                        log.info({
                                host: params.host,
                                heartbeat: self.heartbeat,
                                time: Date.now()
                        }, 'HeartbeatServer: got new standby server');
                        self.emit('newStandby', params.host);
                } else {
                        log.trace({
                                host: params.host,
                                heartbeat: self.heartbeat,
                                time: Date.now()
                        }, 'HeartbeatServer: heartbeat received');
                        self.heartbeat[params.host] = Date.now();
                }

                res.send();
                return next();
        }
};

function _checkHeartbeats(self) {
        var log = self.log;
        var currTime = Date.now();

        for (var host in self.heartbeat) {
                var hostTime = parseInt(self.heartbeat[host], 10);
                if (currTime - hostTime >= self.expirationTime) {
                        log.info({
                                host: host,
                                hostTime: hostTime,
                                currTime: currTime,
                                diff: (currTime - hostTime)
                        }, 'removing expired host');

                        delete self.heartbeat[host];
                        self.emit('expired', host);
                }
        }
}

