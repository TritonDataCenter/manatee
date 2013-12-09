/**
 * @overview Server that handles heartbeats from clients.
 * @copyright Copyright (c) 2013, Joyent, Inc. All rights reserved.
 *
 *                   _.---.._
 *      _        _.-' \  \    ''-.
 *    .'  '-,_.-'   /  /  /       '''.
 *   (       _                     o  :
 *    '._ .-'  '-._         \  \-  ---]
 *                  '-.___.-')  )..-'
 *                           (_/
 */
var assert = require('assert-plus');
var BackupQueue = require('./backupQueue');
var BackupSender = require('./backupSender');
var EventEmitter = require('events').EventEmitter;
var restify = require('restify');
var util = require('util');
var uuid = require('node-uuid');

/**
 * REST server that takes manatee heartbeat requests
 * API:
 * POST /heartbeat/
 *
 * @constructor
 * @augments EventEmitter
 *
 * @fires HeartbeatServer#error If there's an error with the server.
 * @fires HeartbeatServer#newStandby If there's a new standby that's joined.
 * @fires HeartbeatServer#expired If a standby is expired.
 *
 * @param {object} options Options object.
 * @param {Bunyan} options.log Bunyan logger.
 * @param {number} options.port Port of the heartbeat server.
 * @param {number} options.expirationTime Time in ms before a heartbeat is
 * expired.
 * @param {number} options.pollInterval  Period in ms used to check heartbeat
 * expiration.
 */
function HeartbeatServer(options) {
    assert.object(options, 'options');
    assert.object(options.log, 'options.log');
    assert.number(options.port, 'options.port');
    assert.number(options.expirationTime, 'options.expirationTime');
    assert.number(options.pollInterval, 'options.pollInterval');

    EventEmitter.call(this);

    /** @type {Bunyan} The bunyan log object */
    this.log = options.log;
    var log = this.log;
    log.trace('new primary server with options', options);

    /** @type {number} server port */
    this.port = options.port;

    /** @type {Restify} rest server */
    this.server = restify.createServer({
        log: log
    });

    /**
     * @type {number}
     * If no heartbeat has been received for this ms, expire the client.
     */
    this.expirationTime = options.expirationTime;

    /**
     * @type {number}
     * The period in ms used to check for heartbeats.
     */
    this.heartbeatPollInterval = options.heartbeatPollInterval;

    /**
     * @type {object}
     * Map of client to last heart beat time.
     */
    this.heartbeat = {};

    var self = this;
    setInterval(function () {
        self._checkHeartbeats();
    }, this.heartbeatPollInterval);
}

module.exports = HeartbeatServer;
util.inherits(HeartbeatServer, EventEmitter);

/**
 * Initialize the heartbeat server.
 */
HeartbeatServer.prototype.init = function init() {
    var self = this;
    var server = this.server;
    var log = self.log;

    server.use(restify.queryParser());
    server.use(restify.bodyParser());

    server.post('/heartbeat/', postHeartbeat);
    server.on('uncaughtException', function (req, res, route, error) {
        res.send(error);
        /**
         * Error event.
         *
         * @event HeartbeatServer#error
         * @type {Error}
         */
        self.emit('error', error);
    });

    server.listen(self.port, function () {
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
            /**
             * New Standby Event. Indicates a new standby has pinged this
             * server.
             *
             * @event HeartbeatServer#newStandby
             * @type {string} the IP address of the standby.
             */
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

/**
 * #@+
 * @private
 * @memberOf HeartbeatServer
 */

HeartbeatServer.prototype._checkHeartbeats = function () {
    var self = this;
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
            /**
             * Expired event. Indicates a stanby has expired.
             *
             * @event HeartbeatServer#expired
             * @type {object} The host object.
             */
            self.emit('expired', host);
        }
    }
};

/** #@- */
