/**
 * @overview Heartbeat client.
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
var http = require('http');
var once = require('once');
var restify = require('restify');
var shelljs = require('shelljs');
var spawn = require('child_process').spawn;
var url = require('url');
var util = require('util');
var verror = require('verror');


// MANATEE-83 set maxSockets so node gets out of restify's way
http.globalAgent.maxSockets = 500;

/**
 * Client used to send heartbeat requests to its immediate master in the daisy
 * chain.
 *
 * @constructor
 *
 * @param {object} options Options object.
 * @param {Bunyan} options.log Bunyan logger.
 * @param {string} options.url URL of the client, e.g. http://10.0.0.1
 * @param {string} options.primaryUrl The URL to heartbeat to. e.g.
 * http://10.0.0.0:1234
 * @param {number} options.heartbeatInterval Heartbeat interval in ms.
 * @param {number} options.connectTimeout Client connection timeout in ms.
 * @param {number} options.requestTimeout Heartbeat request timeout in ms.
 * @param {object} options.retry Retry client connection.
 */
function HeartbeatClient(options) {
    assert.object(options, 'options');
    assert.object(options.log, 'options.log');
    assert.string(options.url, 'options.url');
    assert.string(options.primaryUrl, 'options.primaryUrl');
    assert.number(options.heartbeatInterval, 'options.heartbeatInterval');
    assert.number(options.connectTimeout, 'options.connectTimeout');
    assert.number(options.requestTimeout, 'options.requestTimeout');
    assert.object(options.retry, 'options.retry');

    var self = this;

    /** @type {Bunyan} The bunyan log object */
    this.log = options.log.child({component: 'HeartbeatClient'}, true);

    /**
     * @type {string}
     * The primary server's url and port. http://10.0.0.0:1234
     */
    this.primaryUrl = options.primaryUrl;

    /** @type {string} My url. http://10.0.0.1 */
    this.url = options.url;

    /** @type {number} Request timeout in ms.  */
    this.requestTimeout = options.requestTimeout;

    /** @type {boolean} restify retry.  */
    this.retry = options.retry;

    /** @type {number} The heartbeat interval in ms. */
    this.heartbeatInterval = options.heartbeatInterval;

    /** @type {object} The setInterval() intervalId for the heart beats. */
    this.intervalId = null;

    var parsedUrl = url.parse(self.primaryUrl);
    if (!parsedUrl.host || !parsedUrl.port) {
        throw new verror.VError('primaryUrl ' + self.primaryUrl +
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


    this.log.trace('initializing HeartbeatClient with options', options);
}

module.exports = HeartbeatClient;

/**
 * @callback HeartbeatClient-cb
 * @param {Error} error
 */

/**
 * Start heartbeating.
 * @param {HeartbeatClient-cb} callback
 */
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
        self.intervalId = setInterval(function () {
            _postHeartbeat(self, function () {});
        }, self.heartbeatInterval);
    }

    return callback();
};

/**
 * Stop heartbeating.
 * @param {HeartbeatClient-cb} callback
 */
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
};

/**
 * #@+
 * @private
 * @memberOf HeartbeatClient
 */

/**
 * Posts a heartbeat request to my master in the chain.
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
        var timeoutId = setTimeout(function () {
            log.warn('heartbeat request timed out');
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
        });
    });
}

/** #@- */
