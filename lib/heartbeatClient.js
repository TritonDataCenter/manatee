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
 * @param {number} options.pgUrl  The URL of the PG instance.
 * @param {number} options.heartbeatInterval Heartbeat interval in ms.
 * @param {number} options.connectTimeout Client connection timeout in ms.
 * @param {number} options.requestTimeout Heartbeat request timeout in ms.
 * @param {object} options.retry Retry client connection.
 *
 * @throws {Error} If the options object is malformed.
 */
function HeartbeatClient(options) {
    assert.object(options, 'options');
    assert.object(options.log, 'options.log');

    assert.number(options.connectTimeout, 'options.connectTimeout');
    assert.number(options.heartbeatInterval, 'options.heartbeatInterval');
    assert.string(options.pgUrl, 'options.pgUrl');
    assert.string(options.primaryUrl, 'options.primaryUrl');
    assert.number(options.requestTimeout, 'options.requestTimeout');
    assert.object(options.retry, 'options.retry');
    assert.string(options.url, 'options.url');

    var self = this;

    /** @type {Bunyan} The bunyan log object */
    this._log = options.log.child({component: 'HeartbeatClient'}, true);

    /**
     * @type {string}
     * The primary server's url and port. http://10.0.0.0:1234
     */
    this._primaryUrl = options.primaryUrl;

    /** @type {string} The postgreSQL url */
    this._pgUrl = options.pgUrl;

    /** @type {string} My url. http://10.0.0.1 */
    this._url = options.url;

    /** @type {number} Request timeout in ms.  */
    this._requestTimeout = options.requestTimeout;

    /** @type {boolean} restify retry.  */
    this._retry = options.retry;

    /** @type {number} The heartbeat interval in ms. */
    this._heartbeatInterval = options.heartbeatInterval;

    /** @type {object} The setInterval() intervalId for the heart beats. */
    this._intervalId = null;

    var parsedUrl = url.parse(self._primaryUrl);
    if (!parsedUrl.host || !parsedUrl.port) {
        throw new verror.VError('primaryUrl ' + self._primaryUrl +
                        ' does not contain host or port');
    }

    /** @type {restify.JsonClient} The REST client used to heartbeat */
    this._client = restify.createJsonClient({
        log: self._log.child({component: 'HeartbeatClient.restify'}, true),
        url: self._primaryUrl,
        retry: self._retry,
        connectTimeout: options.connectTimeout,
        headers: { accept: 'text/plain' }
    });

    self._log.trace('initializing HeartbeatClient with options', options);
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
    var log = self._log;
    log.trace({
        url: self._primaryUrl,
        intervalId: self._intervalId
    }, 'starting heartbeat');

    if (self._intervalId !== null) {
        log.warn({
            url: self._primaryUrl,
            intervalId: self._intervalId
        }, 'heartbeat is already running');
    } else {
        self._intervalId = setInterval(function () {
            self._postHeartbeat(function () {});
        }, self._heartbeatInterval);
    }

    return callback();
};

/**
 * Stop heartbeating.
 * @param {HeartbeatClient-cb} callback
 */
HeartbeatClient.prototype.stop = function stop(callback) {
    var self = this;
    var log = self._log;

    log.trace({
        url: self._primaryUrl,
        intervalId: self._intervalId
    }, 'stopping heartbeat');

    if (self._intervalId) {
        clearInterval(self._intervalId);
        self._intervalId = null;
    } else {
        log.warn({
            url: self._primaryUrl
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
HeartbeatClient.prototype._postHeartbeat = function (callback) {
    var self = this;
    var log = self._log;

    callback = once(callback);

    log.trace('sending heartbeat request');

    self._client.post('/heartbeat',
                      { host: self._url, pgUrl: self._pgUrl },
                      function (connectErr, req)
    {
        log.trace({err: connectErr, req: req}, 'returned from post');

        if (connectErr) {
            self._log.warn({err: connectErr}, 'unable to connect to leader');
            return callback(connectErr);
        }

        return callback();
    });
};

/** #@- */
