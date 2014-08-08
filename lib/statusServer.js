/**
 * @overview Postgres backup server.
 * @copyright Copyright (c) 2013, Joyent, Inc. All rights reserved.
 *
 *                   _.---.._
 *      _        _.-' \  \    ''-.
 *    .'  '-,_.-'   /  /  /       '''.
 *   (       _                     o  :
 *    '._ .-'  '-._         \  \-  ---]
 *                  '-.___.-')  )..-'
 *                           (_/
 *
 */
var util = require('util');

var assert = require('assert-plus');
var restify = require('restify');
var uuid = require('node-uuid');

/**
 *
 * @constructor
 *
 * @param {object} options Options object.
 * @param {Bunyan} options.log Bunyan Logger.
 * @param {number} options.port Server port.
 * @param {Object} options.shard Shard object.
 *
 * @throws {Error} If the options object is malformed.
 */
function StatusServer(options) {
    assert.object(options, 'options');
    assert.object(options.log, 'options.log');
    assert.number(options.port, 'options.port');
    assert.object(options.shard, 'options.shard');

    var self = this;
   /** @type {Bunyan} The bunyan log object */
    this._log = options.log.child({component: 'StatusServer'}, true);
    var log = self._log;
    log.info('new backup server with options', options);

    /** @type {number} Server port. */
    this._port = options.port;

    /** @type {Restify} Restify REST server */
    this._server = restify.createServer({
        log: log
    });

    /** @type {Object} Handle to the shard object.*/
    this._shard = options.shard;

    // restify endpoints
    var server = self._server;

    server.use(restify.queryParser());
    server.use(restify.bodyParser());

    server.get('/status', status);

    server.listen(self._port, function () {
        log.info('status server started');
    });

    // send the status of the current shard
    function status(req, res, next) {
        var stat = {};
        if (self._shard && self._shard._postgresMgr &&
            self._shard._postgresMgr._zfsClient) {
            stat.restore = self._shard._postgresMgr._zfsClient._restoreObject;
        }

        res.send(stat);
        return next();
    }
}

module.exports = {
    start: function (cfg) {
        return new StatusServer(cfg);
    }
};
