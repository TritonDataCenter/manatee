/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2018, Joyent, Inc.
 * Copyright 2025 Edgecast Cloud LLC.
 */

/**
 * @overview Postgres backup server.
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

var assert = require('assert-plus');
var path = require('path');
var restify = require('restify');
var loadClusterDetails = require('./adm').loadClusterDetails;
var prometheusFormatter = require('./prometheusFormatter');

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

    /** @type {Object} Handle to the shard object */
    this._shard = options.shard;

    // restify endpoints
    var server = self._server;

    server.use(restify.queryParser());
    server.use(restify.bodyParser());

    server.get('/', list);
    server.get('/ping', ping);
    server.get('/state', state);
    server.get('/restore', restore);
    server.get('/metrics', metrics);

    server.listen(self._port, function () {
        log.info('status server started');
    });

    // list the valid endpoints
    function list(req, res, next) {
        //This is a little ghetto, but it doesn't seem restify keeps the
        // endpoints around that have been registered...  This will get more
        // complicated if anyone ever adds an HTTP verb that isn't exactly 3
        // characters.
        res.contentType = 'text';
        res.send(Object.keys(server.routes).map(function (key) {
            return ('/' + key.substring(3));
        }).join('\n') + '\n');
        return next();
    }

    // 503 unless postgres is known-healthy.
    function ping(req, res, next) {
        if (self._shard && self._shard._pg) {
            var stat = self._shard._pg.status();
            if (stat.healthy) {
                res.send(200, stat);
            } else {
                res.send(503, stat);
            }
        } else {
            res.send(503, 'PG not inited');
        }

        return next();
    }

    // send state machine debug info
    function state(req, res, next) {
        res.send(self._shard.debugState());
        return next();
    }

    // gets the status of the restore job
    function restore(req, res, next) {
        var stat = {};
        if (self._shard && self._shard._pg &&
            self._shard._pg._zfsClient) {
            stat.restore = self._shard._pg._zfsClient._restoreObject;
        }

        res.send(stat);
        return next();
    }

    function metrics(req, res, next) {
        var shardPath = self._shard._zk._path;
        var clusterArgs = {
            shard: path.basename(shardPath),
            zk: self._shard._zk._connStr,
            skipPostgres: false
        };

        loadClusterDetails(clusterArgs, function (err, details) {
            res.setHeader('Content-Type',
                'text/plain; version=0.0.4; charset=utf-8');

            if (err) {
                log.error({ err: err },
                    'error loading cluster details for metrics');
                res.send(500, prometheusFormatter.formatPrometheusError(err));
            } else {
                var metricsConfig = { shardPath: shardPath };
                var metricsText = prometheusFormatter.formatPrometheusMetrics(
                    details, metricsConfig);
                res.send(200, metricsText);
            }
            return next();
        });
    }


}

module.exports = {
    start: function (cfg) {
        return new StatusServer(cfg);
    }
};
