/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2018, Joyent, Inc.
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
var BackupQueue = require('./backupQueue');
var restify = require('restify');
var uuid = require('node-uuid');

/**
 * REST server that takes zfs backup requests.
 * API:
 * POST /backup/
 * GET /backup/:uuid
 *
 * @constructor
 *
 * @param {object} options Options object.
 * @param {Bunyan} options.log Bunyan logger.
 * @param {number} options.port Server port.
 *
 * @throws {Error} If the options object is malformed.
 */
function BackupServer(options) {
    assert.object(options, 'options');
    assert.object(options.log, 'options.log');

    assert.number(options.port, 'options.port');

    var self = this;

    /** @type {Bunyan} The bunyan log object */
    this._log = options.log.child({component: 'BackupServer'}, true);
    var log = self._log;
    log.info('new backup server with options', options);

    /** @type {number} Server port. */
    this._port = options.port;

    /** @type {Restify} Restify REST server */
    this._server = restify.createServer({
        log: log
    });

    /** @type {BackupQueue} The queue of backup jobs in flight.  */
    this._queue = new BackupQueue({ log: log });

    self._init();
}

module.exports = {
    start: function (cfg) {
        return new BackupServer(cfg);
    }
};

/**
 * @return {BackupQueue} The queue used to hold backup requests
 */
BackupServer.prototype.getQueue = function () {
    var self = this;
    return self._queue;
};

/**
 * #@+
 * @private
 * @memberOf PostgresMgr
 */

/**
 * Initialize the backup server.
 */
BackupServer.prototype._init = function () {
    var self = this;
    var server = self._server;
    var log = self._log;

    server.use(restify.queryParser());
    server.use(restify.bodyParser());

    server.get('/backup/:uuid', checkBackup);
    server.post('/backup/', postBackup);

    server.listen(self._port, function () {
        log.info('backup server started');
    });

    // send the status of the backup
    function checkBackup(req, res, next) {
        return self._queue.get(req.params.uuid, function (backupJob) {
            if (!backupJob) {
                var err = new restify.ResourceNotFoundError();
                log.info({
                    uuid: req.params.uuid,
                    err: err
                }, 'backup job dne');

                return next(err);
            } else if (backupJob.err) {
                var err2 = new restify.InternalError(backupJob.err);
                log.info({
                    uuid: req.params.uuid,
                    err: err2
                }, 'internal error');

                return next(err2);
            }
            res.send(backupJob);
            return next();
        });
    }

    // Enqueue the backup request
    function postBackup(req, res, next) {
        var params = req.params;
        if (!params.host || !params.dataset || !params.port) {
            return next(new restify.MissingParameterError(
            'host, dataset, and port parameters required'));
        }

        var backupJob = {
            uuid: uuid.v4(),
            host:  params.host,
            port: params.port,
            dataset: params.dataset,
            done: false
        };

        self._queue.push(backupJob);

        res.send({
            jobid: backupJob.uuid,
            jobPath: '/backup/' + backupJob.uuid
        });
        return next();
    }
};
