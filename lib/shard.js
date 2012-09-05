// Copyright (c) 2012, Joyent, Inc. All rights reserved.
var assert = require('assert-plus');
var EventEmitter = require('events').EventEmitter;
var HeartbeatServer = require('./heartbeatServer');
var HeartbeatClient = require('./heartbeatClient');
var path = require('path');
var PostgresMan = require('./postgresMan');
var util = require('util');
var uuid = require('node-uuid');
var vasync = require('vasync');
var verror = require('verror');
var zkplus = require('zkplus');

/**
 * Represents a Postgres shard. We want to be as aggressive as possible on
 * pruning errors, which means the shard will emit error events which should
 * indicate to the consumer to restart the shard.
 *
 * Note the Shard assumes that both path, and registrarPath znodes have been
 * created.
 * @constructor
 *
 * @param {string} path The path under which znodes for this shard are stored.
 * Note, it's assumed this path already exists at shard creation time.
 * @param {string} registrarPath The path where registrar info is stored.
 * @param {object} zkCfg The ZK configs.
 * @param {string} url The url of the current node.
 * @param {object} log The bunyan log object.
 */
function Shard(options) {
        assert.object(options, 'options');
        assert.object(options.log, 'options.log');
        assert.string(options.shardPath, 'options.shardPath');
        assert.object(options.zkCfg, 'options.zkCfg');
        assert.object(options.postgresManCfg, 'options.postgresManCfg');
        assert.object(options.heartbeaterCfg, 'options.heartbeaterCfg');
        assert.number(options.backupPort, 'options.backupPort');
        assert.number(options.postgresPort, 'options.postgresPort');
        assert.string(options.url, 'options.url');
        assert.number(options.heartbeatPort, 'options.heartbeatPort');
        assert.number(options.heartbeatInterval, 'options.heartbeatInterval');
        assert.number(options.ttl, 'options.ttl');

        EventEmitter.call(this);
        var self = this;
        this._log = options.log;

        /**
        * The path under which znodes for this shard are stored
        */
        var shardPath = options.shardPath;

        /**
         * The registrar path, since the shardPath is usually
         * /prefix/com.joyent.foo.bar.baz, we strip out the first /prefix
         * replace . with / and append /pg
         */
        this._registrarPathPrefix =
                '/' +
                (shardPath.substring(shardPath.indexOf('/', 1) + 1))
                .split('.').reverse().join('/');

        this._registrarPath = this._registrarPathPrefix + '/pg';

        this._ttl = options.ttl;

        /**
        * The zk cfg
        */
        this._zkCfg = options.zkCfg;

        /**
        * The url of this peer
        */
        this._url = options.url;

        /**
        * This object's uuid
        */
        this._uuid = uuid();

        /**
        * The postgres manager
        */
        this._postgresMan = new PostgresMan(options.postgresManCfg);
        this._postgresPort = options.postgresPort;

        this._heartbeatServer = new HeartbeatServer({
                log: self._log,
                port: options.heartbeatPort
        });

        this._heartbeatPort = options.heartbeatPort;


        this._backupPort = options.backupPort;

        this._primary = null;

        this._heartbeatInterval = options.heartbeatInterval;

        this._currStandby = null;

        /**
        * The ZK session
        */
        this._zk = zkplus.createClient(self._zkCfg);
        this._election = null;

        self._zk.on('connect', function() {
                self._election = zkplus.createGenericElection({
                        client: self._zk,
                        path: shardPath,
                        pathPrefix: self._url,
                        object: {}
                });

                _init(self);
        });
}

module.exports = Shard;
util.inherits(Shard, EventEmitter);

function _init(self) {
        var log = self._log;
        var postgresMan = self._postgresMan;
        log.info('initializing shard');
        var heartbeatClient = null;

        self._election.on('leader', function() {
                log.info({
                        currStandby: self._currStandby
                },'going to primary mode');

                if (heartbeatClient) {
                        heartbeatClient.stop();
                }

                postgresMan.primary(self._currStandby, function(err) {
                        if (err) {
                                log.error({
                                        err: err
                                }, 'unable to transition to primary');
                                throw new verror.VError(err);
                        }

                        _writeRegistrar(self, function(err2) {
                                if (err2) {
                                        log.error({
                                                err: err2
                                        }, 'unable to write primary registrar');
                                        throw new verror.VError(err2);
                                }
                        });
                });
        });

        self._election.on('newLeader', function(leader) {
                var pgUrl = _transformPgUrl(self, leader);
                var backupUrl = _transformBackupUrl(self, leader);
                var heartbeatUrl = _transformHeartbeatUrl(self, leader);
                log.info({
                        leader: leader,
                        primaryUrl: pgUrl,
                        backupUrl: backupUrl,
                        heartbeatUrl: heartbeatUrl
                }, 'going to standby mode');
                postgresMan.standby(pgUrl, backupUrl, function(err) {
                        if (err) {
                                log.error({
                                        err: err,
                                        pgUrl: pgUrl,
                                        backupUrl: backupUrl
                                }, 'unable to transition to standby');
                                throw new verror.VError(err);
                        }

                        if (heartbeatClient) {
                                log.info('stopping heartbeat');
                                heartbeatClient.stop();
                        }

                        // start heartbeating
                        heartbeatClient = new HeartbeatClient({
                                log: log,
                                primaryUrl: heartbeatUrl,
                                url: self._url,
                                heartbeatInterval: self._heartbeatInterval
                        });

                        log.info('starting heartbeat');
                        heartbeatClient.start(function() {});
                });
        });

        self._heartbeatServer.on('newStandby', function(standby) {
                var standbyUrl = _transformPgUrl(self, standby);

                if (standbyUrl === self._currStandby) {
                        log.info({
                                prevStandby: self._currStandby,
                                standbyUrl: standbyUrl
                        }, 'standby not changed');

                        return;
                }

                self._currStandby = standbyUrl;

                if (!self._election.amLeader) {
                        log.info({
                                currStandby: self._currStandby
                        }, 'not primary, ignoring standby heartbeats,' +
                                ' but updating currStandby');
                        return;
                }

                log.info({
                        standby: standbyUrl
                }, 'going to primary mode with sync standby');

                postgresMan.primary(standbyUrl, function(err) {
                        if (err) {
                                log.error({
                                        err: err,
                                        standby: standbyUrl
                                }, 'unable to transition to primary');
                                throw new verror.VError(err);
                        }
                });
        });

        self._heartbeatServer.on('expired', function(standby) {
                var standbyUrl = _transformPgUrl(self, standby);
                if (standbyUrl !== self._currStandby) {
                        log.info({
                                currStandby: self._currStandby,
                                expiredStandby: standbyUrl
                        }, 'expired standby does not match currStandby,' +
                                ' ignoring');
                        return;
                }

                self._currStandby = null;

                // only remove standby if currently leader. If not leader,
                // the async standby doesn't require any action.
                if (self._election.amLeader) {
                        log.info({
                                standby: standbyUrl
                        }, 'standby expired, going to primary standalone mode');
                        postgresMan.primary(null, function(err) {
                                if (err) {
                                        log.error({
                                                err: err
                                        }, 'unable to transition to primary');
                                        throw new verror.VError(err);
                                }
                        });
                } else {
                        log.info({
                                standby: standbyUrl
                        }, 'async standby expired, I am not primary, ignoring');
                }
        });

        self._heartbeatServer.init();
        self._election.vote(function(err) {
                if (err) {
                        throw new verror.VError(err);
                }

                log.info('finished vote');
        });
}

function _transformPgUrl(self, url) {
        return 'tcp://postgres@' + url.split('-')[0] + ':' +
                self._postgresPort + '/postgres';
}

function _transformBackupUrl(self, url) {
        return  'http://' + url.split('-')[0] + ':' + self._backupPort;
}

function _transformHeartbeatUrl(self, url) {
        return  'http://' + url.split('-')[0]+ ':' + self._heartbeatPort;
}

function _writeRegistrar(self, callback) {
        var log = self._log;
        var zk = self._zk;

        var opts = {
                flags:  ['ephemeral'],
                object: {
                        type: 'database',
                        database: {
                                primary: _transformPgUrl(self, self._url),
                                ttl: self._ttl
                        }
                }
        };

        log.info({
                registrarPath: self._registrarPath,
                url: _transformPgUrl(self, self._url),
                opts: opts
        }, 'entering write registrar');

        zk.mkdirp(self._registrarPathPrefix, function(err) {
                if (err) {
                        return callback(err);
                } else {
                        return zk.creat(self._registrarPath, opts, callback);
                }
        });

        return true;
}
