// Copyright (c) 2012, Joyent, Inc. All rights reserved.
var assert = require('assert-plus');
var backoff = require('backoff');
var bunyan = require('bunyan');
var EventEmitter = require('events').EventEmitter;
var HeartbeatServer = require('./heartbeatServer');
var HeartbeatClient = require('./heartbeatClient');
var path = require('path');
var PostgresMgr = require('./postgresMgr');
var url = require('url');
var util = require('util');
var uuid = require('node-uuid');
var vasync = require('vasync');
var verror = require('verror');
var zkplus = require('zkplus');
var ZK = require('zookeeper');

/**
 * Represents a Postgres node in a shard. We want to be as aggressive as
 * possible on pruning errors, which means the shard will emit error events
 * which should indicate to the consumer to restart the shard.
 *
 * @constructor
 *
 * @param {string} shardPath The path under which znodes for this shard are
 * stored.
 * @param {object} zkCfg The ZK configs.
 * @param {object} postgresMgrCfg The PG configs.
 * @param {object} heartbeaterCfg The heartbeater configs.
 * @param {integer} postgresPort
 * @param {integer} backupPort The port used by the backup service.
 * @param {string} url The url of the current node.
 * @param {integer} heartbeatInterval The interval of the heartbeater.
 * @param {integer} ttl The ttl of the registrar node.
 * @param {object} log The bunyan log object.
 */
function Shard(options) {
    assert.object(options, 'options');
    assert.number(options.backupPort, 'options.backupPort');
    assert.number(options.heartbeatInterval, 'options.heartbeatInterval');
    assert.number(options.postgresPort, 'options.postgresPort');
    assert.number(options.ttl, 'options.ttl');
    assert.object(options.heartbeatServerCfg, 'options.heartbeatServerCfg');
    assert.object(options.log, 'options.log');
    assert.object(options.postgresMgrCfg, 'options.postgresMgrCfg');
    assert.object(options.zkCfg, 'options.zkCfg');
    assert.string(options.shardPath, 'options.shardPath');
    assert.string(options.url, 'options.url');
    assert.string(options.zoneId, 'options.zoneId');

    EventEmitter.call(this);
    var self = this;
    this._log = options.log;

    /**
     * The path under which shard metadata such as elections are stored
     */
    this._shardPath = options.shardPath;

    /**
     * The path under which elections for this shard are held
     */
    this._shardElectionPath = options.shardPath + '/election';

    /**
     * The znode that indicates an error with the shard
     */
    this._shardErrorPath = options.shardPath + '/error';

    /**
     * The znode underwhich stores the history of the shard
     */
    this._shardHistoryPath = options.shardPath + '/history';

    /**
     * the uuid of this smartos zone
     */
    this._zoneId = options.zoneId;

    /**
     * The registrar path, since the shardPath is usually
     * /prefix/com.joyent.foo.bar.baz, we strip out the first /prefix
     * replace . with / and append /pg
     */
    this._registrarPathPrefix =
    '/' +
        (self._shardPath.substring(self._shardPath.indexOf('/', 1) + 1))
    .split('.').reverse().join('/');

    this._registrarPath = this._registrarPathPrefix + '/pg';

    this._ttl = options.ttl;

    /**
     * The host name of the most recent leader of this node.
     */
    this._leaderHost = null;

    /**
     * The zk cfg
     */
    this._zkCfg = options.zkCfg;
    self._zkCfg.log = self._log.child({
        component: 'zookeeper',
        level: 'trace'
    });
    self._zkCfg.log.level('trace');

    /**
     * The url of this peer, ex. 10.99.99.16
     */
    this._url = options.url;

    /**
     * This object's uuid
     */
    this._uuid = uuid();

    /**
     * The postgres manager
     */
    this._postgresMgr = new PostgresMgr(options.postgresMgrCfg);
    this._postgresPort = options.postgresPort;

    /**
     * The heartbeat server that listens for standbys
     */
    this._heartbeatServerCfg = options.heartbeatServerCfg;
    this._heartbeatServer = new HeartbeatServer(self._heartbeatServerCfg);
    this._heartbeatInterval = options.heartbeatInterval;
    this._heartbeatClient = null;

    /**
     * The port used by the pg backup service
     */
    this._backupPort = options.backupPort;

    this._currStandby = null;

    /**
     * The queue used to manage transition events such that they occur
     * serially and automically.
     */
    this._transitionQueue = new TransitionQueue({
        log: self._log
    });

    self._transitionQueue.on('execError', function(err) {
        self._log.error({
            err: err
        }, 'Shard: got transition queue execution error');

        throw new verror.VError(err);
    });

    /**
     * The ZK session
     */
    this._election = null;
    this._zk = null;

    _createZkClient(self._zkCfg, function(err, zkClient) {
        if (err) {
            self._log.fatal({err: err}, 'unable to connect to zk');
            self.emit('error', err);
        }
        self._zk = zkClient;

        self._zk.on('error', function(err4) {
            self._log.fatal({err: err4}, 'zk error, exiting');
            process.exit(1);
            return (undefined);
        });

        _initAndCleanupZkNodes(self, function(err2) {
            if (err2) {
                self._log.error({err: err2},
                    'unable to clean up previous emphemeral nodes');
                self.emit('error', err);
            }
            _checkAndWatchError(self, function(err3) {
                if (err3) {
                    self.emit('error', err3);
                }

                self._election = zkplus.createGenericElection({
                    client: self._zk,
                    path: self._shardElectionPath,
                    pathPrefix: self._url,
                    object: {
                        zoneId: self._zoneId,
                        ip: self._url
                    }
                });
                _init(self);
            });
        });
    });
}

module.exports = Shard;
util.inherits(Shard, EventEmitter);

// Transition functions

function _leader(ctx, callback) {
    var self = ctx.self;
    var postgresMgr = self._postgresMgr;
    var log = self._log;
    self._leaderHost = null;
    log.info({
        currStandby: self._currStandby
    },'going to primary mode');

    var tasks = [
        function stopHeartBeat(_, cb) {
            if (self._heartbeatClient) {
                log.info('stopping heartbeat');
                self._heartbeatClient.stop();
            }

            return cb();
        },
        function primary(_, cb) {
            log.info('_leader; going to pg primary mode');
            postgresMgr.primary(self._currStandby, cb);
        },
        function writeRegistrar(_, cb) {
            log.info('_leader; writing registrar');
            _writeRegistrar(self, cb);
        },
        function writeHistoryNode(_, cb) {
            log.info('_leader; writing historyNode');
            var opts = {
                standby: self._currStandby,
                role: 'Leader'
            };
            _writeHistoryNode(self, opts, cb);
        }
    ];

    vasync.pipeline({funcs: tasks, arg: {}}, function(err) {
        log.info({
            err: err,
            currStandby: self._currStandby
        }, 'Shard._newLeader: finished.');

        if (err && err.__walCorruption) {
            log.error({
                err: err
            }, '_leader: got wal corruption');
            _writeErrorNode(self, self._currStandby, function(err2) {
                if (err2) {
                    return callback(new verror.VError(err2,
                                                'unable to write error node'));
                }

                log.info('_leader: entering shardError state');
                _shardError(self);
                // don't return cb to stop any further state changes
                return (undefined);
            });
        } else if (err) {
            err = new verror.VError(err, 'unable to transition to primary');
            return callback(err);
        } else {
            log.info('_leader: successful transition');
            return callback();
        }

        return (undefined);
    });
}

function _newLeader(ctx, callback) {
    var self = ctx.self;
    var log = self._log;
    var myLeader = ctx.leader;
    var pgUrl = _transformPgUrl(self, myLeader);
    var backupUrl = _transformBackupUrl(self, myLeader);
    var heartbeatUrl = _transformHeartbeatUrl(self, myLeader);
    self._leaderHost = url.parse(pgUrl).hostname;
    var postgresMgr = self._postgresMgr;
    log.info({
        leader: myLeader,
        primaryUrl: pgUrl,
        backupUrl: backupUrl,
        heartbeatUrl: heartbeatUrl
    }, 'Shard._newLeader: entering');

    var tasks = [
        function pgStandby(_, cb) {
            log.info({
                pgUrl: pgUrl,
                backupUrl: backupUrl
            }, 'Shard._newLeader: pg going to standby');

            postgresMgr.standby(pgUrl, backupUrl, cb);
        },
        function stopHeartBeat(_, cb) {
            if (self._heartbeatClient) {
                log.info('Shard._newLeader: stopping heartbeat');
                self._heartbeatClient.stop();
            }

            return cb();
        },
        function startHeartbeat(_, cb) {
            log.info({
                primaryUrl: heartbeatUrl,
                url: self._url,
                heartbeatInterval: self._heartbeatInterval
            }, 'Shard._newLeader: starting heartbeat');
            self._heartbeatClient = new HeartbeatClient({
                log: log,
                primaryUrl: heartbeatUrl,
                url: self._url,
                heartbeatInterval: self._heartbeatInterval
            });
            self._heartbeatClient.start(cb);
        },
        function writeHistory(_, cb) {
            log.info('Shard._newLeader: writing history');
            var opts = {
                standby: self._currStandby,
                role: 'Standby',
                leader: self._leaderHost
            };
            _writeHistoryNode(self, opts, cb);
        }
    ];

    vasync.pipeline({funcs: tasks, arg: {}}, function(err) {
        log.info({
            err: err,
            pgUrl: pgUrl,
            backupUrl: backupUrl
        }, 'Shard._newLeader: finished.');

        if (err)  {
            err = new verror.VError(err, 'Shard._newLeader: failed');
        }

        return callback(err);
    });
}

function _newStandby(ctx, callback) {
    var self = ctx.self;
    var log = self._log;
    var postgresMgr = self._postgresMgr;
    var standby = ctx.standby;
    var standbyUrl = _transformPgUrl(self, standby);
    var standbyHostname = url.parse(standbyUrl).hostname;

    var tasks = [
        function checkStandby(_, cb) {
            log.info({
                prevStandby: self._currStandby,
                standbyUrl: standbyUrl
            }, 'Shard._newStandby: checking standby status');

            if (standbyUrl === self._currStandby) {
                log.info('Shard._newStandby: standby not changed');
                _.skipPg = true;
                _.skipHistory = true;
            }

            self._currStandby = standbyUrl;
            if (!self._election.amLeader) {
                log.info({
                    currStandby: self._currStandby
                }, 'Shard._newStandby: not primary, ignoring standby ' +
                   'heartbeats, but updating currStandby');

                _.skipPg = true;
            }
            return cb();
        },
        function updateStandby(_, cb) {
            if (_.skipPg) {
                return cb();
            }
            log.info({
                currStandby: self._currStandby
            }, 'Shard._newStandby: updating pg standby');
            postgresMgr.updateStandby(standbyUrl, cb);

            return (undefined);
        },
        function writeHistory(_, cb) {
            if (_.skipHistory) {
                return cb();
            }
            log.info('Shard._newStandby: writing history');
            var opts = {
                standby: standbyHostname,
                role: self._election.amLeader ? 'Leader' : 'Standby',
                leader: self.leaderHost
            };
            _writeHistoryNode(self, opts, cb);

            return (undefined);
        }
    ];

    vasync.pipeline({funcs: tasks, arg: {}}, function(err) {
        log.info({
            err: err,
            prevStandby: self._currStandby,
            standbyUrl: standbyUrl
        }, 'Shard._newStandby: finished.');

        if (err && err.__walCorruption) {
            log.error({
                err: err,
                prevStandby: self._currStandby,
                standbyUrl: standbyUrl
            }, 'Shard._newStandby: got wal corruption');

            _writeErrorNode(self, self._currStandby, function(err2) {
                if (err2) {
                    return callback(new verror.VError(err2,
                                                'unable to write error node'));
                }

                log.info('Shard._newStandby: entering shardError state');
                _shardError(self);
                // don't return cb to stop any further state changes
                return (undefined);
            });
        } else if (err) {
            err = new verror.VError(err,
                'Shard._newStandby: unable to add new standby');
            return callback(err);
        } else {
            log.info('Shard._newStandby: successful transition');
            return callback();
        }

        return (undefined);
    });
}

function _expired(ctx, callback) {
    var self = ctx.self;
    var postgresMgr = self._postgresMgr;
    var standby = ctx.standby;
    var log = self._log;
    var standbyUrl = _transformPgUrl(self, standby);

    log.info({
        currStandby: self._currStandby,
        expiredStandby: standbyUrl,
        isLeader: self._election.amLeader
    }, 'Shard._expired: entering');

    var tasks = [
        function checkStandbyAndIsLeader(_, cb) {
            _.continue = false;
            if (standbyUrl === self._currStandby && self._election.amLeader) {
                log.info({
                    currStandby: self._currStandby,
                    expiredStandby: standbyUrl,
                    isLeader: self._election.amLeader
                }, 'Shard._expired: expired standby matches currStandby and ' +
                   'isLeader, removing expired standby');
                _.continue = true;
            }

            cb();
        },
        function removeStandby(_, cb) {
            if (!_.continue) {
                return cb();
            }
            log.info('removing expired standby');
            postgresMgr.updateStandby(null, cb);

            return (undefined);
        },
        function writeHistory(_, cb) {
            if (standbyUrl === self._currStandby) {
                log.info('Shard._expired: writing history');
                var opts = {
                    role: self._election.amLeader ? 'Leader' : 'Standby',
                    leader: self.leaderHost
                };
                _writeHistoryNode(self, opts, cb);
            }
        }
    ];

    vasync.pipeline({funcs: tasks, arg: {}}, function(err) {
        if (err) {
            err = new verror.VError(err, 'Shard._expired failed');
        }

        log.info({
            err: err,
            currStandby: self._currStandby,
            expiredStandby: standbyUrl,
            isLeader: self._election.amLeader
        }, 'Shard._expired: finished');

        return callback(err);
    });
}

// private functions

function _createZkClient(opts, cb) {
    assert.object(opts, 'options');
    assert.object(opts.log, 'options.log');
    assert.arrayOfObject(opts.servers, 'options.servers');
    assert.number(opts.timeout, 'options.timeout');
    assert.func(cb, 'callback');

    assert.ok((opts.servers.length > 0), 'options.servers empty');
    for (var i = 0; i < opts.servers.length; i++) {
        assert.string(opts.servers[i].host, 'servers.host');
        assert.number(opts.servers[i].port, 'servers.port');
    }

    function _createClient(_, _cb) {
        var client = zkplus.createClient(opts);

        function onConnect() {
            client.removeListener('error', onError);
            log.info('zookeeper: connected');
            _cb(null, client);
        }

        function onError(err) {
            client.removeListener('connect', onConnect);
            _cb(err);
        }


        client.once('connect', onConnect);
        client.once('error', onError);

        client.connect();
    }

    var log = opts.log;
    var retry = backoff.call(_createClient, null, cb);
    retry.failAfter(Infinity);
    retry.setStrategy(new backoff.ExponentialStrategy({
        initialDelay: 1000,
        maxDelay: 30000
    }));

    retry.on('backoff', function (number, delay) {
        var level;
        if (number === 0) {
            level = 'info';
        } else if (number < 5) {
            level = 'warn';
        } else {
            level = 'error';
        }
        log[level]({
            attempt: number,
            delay: delay
        }, 'zookeeper: connection attempted (failed)');
    });

    return (retry);
}

function _writeErrorNode(self, standby, callback) {
    var log = self._log;
    var zk = self._zk;

    log.info({
        standby: standby
    }, 'entering shard._writeErrorNode');
    var standbyUrl = url.parse(standby).hostname;

    var opts = {
        object: {
            primary: self._url,
            zoneId: self._zoneId,
            standby: standbyUrl
        }
    };

    log.info({
        error: opts.object
    }, 'shard._writeErrorNode: writing error node to zk');

    zk.creat(self._shardErrorPath, opts, function(err) {
        return callback(err);
    });
}

function _writeHistoryNode(self, opts, callback) {
    var log = self._log;
    var zk = self._zk;
    log.info({
        opts: opts
    }, 'Shard._writeHistoryNode: entering');

    var _path = self._shardHistoryPath + '/' + Date.now() + '-' + self._url +
        '-' + opts.role + '-' + opts.leader + '-' + opts.standby + '-';

    log.info({
        path: _path
    }, 'Shard._writeHistoryNode: writing node');

    zk.create(_path, {flags: ['sequence']}, callback);
}

function _checkAndWatchError(self, callback) {
    var log = self._log;
    var zk = self._zk;

    log.info({
        shardErrorPath: self._shardErrorPath
    }, 'entering shard._checkAndWatchError');

    var tasks = [
        function watchForError(_, cb) {
            zk.watch(self._shardPath, { method: 'list' }, function(err, listener) {
                if (err) {
                    return callback(new verror.VError(err,
                    'unable to set watch on error node'));
                }

                listener.on('children', function(children) {
                    children.forEach(function(child) {
                        if (child === 'error') {
                            log.fatal('got shard error, going to readonly mode');
                            _shardError(self);
                        }
                    });
                });

                return cb();
            });
        },
        function checkForError(_, cb) {
            zk.get(self._shardErrorPath, function(err, obj) {
                if (err && err.code === ZK.ZNONODE) {
                    return cb();
                } else if (err) {
                    return cb(new verror.VError(err,
                    'unable to get error node'));
                } else if (obj) {
                    log.fatal('got shard error, ' +
                    'going to readonly mode');
                _shardError(self);
                // don't return cb to prevent further
                // state transitions
                return (undefined);
                } else {
                    return cb();
                }
            });
        }
    ];

    vasync.pipeline({funcs: tasks, arg: {}}, function(err) {
        if (err) {
            return callback(new verror.VError(err,
            'unable to check for shard error state'));
        } else {
            log.info('finished checking for shard errors');
            return callback();
        }
    });
}

/**
 * This function gets called if something catestrophic has happened in the
 * shard. We want to put the node in read only mode and remove all listeners
 * and wait for an operator to manually clear the shard
 */
function _shardError(self) {
    var log = self._log;
    log.fatal('entering shard.shardError: SOMETHING IS LIKELY VERY WRONG ' +
        'WITH THE SHARD, OPERATOR INTERVENTION NEEDED!');
    // remove all listeners
    self.removeAllListeners();
    if (self._heartbeatServer) {
        self._heartbeatServer.removeAllListeners();
    }
    if (self._heartbeatClient) {
        self._heartbeatClient.stop();
    }
    if (self._election) {
        self._election.removeAllListeners();
        self._election.on('error', function() {});
    }
    if (self._zk) {
        self._zk.removeAllListeners();
        // swallow zk errors
        self._zk.on('error', function() {});
        self._zk.close();
    }

    self._postgresMgr.readOnly(function(err){
        // give node something to do so it doesn't exit.
        setInterval(function(){
            log.trace('In forced read only mode due to error');
        }, 2000);

        if (err) {
            self.emit(verror.VError(err,
            'unable to transition to read only mode'));
        }
    });
}

/**
 * Delete any previous emphemeral zk-nodes belonging to this pg node.
 */
function _initAndCleanupZkNodes(self, callback) {
    var log = self._log;
    var zk = self._zk;

    log.info({
        shardElectionPath: self._shardElectionPath,
        url: self._url
    },'Shard._initAndCleanupZkNodes');
    var tasks = [
        function mkdirpElectionPath(_, cb) {
            zk.mkdirp(self._shardElectionPath, function(err) {
                log.debug({
                    path: self._shardElectionPath,
                    err: err
                }, 'returned from mkdirpElectionPath');
                cb(err);
            });
        },
        function mkdirpHistoryPath(_, cb) {
            zk.mkdirp(self._shardHistoryPath, function(err) {
                log.debug({
                    path: self._shardHistoryPath,
                    err: err
                }, 'returned from mkdirpShardHistoryPath');
                cb(err);
            });
        },
        function readdir(_, cb) {
            log.debug({
                shardElectionPath: self._shardElectionPath
            }, 'listing election dir');
            zk.readdir(self._shardElectionPath, function(err, nodes) {
                log.debug({
                    err: err,
                    nodes: nodes
                }, 'returned from readdir');
                if (err) {
                    return cb(err);
                } else {
                    _.nodes = nodes;
                    return cb();
                }
            });
        },
        function determineNodes(_, cb) {
            log.debug({
                nodes: _.nodes
            }, 'entering determine nodes');
            var nodes = [];
            for (var i = 0; i < _.nodes.length; i++) {
                var node = _.nodes[i];
                if (node.split('-')[0] === self._url) {
                    nodes.push(node);
                }
            }

            _.nodes = nodes;
            cb();
        },
        function delPrevNodes(_, cb) {
            log.debug({
                nodes: _.nodes
            },'entering delPrevNodes');
            var done = 0;
            for (var i = 0; i < _.nodes.length; i++) {
                var spath = self._shardElectionPath + '/' + _.nodes[i];
                log.debug({
                    node: spath
                }, 'deleting prev node');
                zk.rmr(spath, function(err) {
                    if (err) {
                        return cb(err);
                    } else {
                        done++;
                        var nLength = _.nodes.length;
                        if (done === (nLength - 1)) {
                            return cb();
                        }
                    }
                    return true;
                });

            }

            return cb();
        }
    ];

    vasync.pipeline({funcs: tasks, arg: {}}, function(err) {
        if (err) {
            log.error('Unable to initialize zk paths or delete previous ' +
                      'emphemeral nodes');
            return callback(err);
        } else {
            log.info('finished initializing zk paths and deleting previous ' +
                     'emphemeral nodes');
            return callback();
        }
    });
}

/**
 * initialize this node
 */
function _init(self) {
    var log = self._log;
    log.info('initializing shard');
    var queue = self._transitionQueue;

    self._election.on('leader', function() {
        log.info('leader event');
        var req = {
            invoke: _leader,
            ctx: {
                self: self
            },
            transition: 'leader'
        };

        queue.push(req);
    });

    self._election.on('newLeader', function(myLeader) {
        log.info({
            newLeader: myLeader
        }, 'newLeader event');
        var req = {
            invoke: _newLeader,
            ctx: {
                self: self,
                leader: myLeader
            },
            transition: 'newLeader'
        };

        queue.push(req);
    });

    self._heartbeatServer.on('newStandby', function(standby) {
        log.info({
            standby: standby
        }, 'newStandby event');
        var req = {
            invoke: _newStandby,
            ctx: {
                self: self,
                standby: standby
            },
            transition: 'newStandby'
        };

        queue.push(req);
    });

    self._heartbeatServer.on('expired', function(standby) {
        log.info({
            standby: standby
        }, 'expired event');
        var req = {
            invoke: _expired,
            ctx: {
                self: self,
                standby: standby
            },
            transition: 'expired'
        };

        queue.push(req);
    });

    self._heartbeatServer.init();
    self._election.vote(function(err) {
        if (err) {
            throw new verror.VError(err);
        }

        log.info('finished vote');
    });
}

function _transformPgUrl(self, _url) {
    return 'tcp://postgres@' + _url.split('-')[0] + ':' +
        self._postgresPort + '/postgres';
}

function _transformBackupUrl(self, _url) {
    return  'http://' + _url.split('-')[0] + ':' + self._backupPort;
}

function _transformHeartbeatUrl(self, _url) {
    return  'http://' + _url.split('-')[0]+ ':' +
        self._heartbeatServerCfg.port;
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

    var tasks = [
        function mkdirp(_, cb) {
            zk.mkdirp(self._registrarPathPrefix, function(err) {
                var err2;
                if (err)  {
                    err2 = verror.VError(err);
                }
                cb(err2);
            });
        },
        function rm(_, cb) {
            zk.rmr(self._registrarPath, function() {
                // don't check for err on rmr, it could fail if
                // the node DNE
                cb();
            });
        },
        function create(_, cb) {
            zk.create(self._registrarPath, opts, function(err) {
                var err2;
                if (err)  {
                    err2 = verror.VError(err);
                }
                cb(err2);
            });
        }
    ];

    vasync.pipeline({funcs: tasks, arg: {}}, function(err) {
        if (err) {
            log.error('Unable to write registrar');
            return callback(err);
        } else {
            log.info('finished writing registrar');
            return callback();
        }
    });

    return true;
}

// private classes

/**
 * Queue used to serialize state transitions
 */
function TransitionQueue(options) {
    EventEmitter.call(this);
    var self = this;
    this._queue = [];
    this._log = options.log;
    this._emitter = new EventEmitter();

    var log = self._log;
    var isPop = false;
    var req = null;
    self._emitter.on('push', function() {
        if (isPop) {
            log.info({
                queue_length: self._queue.length,
                curr_req: req.transition,
                new_req: self._queue[self._queue.length - 1].transition
            }, 'waiting for current transition to finish before ' +
            'executing pushed req');
        return;
        } else {
            exec();
        }
    });

    function exec() {
        isPop = true;
        req = self._queue.shift();
        req.invoke(req.ctx, function(err) {
            log.info({
                transition: req.transition,
                err: err
            }, 'finished transition');
            if (err) {
                self.emit('execError',
                    new verror.VError(err, 'transition queue exec error'));
            }
            if (self._queue.length > 0) {
                log.info({
                    queue_length: self._queue.length
                }, 'more elements in the queue,' +
                ' continuing exec');
            exec();
            } else {
                log.info({
                    queue_length: self._queue.length
                }, 'no more elements in queue');
                isPop = false;
            }
        });
    }
}
util.inherits(TransitionQueue, EventEmitter);

TransitionQueue.prototype.push = function push(req) {
    var self = this;
    var log = self._log;
    log.info({
        req: req.transition
    }, 'pushing request');
    self._queue.push(req);
    self._emitter.emit('push');
};
