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
     * The zk cfg
     */
    this._zkCfg = options.zkCfg;
    self._zkCfg.log = self._log.child({
        component: 'zookeeper',
        level: 'trace'
    });
    self._zkCfg.log.level('trace');

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

        _cleanup(self, function(err2) {
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

function _leader(ctx, cb) {
    var self = ctx.self;
    var postgresMgr = self._postgresMgr;
    var log = self._log;
    log.info({
        currStandby: self._currStandby
    },'going to primary mode');

    if (self._heartbeatClient) {
        log.info('stopping heartbeat');
        self._heartbeatClient.stop();
    }

    postgresMgr.primary(self._currStandby, function(err) {
        if (err && err.__walCorruption) {
            _writeErrorNode(self, self._currStandby, function(err2)
            {
                if (err2) {
                    throw new verror.VError(err2,
                    'unable to write error node');
                }

                _shardError(self);
                // don't return cb to stop any further state
                // changes
                return (undefined);
            });
        } else if (err) {
            log.error({
                err: err
            }, 'unable to transition to primary');
            throw new verror.VError(err);
        } else {
            _writeRegistrar(self, function(err2) {
                if (err2) {
                    log.error({
                        err: err2
                    }, 'unable to write primary registrar');
                    throw new verror.VError(err2);
                }
                return cb();
            });
        }

    });
}

function _newLeader(ctx, cb) {
    var self = ctx.self;
    var log = self._log;
    var myLeader = ctx.leader;
    var pgUrl = _transformPgUrl(self, myLeader);
    var backupUrl = _transformBackupUrl(self, myLeader);
    var heartbeatUrl = _transformHeartbeatUrl(self, myLeader);
    var postgresMgr = self._postgresMgr;
    log.info({
        leader: myLeader,
        primaryUrl: pgUrl,
        backupUrl: backupUrl,
        heartbeatUrl: heartbeatUrl
    }, 'going to standby mode');

    postgresMgr.standby(pgUrl, backupUrl, function(err) {
        if (err) {
            log.error({
                err: err,
                pgUrl: pgUrl,
                backupUrl: backupUrl
            }, 'unable to transition to standby');
            throw new verror.VError(err);
        }

        if (self._heartbeatClient) {
            log.info('stopping heartbeat');
            self._heartbeatClient.stop();
        }

        // start heartbeating
        self._heartbeatClient = new HeartbeatClient({
            log: log,
            primaryUrl: heartbeatUrl,
            url: self._url,
            heartbeatInterval: self._heartbeatInterval
        });

        log.info('starting heartbeat');
        self._heartbeatClient.start(function() {
            cb();
        });
    });
}

function _newStandby(ctx, cb) {
    var self = ctx.self;
    var log = self._log;
    var postgresMgr = self._postgresMgr;
    var standby = ctx.standby;
    var standbyUrl = _transformPgUrl(self, standby);

    if (standbyUrl === self._currStandby) {
        log.info({
            prevStandby: self._currStandby,
            standbyUrl: standbyUrl
        }, 'standby not changed');

        return cb();
    }

    self._currStandby = standbyUrl;
    if (!self._election.amLeader) {
        log.info({
            currStandby: self._currStandby
        }, 'not primary, ignoring standby heartbeats,' +
        ' but updating currStandby');

    return cb();
    }

    log.info({
        standby: standbyUrl
    }, 'going to primary mode with sync standby');

    postgresMgr.updateStandby(standbyUrl, function(err) {
        if (err && err.__walCorruption) {
            _writeErrorNode(self, standbyUrl, function(err2) {
                if (err2) {
                    throw new verror.VError(err2,
                    'unable to write error node');
                }

                _shardError(self);
                // don't return cb to stop any further state
                // changes
            });
        } else if (err) {
            throw new verror.VError(err,
            'unable to transition to primary');
        } else {
            log.info({
                standby: standbyUrl
            }, 'finished primary transition');
            return cb();
        }
        return (undefined);
    });

    return true;
}

function _expired(ctx, cb) {
    var self = ctx.self;
    var postgresMgr = self._postgresMgr;
    var standby = ctx.standby;
    var log = self._log;
    var standbyUrl = _transformPgUrl(self, standby);

    if (standbyUrl !== self._currStandby) {
        log.info({
            currStandby: self._currStandby,
            expiredStandby: standbyUrl
        }, 'expired standby does not match currStandby,' +
        ' ignoring');
    return cb();
    }

    self._currStandby = null;

    // only remove standby if currently leader. If not leader, the async
    // standby doesn't require any action.
    if (self._election.amLeader) {
        log.info({
            standby: standbyUrl
        }, 'standby expired, going to primary standalone mode');
        postgresMgr.updateStandby(null, function(err) {
            if (err) {
                log.error({
                    err: err
                }, 'unable to transition to primary');
                throw new verror.VError(err);
            }
            return cb();
        });
    } else {
        log.info({
            standby: standbyUrl
        }, 'async standby expired, I am not primary, ignoring');
        return cb();
    }

    return true;
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

function _checkAndWatchError(self, callback) {
    var log = self._log;
    var zk = self._zk;

    log.info({
        shardErrorPath: self._shardErrorPath
    }, 'entering shard._checkAndWatchError');

    var tasks = [
        function watchForError(_, cb) {
            zk.watch(self._shardPath,
                { method: 'list' },
                function(err, listener)
                {
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
function _cleanup(self, callback) {
    var log = self._log;
    var zk = self._zk;

    log.info({
        shardElectionPath: self._shardElectionPath,
        url: self._url
    },'entering cleanup');
    var tasks = [
        function mkdirp(_, cb) {
            zk.mkdirp(self._shardElectionPath, function(err) {
                log.debug({
                    path: self._shardElectionPath,
                    err: err
                }, 'returned from mkdirp');
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
            log.error('Unable to delete previous emphemeral nodes');
            return callback(err);
        } else {
            log.info('finished deleting previous emphemeral nodes');
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

function _transformPgUrl(self, url) {
    return 'tcp://postgres@' + url.split('-')[0] + ':' +
        self._postgresPort + '/postgres';
}

function _transformBackupUrl(self, url) {
    return  'http://' + url.split('-')[0] + ':' + self._backupPort;
}

function _transformHeartbeatUrl(self, url) {
    return  'http://' + url.split('-')[0]+ ':' +
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
        req.invoke(req.ctx, function() {
            log.info({
                transition: req.transition
            }, 'finished transition');
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

TransitionQueue.prototype.push = function push(req) {
    var self = this;
    var log = self._log;
    log.info({
        req: req.transition
    }, 'pushing request');
    self._queue.push(req);
    self._emitter.emit('push');
};
