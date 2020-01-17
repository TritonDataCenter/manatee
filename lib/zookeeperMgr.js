/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2020 Joyent, Inc.
 */

/**
 * @overview The Zookeeper wrapper. Handles/abstracts all zk interactions.
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
var EventEmitter = require('events').EventEmitter;
var once = require('once');
var util = require('util');
var vasync = require('vasync');
var zkClient = require('joyent-zookeeper-client');

/**
 * An abstraction over zookeeper for the things that only manatee cares about.
 *
 * Constructor:
 *     - options.log: A Bunyan logger.
 *     - options.id: An optional id for this process.
 *     - options.data: A structure representing "me".  It could include things
 *                     like a zonename, ipaddress, ports, etc.
 *     - options.path: The Manatee path in Zookeeper.  Manatee will create other
 *       directories under this path as required.
 *     - options.connStr: The Zookeeper connection string.  It is a comma
 *       separated set of host:port pairs, each representing a zookeeper host.
 *     - options.opts: Other options to be passed straight through to
 *       node-zookeeper-client.
 *
 * Events:
 *     - 'init': Emitted when the ZK client has first connected and has set up
 *       the required state in ZK.
 *     - 'clusterStateChange': Emitted when the state of the cluster has been
 *       changed.  For example, when a new async has been placed in the
 *       topology.  Returns the cluster state object.
 *     - 'activeChange': Emitted when ZK has determined that a new manatee node
 *       has joined or an old has left.  Returns details on all active nodes.
 *
 * APIs:
 *     - putClusterState(clusterState, cb): When Manatee has determined that the
 *       topology has changed, put a new clusterState
 *     - status: Returns the ZK connection status
 */
function ZookeeperMgr(options) {
    assert.object(options, 'options');
    assert.object(options.log, 'options.log');
    assert.optionalString(options.id, 'options.id');
    assert.object(options.data, 'options.data');
    assert.string(options.path, 'options.path');
    assert.string(options.connStr, 'options.connStr');
    assert.object(options.opts, 'options.opts');

    EventEmitter.call(this);

    var self = this;
    self._log = options.log.child({ component: 'ZookeeperMgr' }, true);
    var log = this._log;
    self._id = options.id;
    self._data = options.data;
    self._path = options.path;
    self._connStr = options.connStr;
    self._opts = options.opts;

    var p = self._path;
    if (p.charAt(p.length - 1) !== '/') {
        p += '/';
    }
    self._ephemeralPath = p + 'election';
    self._ephemeralNode = self._ephemeralPath + '/' + self._id + '-';
    self._historyPath = p + 'history';
    self._clusterStatePath = p + 'state';

    //Filled in at init time
    self._inited = false;
    self._zk = null;
    self._active = [];
    self._clusterState = null;
    self._clusterStateVersion = null;
    self._closed = false;
    self._zkSetupState = 'uninit'; // For looking in memory dumps.

    self.__defineGetter__('active', function active() {
        return (self._active.map(function (a) {
            var c = {
                'id': a.id
            };
            Object.keys(a.data).forEach(function (k) {
                c[k] = a.data[k];
            });

            /**
             * A little backward compatible massaging here.  Newer versions of
             * manatee should already have the pgUrl and backupUrl fields.
             * Older may not.  So this makes sure the fields the postgres
             * manager needs are filled in...  the ugly had to go somewhere.
             * The ZK ids look like:
             *
             * [ip address]:[postgres port]:[backup port]
             *
             * So we can use that to construct the pg and backup urls as the
             * "old" manatee did.
             */
            var data;
            var ip;
            var port;
            if (!c.pgUrl) {
                data = a.id.split(':');
                ip = data[0];
                port = data[1] ? data[1] : '5432';
                c.pgUrl = 'tcp://postgres@' + ip + ':' + port + '/postgres';
            }
            if (!c.backupUrl) {
                data = a.id.split(':');
                ip = data[0];
                port = data[2] ? data[1] : '12345';
                c.backupUrl = 'http://' + ip + ':' + port;
            }
            //End backward compatible ugliness.

            return (c);
        }));
    });

    self.__defineGetter__('clusterState', function clusterState() {
        return (self._clusterState);
    });

    setImmediate(init.bind(self));

    log.trace('zk: new zookeeper manager', options);
}

module.exports = ZookeeperMgr;
util.inherits(ZookeeperMgr, EventEmitter);


// Helpers

/**
 * Zookeeper session to hex string.
 */
function sh(zk) {
    if (!zk) {
        return (null);
    }
    var s = zk.getSessionId();
    if (!s) {
        return (null);
    }
    return (s.toString('hex'));
}

/**
 * Some conditions lead to multiple entries for the same "manatee".  For
 * example, a process restarting will sever the connection to zk, then the
 * new process will establish a new session and, hence, a new ephemeral node.
 *
 * Since we don't care about "older" nodes for the list of actives, what this
 * does is parse out the id/sequence from the nodes, then filters out any
 * "older" nodes.  It returns a list in lexographical order of the ids.
 *
 * For example, this:
 *     [ 'a-10', 'b-25', 'a-5', 'c-10', 'c-5' ]
 *
 * Results in this:
 *     [ { "id": "a", "seq": 10, "name": "a-10" },
 *       { "id": "b", "seq": 25, "name": "b-25" },
 *       { "id": "c", "seq": 10, "name": "c-10" } ]
 */
function parseAndUniqueActives(list) {
    var max = {};
    list.map(function parse(a) {
        return ({
            'id': a.substring(0, a.lastIndexOf('-')),
            'seq': parseInt(a.substring(a.lastIndexOf('-') + 1), 10),
            'name': a
        });
    }).forEach(function (a) {
        if (!max[a.id] || a.seq > max[a.id]['seq']) {
            max[a.id] = a;
        }
    });
    return (Object.keys(max).sort().map(function (o) {
        return (max[o]);
    }));
}


/**
 * Zookeeper watches will only fire once.  That's right.  Once.  So anytime
 * a watch fires we need to re-register the watch.  Since the state could have
 * changed between the time the watch fired and the subsequent read, this will
 * emit if the read is different than the last emit.
 *
 * The wat function is called each time something changes.
 * The cb is only called once, after the watch is initally set with the initial
 * read.
 */
function watch(zk, getFunc, path, wat, cb) {
    var self = this;
    var log = self._log;

    cb = once(cb);
    //The event this gives back isn't what it changed to, only that it
    // changed.
    function watchFired(event) {
        if (self._closed) {
            return;
        }
        return (registerWatch());
    }
    function onGet(err, data, stat) {
        if (self._closed) {
            return;
        }
        var watRes = {
            data: data,
            version: stat ? stat.version : undefined
        };
        if (err && err.name === 'NO_NODE') {
            zk.exists(path, watchFired, function (err2, stat2) {
                //It was created while we were looking away.
                if (stat2) {
                    //There's already a watch set, so we get and return.
                    getFunc.call(zk, path, onGet);
                }
                return (cb(err2));
            });
        } else if (!cb.called) {
            return (cb(err, watRes));
        } else if (err) {
            log.error({
                path: path,
                err: err
            }, 'zk: error fetching zk data or children');
            if (err.code !== zkClient.Exception.CONNECTION_LOSS) {
                setTimeout(registerWatch, 5000);
            }
        } else {
            wat.call(self, watRes);
        }
    }
    function registerWatch() {
        if (self._closed) {
            return;
        }
        getFunc.call(zk, path, watchFired, onGet);
    }
    registerWatch();
}


/**
 * We define equals if the list of ids are equal.  It doens't look at any other
 * data within a and b.
 *
 * The problem with only comparing the ids is that we'll return false if the
 * rest of the data for a given node has changed.  For the moment, we don't
 * care since changes to the data can only mean new fields for operator
 * inspection and that's not something the state machine can care about.  We
 * can always do a deeper data inspection if this becomes an issue.
 */
function idListsEqual(a, b) {
    function extractId(d) {
        return (d.id);
    }

    if (!a || !b) {
        return (false);
    }

    a = a.map(extractId);
    b = b.map(extractId);

    if (a.length !== b.length) {
        return (false);
    }

    for (var i = 0; i < a.length; ++i) {
        if (a[i] !== b[i]) {
            return (false);
        }
    }

    return (true);
}


/**
 * Used as both the initial list handler and the function that handles when
 * the event is fired.
 */
function handleActive(res, zk, cb) {
    var self = this;
    var log = self._log;

    function handleError(err) {
        if (cb) {
            log.debug(err, 'zk: handleActive handleError cb');
            return (cb(err));
        } else {
            log.warn(err, 'zk: handleActive handleError no cb');
            return;
        }
    }

    log.debug({
        res: res,
        localZk: sh(zk),
        selfZk: sh(self._zk)
    }, 'zk: handling active init/notification');
    zk = zk ? zk : self._zk;
    if (!zk) {
        return (handleError(new Error('No zookeeper for fetching data.')));
    }

    if (!res) {
        log.debug(res, 'zk: returning, no res');
        return;
    }

    var active = parseAndUniqueActives(res.data);

    //We could compute this once and save it on self, but we're most likely
    // going to replace it each time.  It's probably a wash...
    var acache = {};
    self._active.forEach(function (a) {
        acache[a.id] = a;
    });

    //Populate the data for the active list.
    vasync.forEachParallel({
        'inputs': active,
        'func': function getData(c, subcb) {
            if (acache[c.id] && acache[c.id]['seq'] === c.seq) {
                return (subcb(null, acache[c.id]['data']));
            }
            zk.getData(self._ephemeralPath + '/' + c.name, function (err, d) {
                if (err) {
                    return (subcb(err));
                }
                return (subcb(null, JSON.parse(d.toString('utf8'))));
            });
        }
    }, function (err, r) {
        if (err) {
            return (handleError(err));
        }

        for (var i = 0; i < active.length; ++i) {
            active[i].data = r.operations[i].result;
        }

        log.debug({
            'inited': self._inited,
            'hasCb': cb ? true : false,
            'active': active,
            'zk': sh(zk)
        }, 'zk: end handling active init/notification');

        var shouldDebounce = idListsEqual(self._active, active);
        self._active = active;
        if (self._inited && !shouldDebounce) {
            self.emit('activeChange', self.active);
        }

        if (cb) {
            log.debug('zk: active calling cb');
            return (cb(null, self._active));
        }
    });
}


function handleClusterState(res) {
    var self = this;
    var log = self._log;

    if (!res) {
        log.debug(res, 'zk: no res handling cluster state init/notification');
        return;
    }

    // Update here so that the log entry is sane.
    res.data = res.data.toString('utf8');
    log.debug(res, 'zk: handling cluster state init/notification');
    self._clusterState = JSON.parse(res.data);
    self._clusterStateVersion = res.version;

    if (!self._inited) {
        return;
    }

    self.emit('clusterStateChange', self._clusterState);
}


function setupData(zk, cb) {
    var self = this;
    var log = self._log;

    log.debug('zk: setting up data');

    //Create directories
    function createEphemeralDirectory(_, subcb) {
        zk.mkdirp(self._ephemeralPath, subcb);
    }

    function createHistoryDirectory(_, subcb) {
        zk.mkdirp(self._historyPath, subcb);
    }

    //Watch the cluster state
    function watchClusterState(_, subcb) {
        function onGet(err, res) {
            if (!err) {
                handleClusterState.call(self, res);
            }
            log.debug(self._clusterStatePath, 'zk: watching path on init');
            return (subcb(err));
        }
        watch.call(self, zk,
                   zk.getData,
                   self._clusterStatePath,
                   handleClusterState,
                   onGet);
    }

    //Join and watch the ephemeral directory
    function joinEphemeralDirectory(_, subcb) {
        function onCreate(err, path) {
            if (err) {
                return (subcb(err));
            }
            log.debug(path, 'zk: joined at path on init');
            return (subcb());
        }
        zk.create(self._ephemeralNode,
                  new Buffer(JSON.stringify(self._data, null, 0)),
                  zkClient.CreateMode.EPHEMERAL_SEQUENTIAL,
                  onCreate);
    }

    function watchEphemeralDirectory(_, subcb) {
        function onList(err, res) {
            if (err) {
                return (subcb(err));
            }
            log.debug('zk: handling active on init');
            handleActive.call(self, res, zk, subcb);
        }
        watch.call(self, zk,
                   zk.getChildren,
                   self._ephemeralPath,
                   handleActive,
                   onList);
    }

    vasync.pipeline({
        'funcs': [
            createEphemeralDirectory,
            createHistoryDirectory,
            watchClusterState,
            joinEphemeralDirectory,
            watchEphemeralDirectory
        ]
    }, function (err) {
        log.debug('zk: done setting up data');
        return (cb(err));
    });
}


function init() {
    var self = this;
    var log = self._log;

    var emitInit = once(function emitInitFunc() {
        self._inited = true;
        self.emit('init', {
            active: self.active,
            clusterState: self.clusterState
        });
    });

    function setupZkClient() {
        self._zkSetupState = 'init';
        //https://github.com/alexguan/node-zookeeper-client#state
        var zk = zkClient.createClient(self._connStr, self._opts);
        self._zk = zk;

        var resetZkClientOnce = once(function resetZkClient() {
            log.debug(sh(zk), 'zk: resetting client');
            self._zkSetupState = 'resetting';
            if (zk) {
                self._zkSetupState = 'closed';
                zk.close();
            }
            setImmediate(setupZkClient);
        });
        var setupDataOnce = once(function setupZkData(cb) {
            log.debug(sh(zk), 'zk: setting up zk data');
            self._zkSetupState = 'data';
            setupData.call(self, zk, cb);
        });

        //In case other parts of the ZK manager decide that this needs to be
        // reset (should be far and few between)
        zk.reset = resetZkClientOnce;

        //Creator says this is "Java Style"
        zk.on('state', function (s) {
            //Just log it.  The other events are called.
            log.debug(sh(zk), s, 'zk: new state');
        });

        //Client is connected and ready. This fires whenever the client is
        // disconnected and reconnected (more than just the first time).
        zk.on('connected', function () {
            log.debug(sh(zk), 'zk: connected');
            setupDataOnce(function (err) {
                if (err) {
                    self._zkSetupState = 'dataError';
                    log.error(sh(zk), err,
                              'zk: err setting up data, reiniting');
                    return (resetZkClientOnce());
                } else {
                    log.debug(sh(zk), 'zk: client ready');
                    self._zkSetupState = 'ready';
                    emitInit();
                }
            });
        });

        //Client is connected to a readonly server.
        zk.on('connectedReadOnly', function () {
            //Don't do anything for this.
            log.debug(sh(zk), 'zk: connected read only');
        });

        //The connection between client and server is dropped.
        zk.on('disconnected', function () {
            //Don't do anything for this.
            log.debug(sh(zk), 'zk: disconnected');
        });

        //The client session is expired.
        zk.on('expired', function () {
            //This causes the client to "go away".  A new one should be
            // created after this.
            log.info(sh(zk), 'zk: session expired, reiniting.');
            resetZkClientOnce();
        });

        //Failed to authenticate with the server.
        zk.on('authenticationFailed', function () {
            //Don't do anything for this.
            log.fatal(sh(zk), 'zk: auth failed');
        });

        //Not even sure if this is really an error that would be emitted...
        zk.on('error', function (err) {
            //Create a new ZK.
            log.fatal(sh(zk), {err: err}, 'zk: unexpected error, reiniting');
            resetZkClientOnce();
        });

        zk.connect();
    }

    setupZkClient();
}


/**
 * Gets the Zookeeper Status.  See node-zookeeper-client/lib/State.js
 */
ZookeeperMgr.prototype.status = function status() {
    var self = this;
    if (!self._zk) {
        return ('UNINIT');
    } else {
        return (self._zk.getState().getName());
    }
};


/**
 * Put the cluster state.
 */
ZookeeperMgr.prototype.putClusterState = function putClusterState(state, cb) {
    assert.object(state, 'state');
    assert.number(state.generation, 'state.generation');

    var self = this;
    var data = new Buffer(JSON.stringify(state));

    function onPut(err, stat) {
        if (err) {
            return (cb(err));
        }
        self._clusterState = state;
        self._clusterStateVersion = stat.version;
        return (cb());
    }

    var hp = self._historyPath + '/' + state.generation + '-';
    var t = self._zk.transaction().
        create(hp, data, zkClient.CreateMode.PERSISTENT_SEQUENTIAL);
    if (self._clusterState) {
        t.setData(self._clusterStatePath, data, self._clusterStateVersion);
    } else {
        t.create(self._clusterStatePath, data, zkClient.CreateMode.PERSISTENT);
    }
    t.commit(onPut);
};


/**
 * Closes this ZK Manager down.
 *
 * If you call this, you'll need to instantiate a new one.
 */
ZookeeperMgr.prototype.close = function close(cb) {
    var self = this;
    var log = self._log;
    if (cb) {
        cb = once(cb);
    }

    if (self._zk) {
        log.info('zk: closing');
        self._zk.removeAllListeners();
        self._zk.on('error', function (err) {
            log.error(err, 'zk: error after close');
        });
        self._zk.on('disconnected', function () {
            if (cb) {
                return (cb());
            }
        });
        self._closed = true;
        self._zk.close();
    }
};


/** #@- */
