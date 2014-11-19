/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2014, Joyent, Inc.
 */

/*
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
var bunyan = require('bunyan');
var childProcess = require('child_process');
var fs = require('fs');
var vasync = require('vasync');
var zkClient = require('node-zookeeper-client');
var ZkMgr = require('../lib/zookeeperMgr');

/**
 * Testing the ZK Manager.  To run this test you'll need to provide the
 * connection string to a zookeeper.  It will do all work under the
 * "/manatee_test" directory in that zookeeper.  For example:
 *
 *     $ ./node_modules/.bin/nodeunit ./test/zookeeperMgr.test.js
 *
 * Configuration is located in ./test/etc/zookeeperMgr.test.cfg which doesn't
 * exist by default.  You should copy the template file and replace fields with
 * valid data.
 */
var LOG = bunyan.createLogger({
        level: (process.env.LOG_LEVEL || 'fatal'),
        name: 'zookeeperMgr.test.js',
        serializers: {
                err: bunyan.stdSerializers.err
        },
        src: true
});

var PATH_PREFIX = '/manateeTest';
var CFG_FILE = __dirname + '/etc/zookeeperMgr.test.cfg';
var CFG = JSON.parse(fs.readFileSync(CFG_FILE));
var CONN_STR = CFG.connStr;
var ZK_OPTS = {
        sessionTimeout: 2000,
        spinDelay: 1000,
        retries: 0
};


//Helpers
function getZkManager(testName, id, onActive, onState, cb) {
    var zopts = {
        'id': id,
        'data': {
            'zoneId': 'localhost',
            'ip': '127.0.0.1'
        },
        'path': PATH_PREFIX + '/' + testName,
        'connStr': CONN_STR,
        'opts': ZK_OPTS,
        'log': LOG
    };

    var zk = new ZkMgr(zopts);
    zk.on('init', function (init) {
        return (cb(null, zk, init));
    });
    zk.on('activeChange', onActive);
    zk.on('clusterStateChange', onState);
}

function activeFromArray(a) {
    return (a.map(function (id) {
        return ({
            'id': id,
            'zoneId': 'localhost',
            'ip': '127.0.0.1'
        });
    }));
}

function setupZkClient(_, cb) {
    var zk = zkClient.createClient(CONN_STR, ZK_OPTS);
    zk.once('connected', function () {
        _.zk = zk;
        return (cb());
    });
    zk.connect();
}


function setupManagers(_, cb) {
    vasync.forEachParallel({
        'inputs': _.managerCfgs,
        'func': function setupManager(m, subcb) {
            m.onActive = m.onActive || function () {};
            m.onState = m.onState || function () {};
            function onInit(err, mgr) {
                if (!err) {
                    if (!_.managers) {
                        _.managers = {};
                    }
                    _.managers[m.id] = mgr;
                }
                return (subcb(err, mgr));
            }
            getZkManager(_.test, m.id, m.onActive, m.onState, onInit);
        }
    }, function (err) {
        return (cb(err));
    });
}


function rmrfTestData(_, p, cb) {
    if (p === null) {
        p = PATH_PREFIX + '/' + _.test;
    }
    _.zk.getChildren(p, function (err, children) {
        if (err && err.name === 'NO_NODE') {
            return (cb());
        }
        vasync.forEachPipeline({
            'inputs': children,
            'func': function (c, subcb) {
                rmrfTestData(_, p + '/' + c, subcb);
            }
        }, function (err2) {
            _.zk.remove(p, cb);
        });
    });
}


function teardown(_, cb) {
    rmrfTestData(_, null, function (err) {
        _.zk.close();
        Object.keys(_.managers).forEach(function (m) {
            _.managers[m].close();
        });
        return (cb());
    });
}


function readHistory(zk, p, cb) {
    function parseSeq(a) {
        return (parseInt(a.substring(a.lastIndexOf('-') + 1), 10));
    }
    zk.getChildren(p, function (err, children) {
        children.sort(function (a, b) {
            return (parseSeq(a) - parseSeq(b));
        });
        if (err) {
            return (cb(err));
        }
        vasync.forEachParallel({
            'inputs': children,
            'func': function (c, subcb) {
                zk.getData(p + '/' + c, subcb);
            }
        }, function (err2, res) {
            var ret = [];
            res.operations.forEach(function (o) {
                ret.push(JSON.parse(o.result.toString('utf8')));
            });
            return (cb(null, ret));
        });
    });
}



//Tests
exports.testSetup = function (t) {
    var opts = {
        'test': 'testSetup',
        'managerCfgs': [ {
            id: 'setu'
        } ]
    };
    vasync.pipeline({
        'arg': opts,
        'funcs': [
            setupZkClient,
            setupManagers,
            function checkHistoryExists(_, subcb) {
                var p = PATH_PREFIX + '/testSetup/history';
                _.zk.exists(p, function (err, stat) {
                    if (err) {
                        t.fail(err);
                        return (subcb(err));
                    }
                    if (!stat) {
                        var e = new Error('no history directory in zk');
                        t.fail(e);
                        return (subcb(err));
                    }
                    return (subcb());
                });
            },
            function checkEphemeralExists(_, subcb) {
                var p = PATH_PREFIX + '/testSetup/election';
                _.zk.getChildren(p, function (err, children) {
                    if (err) {
                        return (subcb(err));
                    }
                    t.ok(children, 'no children');
                    t.equal(1, children.length, 'children.length isnt 1');
                    t.strictEqual('setu-', children[0].substring(0, 5));
                    _.managerZkPath = p + '/' + children[0];
                    return (subcb());
                });
            },
            function checkEphemeralContents(_, subcb) {
                _.zk.getData(_.managerZkPath, function (err, data) {
                    if (err) {
                        return (subcb(err));
                    }
                    t.ok(data, 'no data');
                    t.deepEqual(JSON.parse(data.toString('utf8')), {
                        'zoneId': 'localhost',
                        'ip': '127.0.0.1'
                    });
                    return (subcb());
                });
            },
            teardown
        ]
    }, function (err) {
        if (err) {
            t.fail(err);
        }
        t.done();
    });
};


exports.testCreateUpdateState = function (t) {
    var stateCb = null;
    var opts = {
        'test': 'testCreateUpdateState',
        'managerCfgs': [ {
            id: 'crea',
            onState: function (state) {
                stateCb(state);
            }
        } ]
    };
    var p = PATH_PREFIX + '/testCreateUpdateState/state';
    vasync.pipeline({
        'arg': opts,
        'funcs': [
            setupZkClient,
            setupManagers,
            function writeState(_, subcb) {
                var newState = { 'foo': 'bar' };
                var emitted = false;
                var cbed = false;
                var error;
                function tryEnd() {
                    if (error || (emitted && cbed)) {
                        t.deepEqual(newState, _.managers['crea'].clusterState,
                                    'new state doesnt match manager state');
                        t.deepEqual(activeFromArray(['crea']),
                                    _.managers['crea'].active,
                                    'active doesnt match manager');
                        return (subcb());
                    }
                }
                stateCb = function (state) {
                    emitted = true;
                    t.ok(state, 'state');
                    t.deepEqual(state, newState,
                                'new state doesnt match emitted');
                    return (tryEnd());
                };
                var newStateString = new Buffer(
                    JSON.stringify(newState, null, 0));
                _.zk.create(p, newStateString, function (err) {
                    if (err) {
                        t.fail(err);
                        error = err;
                    }
                    cbed = true;
                    return (tryEnd());
                });
            },
            function updateState(_, subcb) {
                var newState = { 'bar': 'baz' };
                var emitted = false;
                var cbed = false;
                var error;
                function tryEnd() {
                    if (error || (emitted && cbed)) {
                        t.deepEqual(newState, _.managers['crea'].clusterState,
                                    'new state doesnt match manager state');
                        t.deepEqual(activeFromArray(['crea']),
                                    _.managers['crea'].active,
                                    'active doesnt match manager');
                        return (subcb());
                    }
                }
                stateCb = function (state) {
                    emitted = true;
                    t.ok(state, 'state');
                    t.deepEqual(state, newState,
                                'new state doesnt match emitted');
                    return (tryEnd());
                };
                var newStateString = new Buffer(
                    JSON.stringify(newState, null, 0));
                _.zk.setData(p, newStateString, function (err) {
                    if (err) {
                        t.fail(err);
                        error = err;
                    }
                    cbed = true;
                    return (tryEnd());
                });
            },
            function updateState2(_, subcb) {
                var newState = { 'bas': 'bang' };
                var emitted = false;
                var cbed = false;
                var error;
                function tryEnd() {
                    if (error || (emitted && cbed)) {
                        t.deepEqual(newState, _.managers['crea'].clusterState,
                                    'new state doesnt match manager state');
                        t.deepEqual(activeFromArray(['crea']),
                                    _.managers['crea'].active,
                                    'active doesnt match manager');
                        return (subcb());
                    }
                }
                stateCb = function (state) {
                    emitted = true;
                    t.ok(state, 'state');
                    t.deepEqual(state, newState,
                                'new state doesnt match emitted');
                    return (tryEnd());
                };
                var newStateString = new Buffer(
                    JSON.stringify(newState, null, 0));
                _.zk.setData(p, newStateString, function (err) {
                    if (err) {
                        t.fail(err);
                        error = err;
                    }
                    cbed = true;
                    return (tryEnd());
                });
            },
            teardown
        ]
    }, function (err) {
        if (err) {
            t.fail(err);
        }
        t.done();
    });
};


exports.testAddRemoveAnother = function (t) {
    var activeCb = null;
    var opts = {
        'test': 'testAddRemoveAnother',
        'managerCfgs': [ {
            id: 'addr',
            onActive: function (active) {
                activeCb(active);
            }
        } ]
    };
    vasync.pipeline({
        'arg': opts,
        'funcs': [
            setupZkClient,
            setupManagers,
            function addOne(_, subcb) {
                var emitted = false;
                var cbed = false;
                var error;
                function tryEnd() {
                    if (error || (emitted && cbed)) {
                        if (error) {
                            t.fail(error);
                            return (subcb(error));
                        }
                        t.deepEqual(activeFromArray(['addr', 'newr']),
                                    _.managers['addr'].active,
                                    'active doesnt match manager');
                        return (subcb());
                    }
                }
                activeCb = function (active) {
                    emitted = true;
                    t.ok(active, 'active');
                    t.deepEqual(activeFromArray(['addr', 'newr']), active,
                                'unexpected list of actives');
                    return (tryEnd());
                };
                function onGet(err, newMgr) {
                    cbed = true;
                    _.newMgr = newMgr;
                    return (tryEnd());
                }
                getZkManager('testAddRemoveAnother', 'newr',
                             function () {}, function () {}, onGet);
            },
            function closeNewMgr(_, subcb) {
                activeCb = function (active) {
                    t.deepEqual(activeFromArray(['addr']), active,
                                'unexpected list of actives');
                    return (subcb());
                };
                _.newMgr.close();
                return (subcb());
            },
            function removeActiveCb(_, subcb) {
                activeCb = function () {};
                return (subcb());
            },
            teardown
        ]
    }, function (err) {
        if (err) {
            t.fail(err);
        }
        t.done();
    });
};


exports.testDebounce = function (t) {
    var activeCb = null;
    var opts = {
        'test': 'testDebounce',
        'managerCfgs': [ {
            id: 'debo',
            onActive: function (active) {
                activeCb(active);
            }
        } ]
    };
    vasync.pipeline({
        'arg': opts,
        'funcs': [
            setupZkClient,
            setupManagers,
            function addRemoveMany(_, subcb) {
                var error;
                var called = 0;
                var outstanding = 0;
                function tryEnd() {
                    if (error || (outstanding === 0 && called >= 2)) {
                        if (error) {
                            t.fail(error);
                            return (subcb(error));
                        }
                        t.deepEqual(activeFromArray(['debo']),
                                    _.managers['debo'].active,
                                    'active doesnt match manager');
                        return (subcb());
                    }
                }
                //Should see exactly two events: one add and one remove.
                activeCb = function (active) {
                    ++called;
                    t.ok(active, 'active');
                    if (called === 1) {
                        t.deepEqual(activeFromArray(['debo', 'newr']), active,
                                    'unexpected list of actives');
                    } else if (called === 2) {
                        t.deepEqual(activeFromArray(['debo']), active,
                                    'unexpected list of actives');
                    } else {
                        t.fail('Called more than twice');
                    }
                    return (tryEnd());
                };
                var i = 0;
                function createNext() {
                    if (++i === 10) {
                        return;
                    }
                    function onGet(err, newMgr) {
                        --outstanding;
                        newMgr.close();
                        return (tryEnd());
                    }
                    ++outstanding;
                    getZkManager('testDebounce', 'newr',
                                 function () {}, function () {}, onGet);
                }
                createNext();
            },
            function removeActiveCb(_, subcb) {
                activeCb = function () {};
                return (subcb());
            },
            teardown
        ]
    }, function (err) {
        if (err) {
            t.fail(err);
        }
        t.done();
    });
};


exports.testOrdering = function (t) {
    var ord2ActiveCb = null;
    var m1 = {
        id: 'ord1'
    };
    var m2 = {
        id: 'ord2',
        onActive: function (active) {
            ord2ActiveCb(active);
        }
    };
    var opts = {
        'test': 'testOrdering',
        'managerCfgs': [ m1, m2 ]
    };
    vasync.pipeline({
        'arg': opts,
        'funcs': [
            setupZkClient,
            setupManagers,
            function killAndReplaceFirst(_, subcb) {
                ord2ActiveCb = function onClose(active) {
                    t.deepEqual(activeFromArray(['ord2']), active,
                                'unexpected list of actives');

                    var cbed = false;
                    var emitted = false;
                    var error = null;
                    function tryEnd() {
                        if (error || (emitted && cbed)) {
                            return (subcb());
                        }
                    }

                    ord2ActiveCb = function onEmit(active2) {
                        emitted = true;
                        t.deepEqual(activeFromArray(['ord1', 'ord2']), active2,
                                    'unexpected list of actives');
                        return (tryEnd());
                    };

                    function onGet(err, newMgr) {
                        cbed = true;
                        if (err) {
                            t.fail(err);
                            return (subcb(err));
                        }
                        _.managers['ord1'] = newMgr;
                        return (tryEnd());
                    }
                    getZkManager('testOrdering', 'ord1',
                                 function () {}, function () {}, onGet);
                };
                _.managers['ord1'].close();
            },
            function checkMgrs(_, subcb) {
                t.deepEqual(activeFromArray(['ord1', 'ord2']),
                            _.managers['ord1'].active,
                            'unexpected list of actives');
                t.deepEqual(activeFromArray(['ord1', 'ord2']),
                            _.managers['ord2'].active,
                            'unexpected list of actives');
                return (subcb());
            },
            function removeActiveCb(_, subcb) {
                ord2ActiveCb = function () {};
                return (subcb());
            },
            teardown
        ]
    }, function (err) {
        if (err) {
            t.fail(err);
        }
        t.done();
    });
};


exports.testWriteHistoryNode = function (t) {
    var opts = {
        'test': 'testWriteHistoryNode',
        'managerCfgs': [ {
            id: 'wrih'
        } ]
    };
    var p = PATH_PREFIX + '/testWriteHistoryNode/history';
    var s1 = {
        'generation': 0,
        'foo': 'bar'
    };
    var s2 = {
        'generation': 1,
        'foo': 'baz'
    };
    vasync.pipeline({
        'arg': opts,
        'funcs': [
            setupZkClient,
            function clearPreviousTestData(_, subcb) {
                rmrfTestData(_, p, function (err) {
                    return (subcb());
                });
            },
            setupManagers,
            function putState(_, subcb) {
                _.managers['wrih'].putClusterState(s1, function (err) {
                    if (err) {
                        t.fail(err);
                    }
                    return (subcb(err));
                });
            },
            function checkHistory(_, subcb) {
                readHistory(_.zk, p, function (err, hist) {
                    if (err) {
                        return (subcb(err));
                    }
                    t.ok(hist, 'no history');
                    t.equal(1, hist.length, 'history has more than one entry');
                    t.deepEqual(s1, hist[0],
                                'history wasnt recorded properly');
                    return (subcb());
                });
            },
            function writeAnother(_, subcb) {
                _.managers['wrih'].putClusterState(s2, function (err) {
                    if (err) {
                        t.fail(err);
                    }
                    return (subcb(err));
                });
            },
            function checkHistory2(_, subcb) {
                readHistory(_.zk, p, function (err, hist) {
                    if (err) {
                        return (subcb(err));
                    }
                    t.ok(hist, 'no history');
                    t.equal(2, hist.length, 'history must have 2 entries');
                    t.deepEqual(s1, hist[0],
                                'history wasnt recorded properly');
                    t.deepEqual(s2, hist[1],
                                'history wasnt recorded properly');
                    return (subcb());
                });
            },
            teardown
        ]
    }, function (err) {
        if (err) {
            t.fail(err);
        }
        t.done();
    });
};


exports.testFailWriteClusterState = function (t) {
    var opts = {
        'test': 'testFailWriteClusterState',
        'managerCfgs': [ {
            id: 'faiw'
        } ]
    };
    var s = {
        'generation': 0,
        'foo': 'bar'
    };
    var p = PATH_PREFIX + '/testFailWriteClusterState/history';
    vasync.pipeline({
        'arg': opts,
        'funcs': [
            setupZkClient,
            setupManagers,
            function putState(_, subcb) {
                _.managers['faiw'].putClusterState(s, function (err) {
                    if (err) {
                        t.fail(err);
                    }
                    return (subcb(err));
                });
            },
            function failPut(_, subcb) {
                //Dirty to reach in like this... but it works.
                _.managers['faiw']._clusterStateVersion = 19;
                _.managers['faiw'].putClusterState(s, function (err) {
                    t.ok(err, 'should have returned error');
                    t.equal('BAD_VERSION', err.name, 'Expected BAD_VERSION');
                    return (subcb());
                });
            },
            function checkHistory(_, subcb) {
                readHistory(_.zk, p, function (err, hist) {
                    if (err) {
                        return (subcb(err));
                    }
                    t.ok(hist, 'no history');
                    t.equal(1, hist.length, 'history must have 1 entry');
                    t.deepEqual(s, hist[0],
                                'history wasnt recorded properly');
                    return (subcb());
                });
            },
            teardown
        ]
    }, function (err) {
        if (err) {
            t.fail(err);
        }
        t.done();
    });
};
