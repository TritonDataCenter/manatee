var path = require('path');
var util = require('util');

var assert = require('assert-plus');
var bunyan = require('bunyan');
var getopt = require('posix-getopt');
var pg = require('pg');
var vasync = require('vasync');
var zkplus = require('zkplus');


exports.loadTopology = loadTopology;
exports.createZkClient = createZkClient;
exports.printTopology = printTopology;
exports.pgStatus = pgStatus;


/**
 * Common manatee utils
 */


var Client = pg.Client;

var LOG = bunyan.createLogger({
        name: 'manatee_common',
        level: (process.env.LOG_LEVEL || 'fatal'),
        src: true,
        serializers: {
                err: bunyan.stdSerializers.err
        }
});

var PG_REPL_STAT = 'select * from pg_stat_replication;';
var PG_REPL_LAG = 'SELECT now() - pg_last_xact_replay_timestamp() AS time_lag;';


function compareNodeNames(a, b) {
        var seqA = parseInt(a.substring(a.lastIndexOf('-') + 1), 10);
        var seqB = parseInt(b.substring(b.lastIndexOf('-') + 1), 10);

        return (seqA - seqB);
}

function createZkClient(opts, cb) {
        var zk = zkplus.createClient({
                log: LOG,
                servers: [opts.zk],
                timeout: 30000
        });

        var timeoutId;
        zk.once('connect', function () {
                cb(null, zk);
                clearTimeout(timeoutId);
        });

        zk.connect();
        timeoutId = setTimeout(function() {
                console.error('zookeeper connect timed out');
                process.exit(0);
        }, 10000);
}

function formatNodes(nodes, zk, pathPrefix, cb) {
        var output = {};

        var count = 0;
        if (nodes.length === 0) {
                return cb();
        }
        for (var i = 0; i < nodes.length; i++) {
                var node = nodes[i];
                LOG.debug({
                        node: pathPrefix + '/' + node
                }, 'getting node');
                var zkGetCb = function(j, err, obj) {
                        switch(j) {
                                case 0:
                                        output['primary'] = !err ? {
                                                ip: obj.ip,
                                                pgUrl: transformPgUrl(obj.ip),
                                                zoneId: obj.zoneId
                                        } : JSON.stringify(err);
                                        break;
                                case 1:
                                        output['sync'] = !err ? {
                                                ip: obj.ip,
                                                pgUrl: transformPgUrl(obj.ip),
                                                zoneId: obj.zoneId
                                        } : JSON.stringify(err);
                                        break;
                                case 2:
                                        output['async'] = !err ? {
                                                ip: obj.ip,
                                                pgUrl: transformPgUrl(obj.ip),
                                                zoneId: obj.zoneId
                                        } : JSON.stringify(err);
                                        break;
                                default:
                                        var asyncNumber = j - 2;
                                        output['async' + asyncNumber] = !err ? {
                                                ip: obj.ip,
                                                pgUrl: transformPgUrl(obj.ip),
                                                zoneId: obj.zoneId
                                        } : JSON.stringify(err);
                                        break;
                        }
                        count++;
                        LOG.trace({
                                count: count,
                                nodeLength: nodes.length
                        }, 'count is');
                        if (count === nodes.length) {
                                return cb(null, output);
                        }

                        return (undefined);
                };
                // bind the function at invocation time to capture i
                zk.get(pathPrefix + '/' + node, zkGetCb.bind(null, i));
        }

        return (undefined);
}

function loadTopology(zk, callback) {
        var tasks = [
                function listAllShards(arg, cb) {
                        zk.readdir('/manatee', function(err, shards) {
                                arg.shards = shards;
                                cb(err);
                        });
                },
                function shardStatus(arg, cb) {
                        var done = false;
                        function _cb(err) {
                                if (done)
                                        return;
                                done = true;
                                cb(err);
                        }

                        var count = 0;
                        arg.shards.forEach(function (s) {
                                var p = '/manatee/' + s + '/election';
                                zk.readdir(p, function (err, nodes) {
                                        if (err) {
                                                _cb(err);
                                                return (undefined);
                                        }
                                        LOG.debug({
                                                nodes: nodes
                                        }, 'got nodes');

                                        nodes.sort(compareNodeNames);
                                        formatNodes(nodes, zk, p, function(err2, output) {
                                                if (err2) {
                                                        return cb(err2);
                                                }
                                                topology[s] = output ? output : {};
                                                if (++count === arg.shards.length) {
                                                        return cb();
                                                }

                                                return (undefined);
                                        });

                                        return (undefined);
                                });
                        });
                },
                function registrarStatus(arg, cb) {
                        var count = 0;
                        arg.shards.forEach(function (s) {
                                var path2 = '/' +
                                           s.split('.').reverse().join('/') +
                                           '/pg';
                                zk.get(path2, function(err, object) {
                                        topology[s].registrar = object;
                                        count++;

                                        if (count === arg.shards.length)
                                                cb();
                                });
                        });
                },
                function errorStatus(arg, cb) {
                        var count = 0;
                        arg.shards.forEach(function (s) {
                                var p = '/manatee/' + s + '/error';
                                zk.get(p, function (err, object) {
                                        count++;
                                        if (object) {
                                                topology[s].error = object;
                                        }

                                        if (count === arg.shards.length)
                                                cb();
                                });
                        });
                }
        ];
        var topology = {};

        vasync.pipeline({
                funcs: tasks,
                arg: {}
        }, function(err) {
                if (err) {
                        LOG.error(err, 'loadTopology: error');
                } else {
                        LOG.debug(topology, 'loadTopology: done');
                }
                callback(err, topology);
        });
}

function pgStatus(topology, callback) {
        LOG.debug({
                topology: topology
        }, 'pgStatus: entered');

        var count = 0;
        var shards = Object.keys(topology);
        var total = 0;

        shards.forEach(function (s) {
                var peers = Object.keys(topology[s]);
                total += peers.length;
                LOG.debug({peers: peers, shard: s}, 'got peers');

                process.nextTick(peers.forEach.bind(peers, function (k) {
                        if (k === 'error' || k === 'registrar') {
                                LOG.debug('skipping error/registrar node');
                                if (++count === total)
                                        callback();
                                return;
                        }
                        var node = topology[s][k];
                        LOG.debug({
                                node: node
                        }, 'current shard is');
                        if (!node) {
                                LOG.debug('no node, skipping pgStatus');
                                if (++count === total)
                                        callback();
                                return;
                        }
                        var url = node.pgUrl;

                        var q = k !== 'async' ? PG_REPL_STAT : PG_REPL_LAG;

                        LOG.debug({
                                url: url
                        }, 'querying postgres status');
                        query(url, q, function (err, res) {
                                if (err) {
                                        node.repl = JSON.stringify(err);
                                } else {
                                        node.repl =
                                                res.rows[0] ? res.rows[0] : {};
                                }

                                if (++count === total) {
                                        callback();
                                }
                        });
                }));
        });
}

function printTopology(opts, topology) {
        if (opts.shard) {
                if (!topology[opts.shard]) {
                        console.error(opts.shard + ' not found');
                        console.log(JSON.stringify(topology, null, 4));
                        process.exit(1);
                }
                console.log(JSON.stringify(topology[opts.shard], null, 4));
        } else {
                console.log(JSON.stringify(topology, null, 4));
        }
        process.exit(0);
}

function query(url, _query, callback) {
        LOG.debug({
                url: url,
                query: _query
        }, 'query: entering.');

        var client = new Client(url);
        client.connect(function (err) {
                if (err) {
                        return callback(err);
                }
                LOG.debug({
                        sql: _query
                }, 'query: connected to pg, executing sql');
                client.query(_query, function(err2, result) {
                        client.end();
                        return callback(err2, result);
                });

                return (undefined);
        });
}

function transformPgUrl(url) {
        if (!url) {
                return ('');
        }

        return ('tcp://postgres@' + url + ':5432/postgres');
}
