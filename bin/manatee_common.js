#!/usr/bin/env node
// -*- mode: js -*-
// Display status of the manatee shards

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

///--- Globals

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

        zk.once('connect', function () {
                cb(null, zk);
        });

        zk.connect();
}

function getNodeZoneId(zk, node, cb) {
        LOG.debug({
                node: node
        }, 'getting zoneid for node');
        zk.get(node, function(err, obj) {
                return cb(err, obj.zoneId);
        });
}

function formatNodes(nodes, zk, pathPrefix, cb) {
        var output = {};

        var count = 0;
        for (var i = 0; i < nodes.length; i++) {
                var node = nodes[i];
                LOG.debug({
                        node: pathPrefix + '/' + node
                }, 'getting node');
                var zkGetCb = function(i, err, obj) {
                        if (err) {
                                return cb(err);
                        }
                        switch(i) {
                                case 0:
                                        output['primary'] = {
                                                ip: node.split('-')[0],
                                                pgUrl: transformPgUrl(node),
                                                zoneId: obj.zoneId
                                        };
                                        break;
                                case 1:
                                        output['sync'] = {
                                                ip: node.split('-')[0],
                                                pgUrl: transformPgUrl(node),
                                                zoneId: obj.zoneId
                                        };
                                        break;
                                case 2:
                                        output['async'] = {
                                                ip: node.split('-')[0],
                                                pgUrl: transformPgUrl(node),
                                                zoneId: obj.zoneId
                                        };
                                        break;
                                default:
                                        var asyncNumber = currNode - 2;
                                        output['async' + asyncNumber] = {
                                                ip: node.split('-')[0],
                                                pgUrl: transformPgUrl(node),
                                                zoneId: obj.zoneId
                                        };
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
                }
                // bind the function at invocation time to capture i
                zk.get(pathPrefix + '/' + node, zkGetCb.bind(null, i));
        }
        return (undefined);
}

function ifError(err) {
        if (err) {
                console.error(err.toString());
                process.exit(1);
        }
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
                                                topology[s] = output;
                                                count++;
                                                if (count === arg.shards.length) {
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

                process.nextTick(peers.forEach.bind(peers, function (k) {
                        var node = topology[s][k];
                        var url = node.pgUrl;

                        if (!url) {
                                if (++count === total)
                                        callback();
                                return;
                        }

                        var q = k !== 'async' ? PG_REPL_STAT : PG_REPL_LAG;

                        query(url, q, function (err ,res) {
                                ifError(err);

                                node.repl = res.rows[0] ? res.rows[0] : {};
                                if (++count === total)
                                        callback();
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
                ifError(err);
                LOG.debug({
                        sql: _query
                }, 'query: connected to pg, executing sql');
                client.query(_query, function(err2, result) {
                        ifError(err2);
                        client.end();
                        callback(err2, result);
                });
        });
}

function transformPgUrl(url) {
        if (!url)
                return ('');

        return ('tcp://postgres@' + url.split('-')[0] + ':5432/postgres');
}
