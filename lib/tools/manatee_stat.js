#!/usr/bin/env node
/* -*- mode: js -*-
 *
 * Display status of the manatee shards
 *
 */
var assert = require('assert-plus');
var bunyan = require('bunyan');
var getopt = require('posix-getopt');
var pg = require('pg');
var Client = pg.Client;
var util = require('util');
var vasync = require('vasync');
var zkplus = require('zkplus');


var NAME = 'manatee_stat';
var LOG = bunyan.createLogger({
        name: NAME,
        level: (process.env.LOG_LEVEL || 'info'),
        serializers: {
                err: bunyan.stdSerializers.err
        },
        src: true
});

var PG_REPL_STAT_QUERY = 'select * from pg_stat_replication;';
var PG_XLOG_LOCATION_QUERY = 'SELECT pg_current_xlog_location();';

function parseOptions() {
        var option;
        var opts = {};
        var parser = new getopt.BasicParser('z:(zkurl)s:(shard)', process.argv);

        while ((option = parser.getopt()) !== undefined) {
                switch (option.option) {
                case 'z':
                        opts.zkUrl = option.optarg;
                        break;
                case 's':
                        opts.shard = option.optarg;
                        break;
                default:
                        process.exit(1);
                        break;
                }
        }

        if (!opts.zkUrl) {
                console.log('usage: manatee_stat.js -z 10.99.99.111:2181' +
                            ' -s shardpath');
                process.exit(1);
        }
        return (opts);
}

var _options = parseOptions();

var _zk = zkplus.createClient({
        log: LOG,
        servers: [_options.zkUrl]
});

var topology = {};
_zk.on('connect', function() {
        LOG.debug('zk connected');
        getPgPeers(function(err, nodes) {
                if (err) {
                        throw err;
                }
                checkPgReplState(function() {
                        printShardTopology(nodes);
                        process.exit(0);
                });
        });
});

function printShardTopology() {
        for (var node in topology) {
                console.log('----------------------------------------------');
                console.log('shard: ' + node);
                console.log('----------------------------------------------');
                var shard = topology[node];
                for (var i = 0; i < shard.length; i++) {
                        var n = JSON.stringify(shard[i], null, 4);
                        switch (i) {
                                case 0:
                                        console.log('primary: ' + n);
                                        break;
                                case 1:
                                        console.log('sync: ' + n);
                                        break;
                                case 2:
                                        console.log('async: ' + n);
                                        break;
                                default:
                                        console.log('additional-async: ' + n);
                                        break;
                        }
                }
                console.log('----------------------------------------------');
                console.log('\n');
        }
}

function getPgPeers(callback) {
        var tasks = [
                function _getShards(_, cb) {
                        if (_options.shard) {
                                return cb();
                        }
                        _zk.readdir('/manatee', function(err, shards) {
                                _.shards = shards;
                                return cb(err);
                        });

                        return true;
                },
                function _getShardElectionStat(_, cb) {
                        if (_options.shard) {
                                return cb();
                        }
                        var shards = _.shards;
                        var count = 0;
                        shards.forEach(function(shard) {
                                var p = '/manatee/' + shard;
                                _zk.readdir(p, function(err, nodes) {
                                        if (err) {
                                                return cb(err);
                                        }
                                        nodes.sort(compare);
                                        topology[shard] = nodes;
                                        count++;
                                        if (count === shards.length) {
                                                return cb();
                                        }

                                        return true;
                                });
                        });

                        return true;
                }, function _getSingleShard(_, cb) {
                        if (!_options.shard) {
                                return cb();
                        }
                        var p = '/manatee/' + _options.shard;
                        _zk.readdir(p, function(err, nodes) {
                                if (err) {
                                        console.error('shard dne');
                                        process.exit(1);
                                }
                                nodes.sort(compare);
                                topology[_options.shard] = nodes;
                                return cb();
                        });

                        return true;
                }
        ];

        vasync.pipeline({funcs: tasks, arg: {}}, function(err) {
                if (err) {
                        LOG.error({
                                err: err
                        }, 'getPgPeers: error');
                } else {
                        LOG.debug({
                                topology: topology
                        }, 'got topology');
                }

                return callback(err, topology);
        });

}

function checkPgReplState(callback) {
        LOG.debug({
                topology: topology
        }, 'entering checkPgReplState');
        var totalNodes = 0;
        for (var node in topology) {
                totalNodes += topology[node].length;
        }

        if (totalNodes === 0) {
                return callback();
        }

        var count = 0;
        for (node in topology) {
                var nodes = topology[node];
                for (var i = 0; i < nodes.length; i++) {
                        var url = transformPgUrl(nodes[i]);
                        queryDb(url, PG_REPL_STAT_QUERY, node, i,
                                function(err, url2, node2, i2, result)
                        {
                                LOG.debug({
                                        err: err,
                                        url: url2,
                                        result: result
                                }, 'got result:');

                                topology[node2][i2] = {
                                        url: url2,
                                        slave: err || result.rows[0]
                                };

                                count++;
                                if (count === totalNodes) {
                                        return callback();
                                }

                                return true;
                        });
                }
        }

        return true;
}

function queryDb(url, query, node, i, callback) {
        LOG.debug({
                url: url,
                query: query
        }, 'Postgresman.query: entering.');

        var client = new Client(url);
        client.connect(function(err) {
                if (err) {
                        LOG.debug({err: err},
                                 'Postgresman.query: can\'t connect to pg');
                        client.end();
                        return callback(err);
                }
                LOG.debug({
                        query: query
                }, 'Postgresman.query: connected to pg, executing query');
                client.query(query, function(err2, result) {
                        client.end();
                        if (err2) {
                                LOG.error({ err: err2 },
                                         'error whilst querying pg');
                        }
                        return callback(err2, url, node, i, result);
                });

                return true;
        });
}

function compare(a, b) {
        var seqA = parseInt(a.substring(a.lastIndexOf('-') + 1), 10);
        var seqB = parseInt(b.substring(b.lastIndexOf('-') + 1), 10);

        return (seqA - seqB);
}

function transformPgUrl(url) {
        if (!url) {
                return '';
        }
        LOG.debug({
                url: url
        }, 'entering transform url');
        return 'tcp://postgres@' + url.split('-')[0] + ':' +
                5432 + '/postgres';
}
