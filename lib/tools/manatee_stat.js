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
        level: (process.env.LOG_LEVEL || 'fatal'),
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
        var parser = new getopt.BasicParser('z:(zkurl)s:(shard)jp',
                                            process.argv);

        while ((option = parser.getopt()) !== undefined) {
                switch (option.option) {
                case 'z':
                        opts.zkUrl = option.optarg;
                        break;
                case 's':
                        opts.shard = option.optarg;
                        break;
                case 'p':
                        opts.postgres = true;
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
        servers: [_options.zkUrl],
        timeout: 2000
});

_zk.on('error', function(err) {
        console.error('zk error', err);
        process.exit(err.code);
});

var topology = {};
_zk.on('connect', function() {
        LOG.debug('zk connected');
        getShardPeers(function(err) {
                if (err) {
                        throw err;
                }
                checkPgReplState(function() {
                        printShardTopology();
                        process.exit(0);
                });
        });
});

_zk.connect();

function printShardTopology() {
        console.log(JSON.stringify(topology));
        return;
}

function getShardPeers(callback) {
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
                                        topology[shard] =
                                                formatNodes(nodes);
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
                                topology[_options.shard] = formatNodes(nodes);

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

        function formatNodes(nodes) {
                var output = {};
                for (var i = 0; i < nodes.length; i++) {
                        var node = nodes[i];
                        switch(i) {
                                case 0:
                                        output['primary'] = {
                                                url: node.split('-')[0],
                                                pgUrl: transformPgUrl(node)
                                        };
                                        break;
                                case 1:
                                        output['sync'] = {
                                                url: node.split('-')[0],
                                                pgUrl: transformPgUrl(node)
                                        };
                                        break;
                                case 2:
                                        output['async'] = {
                                                url: node.split('-')[0],
                                                pgUrl: transformPgUrl(node)
                                        };
                                        break;
                                default:
                                        var asyncNumber = i - 2;
                                        output['async' + asyncNumber] = {
                                                url: node.split('-')[0],
                                                pgUrl: transformPgUrl(node)
                                        };
                                        break;
                        }
                }

                return output;
        }
}

function checkPgReplState(callback) {
        if (!_options.postgres) {
                return callback();
        }
        LOG.debug({
                topology: topology
        }, 'entering checkPgReplState');

        var totalNodes = 0;
        for (var shard in topology) {
                for (var node in topology[shard]) {
                        totalNodes++;
                }
        }

        if (totalNodes === 0) {
                return callback();
        }

        var count = 0;
        for (var shard in topology) {
                for (var node in topology[shard]) {
                        var peer = topology[shard][node];
                        queryDb(peer.pgUrl, PG_REPL_STAT_QUERY, peer,
                                function(err, result, peer2) {
                                        LOG.debug({
                                                err: err,
                                                peer: peer2,
                                                result: result
                                        }, 'got result:');

                                peer2.slave = err || result.rows[0];
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

function queryDb(url, query, peer, callback) {
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
                        return callback(err2, result, peer);
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

process.on('uncaughtException', function (err) {
        console.error('uncaughtException (exiting error code 1)', err);
        process.exit(1);
});
