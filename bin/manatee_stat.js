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



///--- Globals

var Client = pg.Client;

var NAME = 'manatee_stat';
var LOG = bunyan.createLogger({
        name: path.basename(process.argv[1]),
        level: (process.env.LOG_LEVEL || 'fatal'),
        src: true,
        serializers: {
                err: bunyan.stdSerializers.err
        }
});

var PG_REPL_STAT = 'select * from pg_stat_replication;';
var PG_REPL_LAG = 'SELECT now() - pg_last_xact_replay_timestamp() AS time_lag;';

var ZK;



///--- Internal Functions

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


function formatNodes(nodes) {
        var output = {};
        for (var i = 0; i < nodes.length; i++) {
                var node = nodes[i];
                switch(i) {
                case 0:
                        output['primary'] = {
                                ip: node.split('-')[0],
                                pgUrl: transformPgUrl(node)
                        };
                        break;
                case 1:
                        output['sync'] = {
                                ip: node.split('-')[0],
                                pgUrl: transformPgUrl(node)
                        };
                        break;
                case 2:
                        output['async'] = {
                                ip: node.split('-')[0],
                                pgUrl: transformPgUrl(node)
                        };
                        break;
                default:
                        var asyncNumber = i - 2;
                        output['async' + asyncNumber] = {
                                ip: node.split('-')[0],
                                pgUrl: transformPgUrl(node)
                        };
                        break;
                }
        }

        return (output);
}


function ifError(err) {
        if (err) {
                console.error(err.toString());
                process.exit(1);
        }
}


function loadTopology(opts, callback) {
        var tasks = [
                function listAllShards(arg, cb) {
                        ZK.readdir('/manatee', function(err, shards) {
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
                                var p = '/manatee/' + s;
                                ZK.readdir(p, function (err, nodes) {
                                        if (err) {
                                                _cb(err);
                                                return;
                                        }

                                        nodes.sort(compareNodeNames);
                                        topology[s] = formatNodes(nodes);

                                        count++;
                                        if (count === arg.shards.length)
                                                cb();
                                });
                        });
                },
                function registrarStatus(arg, cb) {
                        var count = 0;
                        arg.shards.forEach(function (s) {
                                var path = '/' +
                                           s.split('.').reverse().join('/') +
                                           '/pg';
                                ZK.get(path, function(err, object) {
                                        topology[s].registrar = object;
                                        count++;

                                        if (count === arg.shards.length)
                                                cb();
                                });
                        });
                }
        ];
        var topology = {}

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

function parseOptions() {
        var option;
        var opts = {};
        var parser = new getopt.BasicParser('hpvs:', process.argv);

        while ((option = parser.getopt()) !== undefined) {
                switch (option.option) {
                case 'h':
                        usage();
                        break;

                case 's':
                        opts.shard = option.optarg;
                        break;

                case 'p':
                        opts.postgres = true;
                        break;

                case 'v':
                        LOG.level(Math.max(bunyan.TRACE, (LOG.level() - 10)));
                        if (LOG.level() <= bunyan.DEBUG)
                                LOG = LOG.child({src: true});
                        break;

                default:
                        usage('invalid option: ' + option.option);
                        process.exit(1);
                        break;
                }
        }

        if (parser.optind() >= process.argv.length)
                usage('missing required argument: "zookeeper_ip"');

        opts.zk = process.argv[parser.optind()];

        return (opts);
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


function query(url, query, callback) {
        LOG.debug({
                url: url,
                query: query
        }, 'query: entering.');

        var client = new Client(url);
        client.connect(function (err) {
                ifError(err);
                LOG.debug({
                        sql: query
                }, 'query: connected to pg, executing sql');
                client.query(query, function(err2, result) {
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


function usage(msg) {
        if (msg)
                console.error(msg);

        var str = 'usage: ' + path.basename(process.argv[1]);
        str += '[-h] [-p] [-v] [-s shard] zookeeper_ip';
        console.error(str);
        process.exit(msg ? 1 : 0);
}



///--- Mainline

var _opts = parseOptions();

createZkClient(_opts, function (err, zk) {
        ifError(err);

        ZK = zk;
        loadTopology(_opts, function (err, topology) {
                ifError(err);

                if (_opts.postgres) {
                        pgStatus(topology, function (err) {
                                ifError(err);

                                printTopology(_opts, topology);
                        });
                } else {
                        printTopology(_opts, topology);
                }
        });
});
