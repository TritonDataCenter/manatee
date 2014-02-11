/**
 * @overview The Manatee client.
 * @copyright Copyright (c) 2014, Joyent, Inc. All rights reserved.
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
var backoff = require('backoff');
var bunyan = require('bunyan');
var once = require('once');
var util = require('util');
var vasync = require('vasync');
var verror = require('verror');
var zkplus = require('zkplus');

var EventEmitter = require('events').EventEmitter;

/**
 * Create a Manatee client.
 * @param {object} options Manatee options.
 * @param {Bunyan} [options.log] Bunyan logger.
 * @param {string} options.path ZK election path of the shard.
 * @param {object} options.zk ZK client options.
 * @param {Bunyan} [options.zk.log] Bunyan logger.
 * @param {object[]} options.zk.servers ZK servers.
 * @param {string} options.zk.servers.host ZK server ip.
 * @param {number} options.zk.servers.port ZK server port.
 * @param {number} options.zk.timeout Time to wait before timing out connect
 * attempt in ms.
 *
 */
function Client(options) {
    assert.object(options, 'options');
    assert.optionalObject(options.log, 'options.log');
    assert.string(options.path, 'options.path');
    assert.object(options.zk, 'options.zk');

    var self = this;
    EventEmitter.call(this);

    this._log = null;
    if (options.log) {
        self._log = options.log.child();
    } else {
        self._log = bunyan.createLogger({
            level: (process.env.LOG_LEVEL || 'info'),
            name: 'mantee-client',
            serializers: {
                err: bunyan.stdSerializers.err
            },
            // always turn source to true, manatee isn't in the data path
            src: true
        });
    }

    this._path = options.path;
    this._zkCfg = options.zk;
    this._zk = null;

    (function setupZK() {
        var zkOpts = {
            log: options.log,
            servers: options.zk.servers,
            timeout: parseInt(options.zk.timeout || 30000, 10)
        };
        self._createZkClient(zkOpts, function (zk_err, zk) {
            if (zk_err) {
                self.emit('error', zk_err);
                return;
            }

            self._zk = zk;

            function reconnect() {
                self._zk = null;
                process.nextTick(setupZK);
            }

            self._zk.once('close', reconnect);
            self._zk.on('error', function onZKClientError(err) {
                self.log.error(err, 'ZooKeeper client error');
                self._zk.removeAllListeners('close');
                self._zk.close();
                reconnect();
            });
            self._init();
        });
    })();
}
util.inherits(Client, EventEmitter);

module.exports = {
    createClient: function createClient(options) {
        return (new Client(options));
    }
};

//
// Reads the topology from ZK, and creates the initial
// DB pool
//
Client.prototype._init = function _init() {
    var self = this;
    var log = self._log;

    log.debug('init: entered');
    self._topology(function (err, urls, children) {
        if (err) {
            log.fatal(err, 'init: error reading from zookeeper');
            throw new VError(err, 'init: error reading from ZK');
        } else if (!urls || !urls.length) {
            log.error('init: no DB shards available');
            self._watch();
            return;
        }

        process.nextTick(self.emit.bind(self, 'ready'));
        process.nextTick(self.emit.bind(self, 'topology', urls));
    });
};


Client.prototype._watch = function _watch() {
    var log = this.log;
    var self = this;
    var zk = this.zk;

    log.debug('watch: entered');
    zk.watch(this.path, {method: 'list'}, function (werr, listener) {
        if (werr) {
            log.fatal(werr, 'watch: failed');
            self.emit('error', werr);
            return;
        }

        listener.once('error', function (err) {
            log.fatal(err, 'watch: error event fired; exiting');
            zk.emit('error', new VError(err, 'watch: unsolicited error event'));
        });

        listener.on('children', function(children) {
            var urls = self._childrenToURLs(children);
            zk.emit('topology', urls);
        });
        log.debug('watch: started');
    });
};

//
// Returns the current DB peer topology as a list of URLs, sorted in descending
// order: that is, '0' is always primary, '1' is always sync, and '2+' are
// async slaves
//
Client.prototype._topology = function _topology(cb) {
    var self =  this;
    assert.func(cb, 'callback');

    cb = once(cb);

    var log = self._log;
    var zk = self._zk;

    log.debug({path: self._path}, 'topology: entered');
    zk.readdir(self._path, function (err, children) {

        if (err) {
            log.debug(err, 'topology: failed');

            if (err.code !== zkplus.ZNONODE) {
                cb(err);
            } else {
                setTimeout(function () {
                    _topology(opts, cb);
                }, 1000);
            }
            return;
        }

        var urls = self._childrenToURLs(children);
        log.debug({urls: urls}, 'topology: done');
        cb(null, urls, children);
    });
};

/**
 * Election nodes are postfixed with -123456. so sort by the number after -,
 * and you'll have:
 * [primary, sync, async, ..., asyncn]
 *
 * @param {string[]} children The list of Postgres peers.
 */
Client.prototype._childrenToURLs = function (children) {
    var self = this;
    function compare(a, b) {
        var seqA = parseInt(a.substring(a.lastIndexOf('-') + 1), 10);
        var seqB = parseInt(b.substring(b.lastIndexOf('-') + 1), 10);

        return (seqA - seqB);
    }
    var urls = (children || []).sort(compare).map(function (val) {
        //arr[idx] = self._transformPgUrl(val);
        return self._transformPgUrl(val);
    });

    return urls;
};

/**
 * transform an zk election node name into a postgres url.
 * @param {string} zkNode The zknode, e.g.
 * 10.77.77.9:pgPort:backupPort:hbPort-0000000057, however previous versions of
 * manatee will only have 10.77.77.9-0000000057, so we have to be able to
 * disambiguate between the 2.
 *
 * @return {string} The transformed pg url, e.g.
 * tcp://postgres@10.0.0.0:5432/postgres
 */
Client.prototype._transformPgUrl = function (zkNode) {
    var encodedString = zkNode.split('-')[0];
    var data = encodedString.split(':');
    // if we're using the legacy format, there will not be ':', and as such the
    // split will return an array of length 1
    if (data.length === 1) {
        return 'tcp://postgres@' + data[0];
    } else {
        return 'tcp://postgres@' + data[0] + ':' + data[1] + '/postgres';
    }
};

/**
 * @callback Shard-createZkClientCb
 * @param {Error} error
 * @param {zkplusClient} client zkplus client.
 */

/**
 * Create a zookeeper client.
 * @param {object} options ZK Client options.
 * @param {Bunyan} [options.log] Bunyan logger.
 * @param {object[]} options.servers ZK servers.
 * @param {string} options.servers.host ZK server ip.
 * @param {number} options.servers.port ZK server port.
 * @param {number} options.timeout Time to wait before timing out connect
 * attempt in ms.
 * @param {Shard-createZkClientCb} cb
 */
Client.prototype._createZkClient = function (opts, cb) {
    var self = this;
    var log = self._log;
    assert.object(opts, 'options');
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
};

