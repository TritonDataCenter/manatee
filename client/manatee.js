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
 *
 * @constructor
 * @augments EventEmitter
 *
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
 * @throws {Error} If the options object is malformed.
 *
 * @fires error If there is an error creating the ZKClient.
 * @fires topology When the topology has changed, in the form of an array of
 * Postgres URLS. e.g  ["tcp://postgres@127.0.0.1:30003/postgres",
 * "tcp://postgres@127.0.0.1:20003/postgres",
 * "tcp://postgres@127.0.0.1:10003/postgres"], where the first element is the
 * primary, the second element is the sync slave, and the third and additional
 * elements are the async slaves.
 * @fires ready When the client is ready and conneted.
 *
 */
function Manatee(options) {
    assert.object(options, 'options');
    assert.optionalObject(options.log, 'options.log');
    assert.string(options.path, 'options.path');
    assert.object(options.zk, 'options.zk');

    var self = this;
    EventEmitter.call(this);

    /** @type {Bunyan} The bunyan log object */
    this._log = null;

    if (options.log) {
        self._log = options.log.child();
    } else {
        self._log = bunyan.createLogger({
            level: (process.env.LOG_LEVEL || 'info'),
            name: 'mantee-client',
            serializers: {
                err: bunyan.stdSerializers.err
            }
        });
    }

    /**
     * @type {string} Path under which shard metadata such as elections are
     * stored. e.g. /manatee/com.joyent.1/election
     */
    this._path = options.path;
    /** @type {Object} The zk cfg */
    this._zkCfg = options.zk;
    /** @type {zkplus.client} The ZK client */
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
util.inherits(Manatee, EventEmitter);

module.exports = {
    createClient: function createClient(options) {
        return (new Manatee(options));
    }
};

/**
 * #@+
 * @private
 * @memberOf Shard
 */

/**
 * Reads the topology from ZK.
 */
Manatee.prototype._init = function _init() {
    var self = this;
    var log = self._log;

    log.debug('init: entered');
    self._topology(function (err, urls, children) {
        if (err) {
            log.fatal(err, 'init: error reading from zookeeper');
            throw new VError(err, 'init: error reading from ZK');
        }
        if (!urls || !urls.length) {
            log.error('init: no DB shards available');
        }

        self._watch();
        process.nextTick(self.emit.bind(self, 'ready'));
        log.info({db: urls}, 'Manatee._init: emitting db topology');
        process.nextTick(self.emit.bind(self, 'topology', urls));
    });
};


/**
 * Watch the election path for any changes.
 */
Manatee.prototype._watch = function _watch() {
    var self = this;
    var log = self._log;
    var zk = self._zk;

    log.debug('watch: entered');
    zk.watch(self._path, {method: 'list'}, function (werr, listener) {
        if (werr) {
            log.fatal(werr, 'watch: failed');
            self.emit('error', werr);
            return;
        }

        listener.once('error', function (err) {
            log.fatal(err, 'watch: error event fired; exiting');
            /*
             * we never emit zk errors up stack, since we'll handle the
             * reconnect ourselves
             */
            zk.emit('error', new VError(err, 'watch: unsolicited error event'));
        });

        listener.on('children', function(children) {
            log.debug({children: children}, 'Manatee.watch: got children');
            var urls = self._childrenToURLs(children);
            log.info({dbs: urls}, 'Manatee.watch: emitting new db topology');
            self.emit('topology', urls);
        });
        log.debug('watch: started');
    });
};

/**
 * Returns the current DB peer topology as a list of URLs, sorted in descending
 * order: that is, '0' is always primary, '1' is always sync, and '2+' are
 * async slaves
 */
Manatee.prototype._topology = function _topology(cb) {
    var self = this;
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
                    self._topology(cb);
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
 * @param {string[]} children The array of Postgres peers.
 * @return {string[]} The array of transformed PG URLs. e.g.
 * [tcp://postgres@10.0.0.0:5432]
 */
Manatee.prototype._childrenToURLs = function (children) {
    var self = this;

    function compare(a, b) {
        var seqA = parseInt(a.substring(a.lastIndexOf('-') + 1), 10);
        var seqB = parseInt(b.substring(b.lastIndexOf('-') + 1), 10);

        return (seqA - seqB);
    }

    /**
     * transform an zk election node name into a postgres url.
     * @param {string} zkNode The zknode, e.g.
     * 10.77.77.9:pgPort:backupPort:hbPort-0000000057, however previous
     * versions of manatee will only have 10.77.77.9-0000000057, so we have to
     * be able to disambiguate between the 2.
     *
     * @return {string} The transformed PG URL, e.g.
     * tcp://postgres@10.0.0.0:5432
     */
    function transformPgUrl(zkNode) {
        var encodedString = zkNode.split('-')[0];
        var data = encodedString.split(':');
        /*
         * if we're using the legacy format, there will not be ':', and as such
         * the split will return an array of length 1
         */
        if (data.length === 1) {
            return 'tcp://' + data[0];
        } else {
            return 'tcp://' + data[0] + ':' + data[1];
        }
    }

    var urls = (children || []).sort(compare).map(function (val) {
        return transformPgUrl(val);
    });

    return urls;
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
Manatee.prototype._createZkClient = function (opts, cb) {
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

/** #@- */
