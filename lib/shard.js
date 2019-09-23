/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2014, Joyent, Inc.
 */

/*
 * shard.js: encapsulates the operation of a peer in a manatee shard, including
 * the distributed cluster state machine and the Postgres and ZooKeeper
 * interfaces.
 */

var assert = require('assert-plus');
var util = require('util');

var PostgresMgr = require('./postgresMgr');
var ZookeeperMgr = require('./zookeeperMgr');
var createManateePeer = require('manatee-state-machine'); /* XXX */

exports.start = startShard;

function startShard(config) {
    var shard = new Shard(config);
    return (shard);
}

function Shard(config) {
    var zkconfig;
    var id, backupurl;

    /* XXX validate */
    assert.object(config, 'config');
    assert.object(config.log, 'config.log');

    id = util.format('%s:%s:%s',
        config.ip, config.postgresPort, config.backupPort);
    backupurl = util.format('http://%s:%s', config.ip, config.backupPort);
    zkconfig = {
        'log': config.log,
        'id': id,
        'data': {
            'zoneId': config.zoneId,
            'ip': config.ip,
            'pgUrl': config.postgresMgrCfg.url,
            'backupUrl': backupurl
        },
        'path': config.shardPath,
        'connStr': config.zkCfg.connStr,
        'opts': config.zkCfg.opts
    };

    this._log = config.log;
    this._pg = new PostgresMgr(config.postgresMgrCfg);
    this._zk = new ZookeeperMgr(zkconfig);
    this._cluster = createManateePeer({
        'log': this._log,
        'zkinterface': this._zk,
        'pginterface': this._pg,
        'singleton': config.postgresMgrCfg.oneNodeWriteMode,
        'self': {
            'id': id,
            'ip': config.ip,
            'pgUrl': config.postgresMgrCfg.url,
            'zoneId': config.zoneId,
            'backupUrl': backupurl
        }
    });
}

Shard.prototype.debugState = function () {
    return (this._cluster.debugState());
};

Shard.prototype.shutdown = function (cb) {
    //Due to MANATEE-188 we need to let postgres be shot in the head rather than
    // shut down cleanly.  When it is shut down cleanly it writes a checkpoint
    // to the xlog, then we have an almost guaranteed xlog divergence between it
    // and the new primary.  Keeping it from writing the shutdown checkpoint
    // gives us a *chance* that it can come back as an async.

    var self = this;

    //Shut down ZK, only if it is connected already.
    if (self._zk && self._zk.status() === 'SYNC_CONNECTED') {
        return (self._zk.close(cb));
    } else {
        return (setImmediate(cb));
    }
};
