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
var ZkMgr = require('../lib/zookeeperMgr');

/**
 * Testing some zk assumptions....
 */
var LOG = bunyan.createLogger({
        level: (process.env.LOG_LEVEL || 'debug'),
        name: 'zk.js',
        serializers: {
                err: bunyan.stdSerializers.err
        },
        // always turn source to true, manatee isn't in the data path
        src: true
});

var cfg = JSON.parse(
        fs.readFileSync('/opt/smartdc/manatee/etc/sitter.json', 'utf8')
);
var path = '/some/test/1.zkclient';
var connStr = cfg.zkCfg.connStr;
var opts = {
//        sessionTimeout: 60000,
        sessionTimeout: 3000,
        spinDelay: 1000,
        retries: 0
};
var zonename;
var id = process.argv[2];
if (!id) {
    id = process.pid;
}

function getZonename(_, subcb) {
    childProcess.exec('zonename', function (err, stdout, stderr) {
        if (err) {
            return (subcb(err));
        }
        zonename = stdout;
        return (subcb());
    });
}

function startZkMgr(_, subcb) {
    var zopts = {
        'id': id,
        'zonename': zonename,
        'ip': '127.0.0.1',
        'path': path,
        'connStr': connStr,
        'opts': opts
    };

    LOG.info(zopts, 'starting zk manager');
    zopts.log = LOG;
    var zk = new ZkMgr(zopts);

    zk.on('init', function (status) {
        LOG.fatal(status, 'zkMgr: inited');
        return (subcb());
    });

    zk.on('activeChange', function (allActive) {
        LOG.fatal(allActive, 'active change');
    });

    zk.on('clusterStateChange', function (state) {
        LOG.fatal(state, 'cluster state change');
    });
}

vasync.pipeline({
    'funcs': [
        getZonename,
        startZkMgr
    ]
}, function (err) {
    if (err) {
        LOG.error(err);
    }
});
