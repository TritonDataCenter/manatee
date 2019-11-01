/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2019 Joyent, Inc.
 */

var mod_assertplus = require('assert-plus');
var mod_bunyan = require('bunyan');
var fs = require('fs');

var PostgresMgr = require('../lib/postgresMgr');

var config = JSON.parse(fs.readFileSync('./etc/sitter.json', 'utf8'));

var log = mod_bunyan.createLogger({
    'name': 'tst.postgresMgr',
    'streams': [ {
        'path': '/dev/null'
    } ]
});

config.postgresMgrCfg.log = log;
config.postgresMgrCfg.zfsClientCfg.log = log;

var pg = new PostgresMgr(config.postgresMgrCfg);

console.log(pg.resolveWalTranslations('11'));

console.log(pg.resolveWalTranslations('9.6'));

console.log(pg.resolveWalTranslations('123'));

console.log(pg.resolveWalTranslations('1.1'));

console.log(pg.resolveWalTranslations('9'));

pg.close(function (err) {
    process.exit();
});
