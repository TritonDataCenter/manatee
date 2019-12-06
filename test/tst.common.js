/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2019 Joyent, Inc.
 */

var mod_assertplus = require('assert-plus');

var lib_common = require('../lib/common');

var versions = [ {
    'value':  1,
    'expected': 'version (string) is required'
}, {
    'value': 9,
    'expected': 'version (string) is required'
}, {
    'value': 1.1,
    'expected': 'version (string) is required'
}, {
    'value': '9',
    'expected': 'not enough elements in version'
}, {
    'value': '9.2.4',
    'expected': '9.2'
}, {
    'value': '9.6.2',
    'expected': '9.6'
}, {
    'value': '9.x.y',
    'expected': '9.x'
}, {
    'value': '9.2',
    'expected': '9.2'
}, {
    'value': '9.2.3.2',
    'expected': '9.2'
}, {
    'value': '10',
    'expected': '10'
}, {
    'value': '10.5',
    'expected': '10'
}, {
    'value': '10.5.2',
    'expected': '10'
}, {
    'value': '13.9',
    'expected': '13'
}, {
    'value': '20.x',
    'expected': '20'
}, {
    'value': '21.2',
    'expected': '21'
} ];

function main() {
    versions.forEach(function (version) {
        var rv;

        try {
            rv = lib_common.pgStripMinor(version.value);
        } catch (e) {
            rv = e.message;
        }

        mod_assertplus.equal(rv, version.expected);
    });
}

main();
