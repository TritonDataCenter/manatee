/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2014, Joyent, Inc.
 */

/**
 * @overview Mantatee cluster state
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

/**
 * Validates and instantiates a new cluster state object.  Must be in the
 * following structure:
 * {
 *   "generation": 1,
 *   "primary": {
 *     "zonename": "9221638d-24f7-4ffe-9dda-64c1dd024c35",
 *     "ip": "10.99.99.54"
 *   },
 *   "sync": {
 *     <same structure as primary>
 *   },
 *   "async": [
 *     { <same structure as primary> },
 *     ...
 *   ],
 *   "initWal": "0/174A3A8"
 * }
 */
function ClusterState(opts) {
    if ((typeof (opts) === 'string')) {
        opts = JSON.parse(opts);
    }
    function assertHost(h) {
        assert.string(h.zonename, 'host.zonename');
        assert.string(h.ip, 'host.ip');
    }
    assert.object(opts, 'opts');
    assert.number(opts.generation, 'opts.generation');
    assert.object(opts.primary, 'opts.primary');
    assertHost(opts.primary);
    assert.object(opts.sync, 'opts.sync');
    assertHost(opts.sync);
    assert.optionalArrayOfObject(opts.async, 'opts.async');
    opts.async.forEach(function (a) {
        assertHost(a);
    });
    assert.string(opts.initWal, 'opts.initWal');

    this.state = opts;
}
module.exports = ClusterState;



/**
 * Given a js conf object, write it out to a file in conf format
 * @param: {String} file The path of the output file.
 * @param: {object} conf The conf object.
 * @param: {function} callback The callback of the form f(err).
 */
ClusterState.prototype.toString = function toString() {
    return (JSON.serialize(this.state, null, 0));
};
