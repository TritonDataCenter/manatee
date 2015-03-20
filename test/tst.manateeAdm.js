/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2015, Joyent, Inc.
 */

/*
 * manateeAdm.test.js: test specific invocations of the "manatee-adm" command.
 * Run this test case with "catest".
 *
 * This test case runs through a number of possible cluster states (defined
 * somewhat declaratively below) and runs "manatee-adm peers", "manatee-adm
 * pg-status", "manatee-adm show", and "manatee-adm verify" with each of them.
 * The stdout and stderr are written to this program's stdout (separately).
 * When run under "catest", the output file will be compared against the
 * expected output and the test case will fail if they don't match.  This test
 * runner also explicitly checks the exit codes of each "manatee-adm" command.
 */

var assertplus = require('assert-plus');
var forkexec = require('forkexec');
var fs = require('fs');
var path = require('path');
var vasync = require('vasync');
var VError = require('verror');
var sprintf = require('extsprintf').sprintf;

var testFileName = path.join(
    process.env['TMP'] || process.env['TMPDIR'] || '/var/tmp',
    path.basename(process.argv[1]) + '.' + process.pid);

/*
 * This guard checks that something doesn't erroneously end the pipeline early
 * and allow Node to exit without having run all the tests.
 */
var done = false;
process.on('exit', function () { assertplus.ok(done); });

function main()
{
    var funcs, testcases;

    /*
     * If test cases were passed on the command line, then only run those.
     */
    if (process.argv.length > 2) {
        testcases = process.argv.slice(2);
        console.error('only running test cases: %s',
            testcases.join(', '));
    } else {
        testcases = null;
    }

    /*
     * Set up a vasync waterfall to run each command with each of the predefined
     * cluster states.
     */
    funcs = [];
    clusterStates.forEach(function (testcase) {
        var testname, expected, cs;

        testname = testcase[0];
        expected = testcase[1];
        cs = testcase[2];

        if (testcases !== null && testcases.indexOf(testname) == -1)
            return;

        /*
         * "peers", "pg-status", and "show" always exit 0.  "verify" exits
         * non-zero if there were any problems.
         */
        funcs.push(runTestCase.bind(null, testname, cs, 0, [ 'peers' ]));
        funcs.push(runTestCase.bind(null, testname, cs, 0, [ 'pg-status' ]));
        funcs.push(runTestCase.bind(null, testname, cs, 0, [ 'show' ]));
        funcs.push(runTestCase.bind(null, testname, cs, 0, [ 'show', '-v' ]));
        funcs.push(runTestCase.bind(null, testname, cs,
            expected, [ 'verify' ]));
        funcs.push(runTestCase.bind(null, testname, cs,
            expected, [ 'verify', '-v' ]));

        if (testname != 'normalOk')
            return;

        /*
         * Here we add some invocations that exercise the other flags available
         * on these commands.  We don't need to run these for every cluster
         * state.
         */

        /* Basic uses of -H/--omitHeader, -o/--columns, and -r/--role. */
        funcs.push(runTestCase.bind(null, testname, cs, 0,
            [ 'peers', '-H' ]));
        funcs.push(runTestCase.bind(null, testname, cs, 0,
            [ 'peers', '-H', '-o', 'role' ]));
        funcs.push(runTestCase.bind(null, testname, cs, 0,
            [ 'peers', '--omitHeader', '-o', 'role', '-o', 'peername' ]));
        funcs.push(runTestCase.bind(null, testname, cs, 0,
            [ 'peers', '--columns', 'role', '-o', 'peername,ip' ]));
        funcs.push(runTestCase.bind(null, testname, cs, 0,
            [ 'peers', '--role=primary' ]));
        funcs.push(runTestCase.bind(null, testname, cs, 0,
            [ 'peers', '-H', '-o', 'ip', '-r', 'sync' ]));

        /* Same test cases for "pg-status". */
        funcs.push(runTestCase.bind(null, testname, cs, 0,
            [ 'pg-status', '-H' ]));
        funcs.push(runTestCase.bind(null, testname, cs, 0,
            [ 'pg-status', '-H', '-o', 'role' ]));
        funcs.push(runTestCase.bind(null, testname, cs, 0,
            [ 'pg-status', '--omitHeader', '-o', 'role', '-o', 'peername' ]));
        funcs.push(runTestCase.bind(null, testname, cs, 0,
            [ 'pg-status', '--columns', 'role', '-o', 'peername,ip' ]));
        funcs.push(runTestCase.bind(null, testname, cs, 0,
            [ 'pg-status', '--role=primary' ]));
        funcs.push(runTestCase.bind(null, testname, cs, 0,
            [ 'pg-status', '-H', '-o', 'ip', '-r', 'sync' ]));

        /* Attempting to access pg fields from "peers" should fail. */
        funcs.push(runTestCase.bind(null, testname, cs, 2,
            [ 'peers', '-o', 'pg-sent' ]));

        /* Attempting to access bogus fields from both commands should fail. */
        funcs.push(runTestCase.bind(null, testname, cs, 2,
            [ 'peers', '-o', 'badcolumn' ]));
        funcs.push(runTestCase.bind(null, testname, cs, 2,
            [ 'pg-status', '-o', 'badcolumn' ]));

        /* Run "pg-status" a couple of times. */
        funcs.push(runTestCase.bind(null, testname, cs, 0,
            [ 'pg-status', '1', '2' ]));

        /* Run "pg-status" in cinematic mode. */
        funcs.push(runTestCase.bind(null, testname, cs, 0,
            [ 'pg-status', '-w' ]));
    });

    console.error('using tmpfile: "%s"', testFileName);
    vasync.waterfall(funcs, function (err) {
        if (err) {
            console.error('error: %s', err.message);
            process.exit(1);
        }

        done = true;
        console.log('TEST PASSED');
    });
}

/*
 * Helper class for generating valid (but possibly broken) cluster states.
 * See addPeer().
 */
function MockState() {
    this.pgs_peers = {};
    this.pgs_generation = 3;
    this.pgs_initwal = '3/12345678';
    this.pgs_primary = null;
    this.pgs_sync = null;
    this.pgs_asyncs = [];
    this.pgs_deposed = [];
    this.pgs_frozen = false;
    this.pgs_freeze_reason = null;
    this.pgs_freeze_time = null;
    this.pgs_singleton = false;
    this.pgs_errors = [];
    this.pgs_warnings = [];

    this._npeers = 0;
    this._ip_base = '10.0.0.';

    /*
     * In order to get the same uuids each time we run this program,
     * we hardcode them.  By putting them inside this class, they're reset for
     * each test case too.
     */
    this._uuids = [
        '301e2d2c-cd09-11e4-837d-13ea7132a060',
        '30219700-cd09-11e4-a971-cf7f957afa2d',
        '3022acc6-cd09-11e4-9716-1ff89e748aff',
        '30250e3a-cd09-11e4-a9e0-cbb241418d62',
        '30265b32-cd09-11e4-b42b-d3e4ff184bb8',
        '3028df9c-cd09-11e4-baa6-23ac5bd2e03c',
        '302b6db6-cd09-11e4-af3b-5bda767e5008',
        '302d2bec-cd09-11e4-a023-a3ebc0c6fd36',
        '302f53b8-cd09-11e4-b38f-a35b1d876bb4',
        '30314268-cd09-11e4-bf7c-4f69c228740e'
    ];
}

/*
 * addPeer(role, peerstatus, log): add a peer to the cluster state with role
 * "role" ("primary, "sync", "async", or "deposed") and status "peerstatus",
 * which is one of:
 *
 *     down     act like we failed to contact postgres at all
 *
 *     no-repl  act like we contacted postgres, but replication was not
 *              established
 *
 *     sync,    act like we contacted postgres and streaming replication
 *     async    is established with sync_state = "sync" or "async",
 *              respectively.
 *
 * If "lag" is non-null and the peer is an async, then "lag" should be a number
 * of seconds of replication lag to report.
 */
MockState.prototype.addPeer = function (role, peerstatus, lag) {
    var ip, zoneId, id, pgUrl, backupUrl, pgerr, repl, lagobj;
    var peer;

    assertplus.ok(this._uuids.length > 0, 'too many peers defined ' +
        '(define more uuids)');
    assertplus.string(role, 'role');
    assertplus.string(peerstatus);
    if (peerstatus == 'down') {
        pgerr = new VError('failed to connect to peer: ECONNREFUSED');
        repl = null;
    } else if (peerstatus == 'no-repl') {
        pgerr = null;
        repl = null;
    } else if (peerstatus == 'sync' || peerstatus == 'async') {
        pgerr = null;
        repl = {
            /* We don't bother populating fields that aren't used here. */
            'state': 'streaming',
            'sync_state': peerstatus,
            'client_addr': null,
            'sent_location': '0/12345678',
            'flush_location': '0/12345678',
            'write_location': '0/12345678',
            'replay_location': '0/12345678'
        };
    } else {
        throw (new VError('unsupported peer status: "%s"', peerstatus));
    }

    if (role == 'async') {
        if (lag === null)
            lag = 342;
        lagobj = { 'minutes': Math.floor(lag / 60), 'seconds': lag % 60 };
    } else {
        lagobj = null;
    }

    /*
     * Construct the new peer details.
     */
    ip = this._ip_base + (++this._npeers);
    zoneId = this._uuids.shift();
    id = sprintf('%s:5432:12345-0000000001', ip);
    pgUrl = sprintf('tcp://postgres@%s:5432/postgres', ip);
    backupUrl = sprintf('http://%s:12345', ip);

    peer = {};
    peer.pgp_label = zoneId.substr(0, 8);
    peer.pgp_ident = {
        'id': id,
        'ip': ip,
        'zoneId': zoneId,
        'pgUrl': pgUrl,
        'backupUrl': backupUrl
    };
    peer.pgp_pgerr = pgerr;
    peer.pgp_lag = lagobj;
    peer.pgp_repl = repl;

    /*
     * Hook this new peer into the simulated cluster state.
     */
    this.pgs_peers[id] = peer;
    switch (role) {
    case 'primary':
        this.pgs_primary = id;
        break;

    case 'sync':
        this.pgs_sync = id;
        break;

    case 'async':
        this.pgs_asyncs.push(id);
        break;

    case 'deposed':
        this.pgs_deposed.push(id);
        break;

    default:
        throw (new VError('unsupported role: "%s"', role));
    }
};

/*
 * Finish hooking up any pieces of state that can only be done after all peers
 * have been added.  Today, this only hooks up replication connections to
 * downstream peers.
 */
MockState.prototype.finish = function ()
{
    var self = this;
    var peers, i;

    peers = [ this.pgs_peers[this.pgs_primary] ];
    if (this.pgs_sync !== null) {
        peers.push(this.pgs_peers[this.pgs_sync]);

        this.pgs_asyncs.forEach(function (a) {
            peers.push(self.pgs_peers[a]);
        });
    }

    for (i = 0; i < peers.length - 1; i++) {
        if (peers[i].pgp_repl === null ||
            !peers[i].pgp_repl.hasOwnProperty('client_addr'))
            continue;

        peers[i].pgp_repl.client_addr = peers[i + 1].pgp_ident.ip;
    }
};

/*
 * Return a JSON representation of this state.  This is written to a temporary
 * file, and then we override manatee-adm to use this simulated state rather
 * than fetching it from ZooKeeper.
 */
MockState.prototype.toJson = function ()
{
    var obj, prop;

    obj = {};
    for (prop in this) {
        if (!this.hasOwnProperty(prop))
            continue;

        if (prop.substr(0, 'pgs_'.length) != 'pgs_')
            continue;

        obj[prop] = this[prop];
    }

    return (JSON.stringify(obj));
};


/*
 * In order to test various outputs, we generate a bunch of cluster states and
 * trick the loadClusterDetails() interface in lib/adm.js to return these
 * instead of actually contacting any remote servers.
 */
var clusterStates = [
    /* "S" is the expected exit status of "manatee-adm verify" for this case. */
    /* TEST CASE NAME         S  CLUSTER STATE */
    [ 'singletonOk',          0, makeStateSingleton({}) ],
    [ 'singletonDown',        1, makeStateSingleton({ 'primary': 'down' }) ],

    [ 'normalOk',             0, makeStateNormal({}) ],
    [ 'normal2Peers',         1, makeStateNormal({ 'asyncs': 0 }) ],
    [ 'normal5Peers',         0, makeStateNormal({ 'asyncs': 3 }) ],
    [ 'normalDeposed',        1, makeStateNormal({ 'deposed': 1 }) ],
    [ 'normal2Deposed',       1, makeStateNormal({ 'deposed': 2 }) ],
    [ 'normal5Peers2deposed', 1, makeStateNormal({
        'asyncs': 3,
        'deposed': 2
    }) ],
    [ 'normalPdown',          1, makeStateNormal({ 'primary': 'down' }) ],
    [ 'normalPnorepl',        1, makeStateNormal({ 'primary': 'no-repl' }) ],
    [ 'normalPasync',         1, makeStateNormal({ 'primary': 'async' }) ],
    [ 'normalSdown',          1, makeStateNormal({ 'sync': 'down' }) ],
    [ 'normalSnorepl',        1, makeStateNormal({ 'sync': 'no-repl' }) ],

    [ 'normalNoLag',          0, makeStateNormal({ 'lag': 0 }) ],
    [ 'normalLargeLag',       0, makeStateNormal({ 'lag': 86465 }) ]
];

/*
 * Generate a singleton (one-node-write-mode) cluster state.
 *
 * options.primary is passed to addPeer() as "peerstatus".  The only cases that
 * are appropriate here are "no-repl" (the default) and "down".
 */
function makeStateSingleton(options)
{
    var state;

    assertplus.object(options);
    assertplus.optionalString(options.primary);

    state = new MockState();
    state.pgs_singleton = true;
    state.addPeer('primary', options.primary || 'no-repl', 'none');

    state.pgs_frozen = true;
    state.pgs_freeze_reason = 'manatee setup: one node write mode';
    state.pgs_freeze_time = new Date('2006-02-15').toISOString();

    state.finish();
    return (state);
}

/*
 * Generate a normal (non-singleton) cluster state.
 *
 * By default, the cluster state has a working primary, sync, and async.  You
 * can also specify "options":
 *
 *     asyncs       an integer number of async peers
 *
 *     deposed      an integer number of deposed peers
 *
 *     primary,     "peerstatus" for the primary or sync.  The defaults are
 *     sync         "sync" and "async", respectively (which denote the status of
 *                  these peers in a working cluster).  Either of these
 *                  properties can have either of those two values, plus
 *                  "no-repl" or "down".  See addPeer() above.
 *
 *     lag          an integer number of seconds to use for an async peer's lag
 */
function makeStateNormal(options)
{
    var state, i;
    var nasyncs = 1;
    var ndeposed = 0;

    assertplus.object(options);
    assertplus.optionalString(options.primary);
    assertplus.optionalNumber(options.asyncs);
    assertplus.optionalNumber(options.deposed);

    if (options.hasOwnProperty('asyncs'))
        nasyncs = options.asyncs;
    if (options.hasOwnProperty('deposed'))
        ndeposed = options.deposed;

    state = new MockState();
    state.addPeer('primary', options.primary || 'sync');
    state.addPeer('sync', options.sync ||
        (nasyncs === 0 ? 'no-repl' : 'async'));

    for (i = 0; i < nasyncs; i++) {
        state.addPeer('async', i == nasyncs - 1 ? 'no-repl' : 'async',
            options.hasOwnProperty('lag') ? options.lag : null);
    }

    for (i = 0; i < ndeposed; i++) {
        state.addPeer('deposed', 'down');
    }

    state.finish();
    return (state);
}

/*
 * Run one of the "manatee-adm" commands on the given cluster state "cs" as part
 * of test case "testname".  The command to use (and associated arguments) is
 * specified by "execArgs", and the expected exit status is given by "expected".
 */
function runTestCase(testname, cs, expected, execArgs, callback)
{
    var json, execname;

    json = cs.toJson();
    fs.writeFileSync(testFileName, json);
    execname = path.join(__dirname, '..', 'bin', 'manatee-adm');

    forkexec.forkExecWait({
        'argv': [ execname ].concat(execArgs),
        'timeout': 5000,
        'env': {
            /*
             * This special flag tells "manatee-adm" to load the cluster state
             * from this file rather than reaching out to a live Manatee
             * cluster.
             */
            'MANATEE_ADM_TEST_STATE': testFileName,

            /*
             * manatee-adm requires that the SHARD and ZK_IPS arguments be
             * provided, but they can have any value since we're overriding the
             * cluster state anyway.
             */
            'SHARD': 'UNUSED',
            'ZK_IPS': 'UNUSED',

            /*
             * PATH must be passed through because manatee-adm uses env(1) to
             * locate node.
             */
            'PATH': process.env['PATH']
        }
    }, function (err, info) {
        /*
         * If we expected the command to exit with a non-zero status and it
         * failed for exactly that reason, ignore the error.
         */
        if (err && expected !== 0 && info.status == expected)
            err = null;

        /*
         * If the command failed for any other reason or with a different error
         * code than expected, then bail out here.
         */
        if (err) {
            console.error('manatee-adm exec failed: ', info);
            callback(err);
            return;
        }

        /*
         * Given that we're going to succeed at this point, remove the temporary
         * file before proceeding.
         */
        fs.unlink(testFileName, function (warn) {
            if (warn) {
                console.error('warning: failed to unlink "%s": %s',
                    testFileName, warn.message);
            }

            /*
             * We emit both stdout and stderr to our own stdout in a way that
             * makes them distinguishable.  catest (the test runner) will
             * compare this output to an expected output file and fail if they
             * don't match.
             */
            console.log('TEST CASE "%s": manatee-adm %s:', testname,
                execArgs.join(' '));
            console.log('--------- stdout ------------');
            process.stdout.write(info.stdout);
            console.log('--------- stderr ------------');
            process.stdout.write(info.stderr);
            console.log('-----------------------------');
            console.log('');
            callback();
        });
    });
}

main();
