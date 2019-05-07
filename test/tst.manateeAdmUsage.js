/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2019, Joyent, Inc.
 */

/*
 * manateeAdmUsage.test.js: test invocations of the "manatee-adm" command
 * Just run this test directly with Node, not with nodeunit.  All manatee-adm
 * subcommands should be covered by a test case below.  This program will fail
 * if it finds a subcommand with no test cases.
 */

var assertplus = require('assert-plus');
var forkexec = require('forkexec');
var path = require('path');
var vasync = require('vasync');
var VError = require('verror');
var extsprintf = require('extsprintf');

var sprintf = extsprintf.sprintf;
var fprintf = extsprintf.fprintf;

/*
 * Test cases are configured with the "subcmds" object below, which specifies
 * two properties for each "manatee-adm" subcommand:
 *
 *     o the list of required arguments
 *
 *     o whether running the command with all required arguments is
 *       "destructive", by which we mean only that it makes any actual changes
 *       to a Manatee cluster
 *
 * Using this configuration, the test suite will invoke the subcommand several
 * times:
 *
 *     o For each required argument, the subcommand will be invoked once with
 *       that argument _missing_ but the other required arguments present.  The
 *       test suite will make sure that the command exits with code 2
 *       (EXIT_USAGE) and both an option-specific error message and the full
 *       help message is printed.
 *
 *     o If the command is non-destructive, the subcommand will be invoked once
 *       with all required arguments present.  The test suite will make sure
 *       only that the command exits with code 0.  This is not a substitute for
 *       actually testing the command's normal behavior!
 *
 * To run these tests, the test suite requires valid values for the required
 * arguments for all non-destructive commands.  (Valid values for destructive
 * commands are not required because we'll never run that command and expect it
 * to succeed).  These are configured with the "requiredOptionEnvVars",
 * described below.  Since some arguments can be specified by environment
 * variables, the test runner always invokes "manatee-adm" in a stripped-down
 * environment with only the required variables present.  The normal variables
 * (like SHARD and ZK_IPS) are not passed through except when we explicitly
 * intend them to be.
 */
var subcmds = {
    'help': {
        'required': [],
        'destructive': false
    },
    'version': {
        'required': [],
        'destructive': false
    },
    'status': {
        'required': [ 'zk' ],
        'destructive': false
    },
    'peers': {
        'required': [ 'shard', 'zk' ],
        'destructive': false
    },
    'show': {
        'required': [ 'shard', 'zk' ],
        'destructive': false
    },
    'verify': {
        'required': [ 'shard', 'zk' ],
        'destructive': false
    },
    'pg-status': {
        'required': [ 'shard', 'zk' ],
        'destructive': false
    },
    'zk-state': {
        'required': [ 'shard', 'zk' ],
        'destructive': false
    },
    'zk-active': {
        'required': [ 'shard', 'zk' ],
        'destructive': false
    },
    'state-backfill': {
        'required': [ 'shard', 'zk' ],
        'destructive': true
    },
    'check-lock': {
        'required': [ 'path', 'zk' ],
        /*
         * This command isn't technically destructive, but we don't have a
         * useful value for "path" that we can use to test the happy case, so we
         * just tell the test suite not to test that.
         */
        'destructive': true
    },
    'history': {
        'required': [ 'shard', 'zk' ],
        'destructive': false
    },
    'rebuild': {
        'required': [ 'config', 'zk' ],
        'destructive': true
    },
    'freeze': {
        'required': [ 'reason', 'shard', 'zk' ],
        'destructive': true
    },
    'unfreeze': {
        'required': [ 'shard', 'zk' ],
        'destructive': true
    },
    'set-onwm': {
        'required': [ 'mode', 'shard', 'zk' ],
        'destructive': true
    },
    'reap': {
        'required': [ 'shard', 'zk' ],
        'destructive': true
    },
    'promote': {
        'required': [ 'role', 'zonename', 'shard', 'zk' ],
        'destructive': true
    },
    'clear-promote': {
        'required': [ 'shard', 'zk' ],
        'destructive': true
    }
};

/*
 * As mentioned above, the test suite needs valid values for any arguments that
 * are required by non-destructive commands.  These values are taken from the
 * environment using the same environment variables that "manatee-adm" also
 * interprets.
 *
 * If an entry in this object is non-null and not a boolean, then it should be
 * the name of an environment variable from which to pull the value of the
 * corresponding argument.  In this case, the test runner will run a given test
 * twice: once providing the argument to "manatee-adm" via the environment, and
 * once providing the argument via the command line.  The test runner will fail
 * if the corresponding environment variable is not set.
 *
 * If an entry in this object is null, then that indicates that this is a
 * required argument only for destructive subcommands.  In that case, a _valid_
 * value is not needed and the test suite may make up any old value.
 *
 * If an entry in this object is true (not truthy, but actually "true"), then
 * it's a boolean argument.
 */
var requiredOptionEnvVars = {
    'shard': 'SHARD',
    'zk': 'ZK_IPS',

    'help': true,

    'config': null,
    'mode': null,
    'path': null,
    'reason': null,
    'role': null,
    'zonename': null
};

var testsOk = 0;            /* count of tests that passed */
var testsFailed = 0;        /* count of tests that failed */
var testsStarted = 0;       /* count of tests that started */
var testsFinished = false;  /* all tests have been completed */

function main()
{
    var subcmd, funcs;

    /*
     * Since each test case invokes the "manatee-adm" command asynchronously,
     * we execute them all with a large waterfall.  The general pattern is that
     * each test case pushes async functions onto "funcs" and then we execute
     * the whole waterfall.
     */
    funcs = [];

    genTestMissingSubcmds(funcs);

    for (subcmd in subcmds) {
        assertplus.object(subcmds[subcmd], 'subcmd ' + subcmd);
        assertplus.arrayOfString(subcmds[subcmd].required,
            'subcmd ' + subcmd + ' required args');
        assertplus.bool(subcmds[subcmd].destructive,
            'subcmd ' + subcmd + ' destructive');
        genTestSubcmd(funcs, subcmd, subcmds[subcmd]);
    }

    /*
     * This is a canary to make sure that we've executed the last test.  If a
     * programmer forgets to invoke a callback, we might otherwise exit 0
     * without having run all tests.
     */
    funcs.push(function (callback) {
        testsFinished = true;
        callback();
    });
    process.on('exit', function () {
        assertplus.ok(testsFinished, 'missing callback invocation');
    });

    /*
     * Now execute the actual waterfall.
     */
    vasync.waterfall(funcs, function (err) {
        if (err) {
            console.error('error: %s', err.message);
            process.exit(1);
        }

        assertplus.equal(testsStarted, testsOk + testsFailed,
            'programmer error: last test case did not complete');
        console.log('%d / %d tests passed', testsOk, testsStarted);
        if (testsFailed > 0)
            process.exit(1);
    });
}

/*
 * Generate a test case that checks whether "manatee-adm help" reports any
 * subcommands that have no test cases.
 */
function genTestMissingSubcmds(funcs)
{
    funcs.push(function (callback) {
        testStart('all subcommands have test cases');
        execChildForTest('help', [], false, function (cmdresult) {
            if (!cmdResultCompleted(cmdresult)) {
                testFail('command timed out', cmdresult);
                callback();
                return;
            }
            if (!cmdResultExitOk(cmdresult)) {
                testFail('exited non-zero', cmdresult);
                callback();
                return;
            }

            if (!cmdResultHasUsage(cmdresult, 'stdout')) {
                testFail('expected help message', cmdresult);
                callback();
                return;
            }

            var cmds, lines, cmdsstart, cmdsend;
            cmds = [];
            lines = cmdresult.stdout.split(/\n/);
            lines.forEach(function (line) {
                if (line.trim() == 'Commands:') {
                    cmdsstart = true;
                    return;
                }

                if (cmdsstart && line.length > 0 && line.charAt(0) != ' ') {
                    cmdsend = true;
                    return;
                }

                if (cmdsstart && !cmdsend && line.substr(0, 4) == '    ')
                    cmds.push(line.split(/\s+/)[1]);
            });

            if (cmds.length === 0) {
                testFail('did not find any subcommands in help output');
                callback();
                return;
            }

            cmds = cmds.filter(function (cmd) {
                return (!subcmds.hasOwnProperty(cmd));
            });

            if (cmds.length === 0) {
                testPass();
            } else {
                testFail('missing test cases for subcommands: "' +
                    cmds.join(', ') + '"', cmdresult);
            }

            callback();
        });
    });
}

/*
 * Generate all test cases for a given "subcmd" with configuration "subcmdinfo".
 */
function genTestSubcmd(funcs, subcmd, subcmdinfo)
{
    assertplus.string(subcmd, 'subcmd');
    assertplus.object(subcmdinfo, 'subcmdinfo');
    assertplus.arrayOfString(subcmdinfo.required, 'subcmdinfo.required');

    /*
     * For each required field, test invoking the command without that field.
     * Test twice: once with the other required fields specified in the
     * environment and once with the other required fields specified on the
     * command line.
     */
    subcmdinfo.required.forEach(function (required, i) {
        funcs.push(function (callback) {
            testSubcmdMissingRequired(subcmd, subcmdinfo, required, true,
                callback);
        });

        funcs.push(function (callback) {
            testSubcmdMissingRequired(subcmd, subcmdinfo, required, false,
                callback);
        });
    });

    /*
     * Invoke the "help" version of the command and make sure it works.
     * Amusingly (but reasonably), node-cmdln does not supply this option for
     * the "help" subcommand.
     */
    if (subcmd != 'help') {
        funcs.push(function (callback) {
            testSubcmdHelp(subcmd, subcmdinfo, callback);
        });
    }

    /*
     * If the command is not destructive, test invoking it will all required
     * arguments.
     */
    if (subcmdinfo.destructive) {
        funcs.push(function (callback) {
            console.error('test: subcmd "%s" with required args: ' +
                'skipped (destructive)', subcmd);
            callback();
        });
        return;
    }

    funcs.push(function (callback) {
        testSubcmdOk(subcmd, subcmdinfo, true, callback);
    });

    funcs.push(function (callback) {
        testSubcmdOk(subcmd, subcmdinfo, false, callback);
    });
}


/*
 * General test suite helpers.
 */

/*
 * testStart(PRINTF_ARGS): indicate that we're starting a test case.  Test cases
 * must not be started concurrently.
 */
function testStart()
{
    var args;

    assertplus.equal(testsStarted, testsOk + testsFailed,
        'started a test while one was pending');
    process.stderr.write('test: ');
    args = Array.prototype.slice.call(arguments);
    args.unshift(process.stderr);
    fprintf.apply(null, args);
    process.stderr.write(': ');
    testsStarted++;
}

/*
 * testPass(): indicate that the currently running test case passed.
 */
function testPass()
{
    process.stderr.write('OK\n');
    testsOk++;
}

/*
 * testFail(message, cmdresult): indicate that the currently running test case
 * failed.  Dumps "cmdresult" to stderr.
 */
function testFail(message, cmdresult)
{
    fprintf(process.stderr, 'FAIL (%s)\n', message);
    testsFailed++;

    fprintf(process.stderr, 'command details:\n');
    console.error(cmdresult);
}


/*
 * Test case functions.
 */

/*
 * Test invoking subcommand "subcmd" (with configuration "subcmdinfo") without
 * required argument named "required".  If "useenv" is set, pass other arguments
 * via the environment.  Otherwise, pass them via the command line.
 */
function testSubcmdMissingRequired(subcmd, subcmdinfo, required, useenv,
    callback)
{
    var args;

    assertplus.string(subcmd, 'subcmd');
    assertplus.object(subcmdinfo, 'subcmdinfo');
    assertplus.arrayOfString(subcmdinfo.required, 'subcmdinfo.required');
    assertplus.string(required, 'required');
    assertplus.bool(useenv, 'useenv');

    testStart('subcmd "%s" without required arg "%s" (%s)',
        subcmd, required, useenv ? 'using env vars' : 'using args');

    args = subcmdinfo.required.filter(function (a) { return (a != required); });
    assertplus.equal(args.length, subcmdinfo.required.length - 1);
    execChildForTest(subcmd, args, useenv, function (cmdresult) {
        if (!cmdResultCompleted(cmdresult))
            testFail('command timed out', cmdresult);
        else if (!cmdResultExitUsage(cmdresult))
            testFail('command exited with wrong status', cmdresult);
        else if (!cmdResultMissingRequired(cmdresult, required))
            testFail('no message about missing arg', cmdresult);
        else if (!cmdResultHasUsage(cmdresult, 'stderr'))
            testFail('no usage message', cmdresult);
        else
            testPass();
        callback();
    });
}

/*
 * Test invoking subcommand "subcmd" with its --help option.
 */
function testSubcmdHelp(subcmd, subcmdinfo, callback)
{
    assertplus.string(subcmd, 'subcmd');
    assertplus.object(subcmdinfo, 'subcmdinfo');

    testStart('subcmd "%s" with --help', subcmd);
    execChildForTest(subcmd, [ 'help' ], false, function (cmdresult) {
        if (!cmdResultCompleted(cmdresult))
            testFail('command timed out', cmdresult);
        else if (!cmdResultExitOk(cmdresult))
            testFail('command exited with non-zero status', cmdresult);
        else if (!cmdResultHasUsage(cmdresult, 'stdout'))
            testFail('no usage message', cmdresult);
        else
            testPass();
        callback();
    });
}

/*
 * Test invoking subcommand "subcmd" (with configuration "subcmdinfo") with all
 * required argments.  The subcmd must not be destructive.
 */
function testSubcmdOk(subcmd, subcmdinfo, useenv, callback)
{
    var args;

    assertplus.string(subcmd, 'subcmd');
    assertplus.object(subcmdinfo, 'subcmdinfo');
    assertplus.arrayOfString(subcmdinfo.required, 'subcmdinfo.required');
    assertplus.bool(useenv, 'useenv');
    assertplus.ok(!subcmdinfo.destructive);

    testStart('subcmd "%s" with all required args (%s)', subcmd,
        useenv ? 'using env vars' : 'using args');
    args = subcmdinfo.required.slice(0);
    execChildForTest(subcmd, args, useenv, function (cmdresult) {
        if (!cmdResultCompleted(cmdresult))
            testFail('command timed out', cmdresult);
        else if (!cmdResultExitOk(cmdresult))
            testFail('command did not exit 0', cmdresult);
        else if (subcmd != 'help' && (cmdResultHasUsage(cmdresult, 'stdout') ||
            cmdResultHasUsage(cmdresult, 'stderr')))
            testFail('found usage message', cmdresult);
        else
            testPass();
        callback();
    });
}


/*
 * Helpers for child process execution.
 */

/*
 * Execute "manatee-adm" with subcommand "subcmd", passing required arguments
 * "args".  The values for the named arguments are taken as documented at the
 * top of this file.  If "useenv" is true, then pass arguments via the
 * environment when possible.  Otherwise, pass them via the command line.
 */
function execChildForTest(subcmd, args, useenv, callback)
{
    var execname, cmdresult;

    /*
     * The "cmdresult" object stores all the state associated with this
     * invocation.  We'll pass it to the caller upon completion, regardless of
     * success or failure.  The cmdResult* functions below test various
     * conditions of this object.
     */
    cmdresult = {
        'env': {        /* execution environment */
            /* only for #!/usr/bin/env */
            'PATH': process.env['PATH']
        },
        'execArgs': [ subcmd ], /* command-line arguments */
        'err': null,    /* if non-null, child was killed by a signal */
        'code': null,   /* if non-null, child exited normally with this code. */
        'stdout': null, /* full contents of stdout */
        'stderr': null  /* full contents of stderr */
    };

    args.forEach(function (argname) {
        var envvar, argvalue;

        /*
         * The ways we obtain argument values and pass them to the child are
         * driven by the requiredOptionEnvVars variable, documented at the top
         * of this file.
         */
        if (!requiredOptionEnvVars.hasOwnProperty(argname)) {
            throw (new VError('programmer error: don\'t know what ' +
                'environment variable to use for argument "%s"', argname));
        }

        envvar = requiredOptionEnvVars[argname];
        if (envvar === null) {
            /* This means any value will do. */
            argvalue = 'bogusvalue';
        } else if (envvar === true) {
            argvalue = true;
        } else if (process.env[envvar]) {
            argvalue = process.env[envvar];
        } else {
            throw (new VError('test misconfiguration: you must define "%s" ' +
                'in your environment', envvar));
        }

        if (useenv && envvar !== null) {
            cmdresult.env[envvar] = argvalue;
        } else if (envvar === true) {
            cmdresult.execArgs.push(sprintf('--%s', argname));
        } else {
            cmdresult.execArgs.push(sprintf('--%s=%s', argname, argvalue));
        }
    });

    execname = path.resolve(path.join(__dirname, '..', 'bin', 'manatee-adm'));
    forkexec.forkExecWait({
        'argv': [ execname ].concat(cmdresult.execArgs),
        'env': cmdresult.env,
        'timeout': 10000
    }, function (err, info) {
        cmdresult.err = err;
        cmdresult.code = info.status;
        cmdresult.stdout = info.stdout;
        cmdresult.stderr = info.stderr;
        callback(cmdresult);
    });
}

/*
 * Returns true iff the command (executed with execChildForTest) exited normally
 * without being terminated by a signal.
 */
function cmdResultCompleted(cmdresult)
{
    return (cmdresult.code !== null);
}

/*
 * Returns true iff the command (executed with execChildForTest) exited with
 * status code 2 (indicating a usage error).
 */
function cmdResultExitUsage(cmdresult)
{
    return (cmdresult.code == 2);
}

/*
 * Returns true iff the command (executed with execChildForTest) exited with
 * status code 0 (indicating successful completion).
 */
function cmdResultExitOk(cmdresult)
{
    return (cmdresult.code === 0);
}

/*
 * Returns true iff the command (executed with execChildForTest) produced an
 * error message about required option "required" being missing.
 */
function cmdResultMissingRequired(cmdresult, required)
{
    var text = 'manatee-adm: option is required: --' + required + '\n';
    return (cmdresult.stderr.indexOf(text) != -1);
}

/*
 * Returns true iff the command (executed with execChildForTest) produced a
 * usage message.
 */
function cmdResultHasUsage(cmdresult, whichstream)
{
    var stream;

    assertplus.ok([ 'stdout', 'stderr' ].indexOf(whichstream) != -1);
    stream = whichstream == 'stdout' ?  cmdresult.stdout : cmdresult.stderr;
    return (/Usage:/.test(stream) && /Options:/.test(stream));
}

main();
