/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2018, Joyent, Inc.
 */

var mod_fs = require('fs');
var mod_util = require('util');

var mod_assert = require('assert-plus');
var mod_forkexec = require('forkexec');
var mod_jsprim = require('jsprim');
var mod_verror = require('verror');
var mod_vasync = require('vasync');
var mod_mooremachine = require('mooremachine');

var VE = mod_verror.VError;




function Stdio(log, name, stream, keeplines) {
    var self = this;

    mod_assert.object(log, 'log');
    mod_assert.string(name, 'name');
    mod_assert.object(stream, 'stream');
    mod_assert.bool(keeplines, 'keeplines');

    self.sio_stream = stream;
    self.sio_lstream = new mod_lstream();
    self.sio_writable = new mod_stream.Writable({ objectMode: true,
      highWaterMark: 0 });

    self.sio_finished = false;
    self.sio_finishq = [];
    self.sio_keeplines = keeplines;
    self.sio_lines = [];

    self.sio_writable.on('finish', function () {
        self.sio_finished = true;
        while (self.sio_finishq.length > 0) {
            setImmediate(self.sio_finishq.shift());
        }
    });

    self.sio_writable._write = function (line, _, done) {
        log.debug('%s: %s', name, line);

        if (self.sio_keeplines) {
            self.sio_lines.push(line);
        }

        setImmediate(done);
    };

    self.sio_stream.pipe(self.sio_lstream).pipe(self.sio_writable);
}

Stdio.prototype.lines = function () {
    var self = this;

    mod_assert.ok(self.sio_finished, 'finished');

    return (mod_jsprim.deepCopy(self.sio_lines));
};

Stdio.prototype.wait = function (callback) {
    var self = this;

    mod_assert.func(callback, 'callback');

    if (!self.sio_finished) {
        self.sio_finishq.push(callback);
        return;
    }

    setImmediate(callback);
};



function ProcessFSM() {
    var self = this;

    self.pfsm_resets = 0;

    mod_mooremachine.FSM.call(self, 'rest');

}
mod_util.inherits(ProcessFSM, mod_mooremachine.FSM);

/*
 * Call to start the child process.
 */
ProcessFSM.prototype.start = function (opts) {
    mod_assert.object(opts, 'opts');
    mod_assert.arrayOfString(opts.argv, 'opts.argv');
    mod_assert.ok(opts.argv.length > 0, 'opts.argv needs at least one element');
    mod_assert.number(opts.uid, 'opts.uid');

    var self = this;

    mod_assert.ok(self.isInState('rest'), 'must be in rest state to start()');

    self.pfsm_cmd = opts.argv[0];
    self.pfsm_cmdargs = opts.argv.slice(1);
    self.pfsm_uid = opts.uid;

    self.emit('start_call');
};

/*
 * Call to stop the child process.
 */
ProcessFSM.prototype.stop = function (callback) {
    var self = this;

    if (self.isInState('rest') || self.isInState('closed')) {
        /*
         * We have not yet been started, or we have already completed shutdown.
         */
        setImmediate(callback);
        return;
    }

    if (self.isInState('running')) {
        /*
         * If the process is currently running, trigger the shutdown sequence.
         */
        self.emit('shutdown_call');
    }

    /*
     * Wait for the 'closed' state.
     */
    self.once('closed', function () {
        callback();
    });
};

/*
 * Reset (clear the error state, stderr output, etc) the FSM so that another
 * child process can be started.
 */
ProcessFSM.prototype.reset = function () {
    var self = this;

    if (self.isInState('rest')) {
        return;
    }

    if (self.isInState('closed')) {
        self.emit('reset_call');
        return;
    }

    throw (new VE('cannot reset except in REST or CLOSED state'));
};

/*
 * Provide a function to be called when the state machine reaches the CLOSED
 * state.  It is an error to try and set this function when we are already in
 * the CLOSED state.  The provided function will replace any existing function,
 * if one exists.
 */
ProcessFSM.prototype.set_close_listener = function (callback) {
    var self = this;

    if (self.isInState('closed')) {
        throw (new VE('cannot set close listener when already closed'));
    }

    self.pfsm_close_listener = callback;
};

ProcessFSM.prototype.exit_info = function () {
    var self = this;

    if (!self.isInState('closed')) {
        throw (new VE('must be in state CLOSED to get exit info'));
    }

    mod_assert.object(self.pfsm_exit, 'pfsm_exit');
    var info = mod_jsprim.deepCopy(self.pfsm_exit);
    info.stderr = mod_jsprim.deepCopy(self.pfsm_stderr.lines());

    return (info);
};

/*
 * State: REST
 */
ProcessFSM.prototype.state_rest = function (s) {
    var self = this;

    /*
     * Arguments provided by the consumer when calling start():
     */
    self.pfsm_cmd = null;
    self.pfsm_cmdargs = null;
    self.pfsm_uid = null;

    /*
     * Close listener callback provided via set_close_listener():
     */
    self.pfsm_close_listener = null;

    /*
     * Internal tracking data:
     */
    self.pfsm_resets++;
    self.pfsm_child = null;
    self.pfsm_errors = [];
    self.pfsm_exit = null;
    self.pfsm_stdout = null;
    self.pfsm_stderr = null;

    s.on(self, 'start_call', function () {
        s.gotoState('running');
    });
};

/*
 * State: RUNNING
 */
ProcessFSM.prototype.state_running = function (s) {
    var self = this;

    mod_assert.strictEqual(self.pfsm_child, null, 'two children?');
    try {
        self.pfsm_child = spawn(self.pfsm_cmd, self.pfsm_cmdargs,
          { uid: self.pfsm_uid });
    } catch (ex) {
        /*
         * If we catch a synchronous error, we'll go immediately to the closed
         * state.  No child process handle was returned so there is nothing for
         * us to clean up.
         */
        self.pfsm_errors.push(new VE(ex, 'spawn error'));
        s.gotoState('closed');
        return;
    }

    /*
     * If we get this far, we assume that at least fork(2) succeeded and we got
     * a pid.  Once we set "pfsm_running" we will require a subsequent "exit"
     * event to know that the child process has been cleaned up.
     */
    mod_assert.number(self.pfsm_child.pid, 'child pid');

    /*
     * We want to log the output of the stdout and stderr streams, and keep the
     * stderr stream contents for analysis in the face of unexpected exits.
     */
    self.pfsm_stderr = new Stdio(self.pfsm_log, 'postgres stderr',
      self.pfsm_child.stderr, true);
    self.pfsm_stdout = new Stdio(self.pfsm_log, 'postgres stdout',
      self.pfsm_child.stdout, false);

    /*
     * This is a long-running process.  Unless we are terminating it, we do not
     * expect it to exit; this is an error condition.
     */
    s.on(self.pfsm_child, 'exit', function (code, signal) {
        mod_assert.strictEqual(self.pfsm_exit, null, 'exited already');
        self.pfsm_exit = { code: code, signal: signal, time: Date.now(),
          planned: false };

        s.gotoState('closing');
    });

    /*
     * An "error" event may be emitted either during startup, or as a result of
     * some subsequent error; e.g., if a signal could not be sent to the
     * process.
     */
    s.on(self.pfsm_child, 'error', function (err) {
        self.pfsm_errors.push(new VE(ex, 'child process error'));

        mod_assert.strictEqual(self.pfsm_exit, null, 'no exit yet');

        s.gotoState('running.killing');
    });

    /*
     * If the consumer has requested the shutdown of the process, begin that
     * sequence now.
     */
    s.on(self, 'shutdown_call', function () {
        s.gotoState('shutdown');
    });
};

ProcessFSM.prototype.state_running.killing = function (s) {
    var self = this;

    /*
     * Try to make sure we don't end up with a lingering child process.
     */
    self.pfsm_child.kill('SIGKILL');

    s.timeout(60 * 1000, function () {
        /*
         * If we send SIGKILL.
         */
    });
};

ProcessFSM.prototype.state_closing = function (s) {
    var self = this;

    s.on(self.pfsm_child, 'error', function (err) {
        self.pfsm_errors.push(new VE(ex, 'child process error during close'));
        s.gotoState('error');
    });

    s.gotoState('closing.stdout');
};

ProcessFSM.prototype.state_closing.stdout = function (s) {
    var self = this;

    self.pfsm_stdout.wait(s.callback(function () {
        s.gotoState('closing.stderr');
    }));
};

ProcessFSM.prototype.state_closing.stderr = function (s) {
    var self = this;

    self.pfsm_stderr.wait(s.callback(function () {
        s.gotoState('closed');
    }));
};

ProcessFSM.prototype.state_closed = function (s) {
    var self = this;

    /*
     * Report that we have reached the CLOSED state to the consumer.
     */
    if (self.pfsm_close_listener) {
        setImmediate(self.pfsm_close_listener);
    }

    s.on(self, 'reset_call', function () {
        s.gotoState('rest');
    });
};

ProcessFSM.prototype.state_error = function (s) {
    var self = this;

    if (self.pfsm_child !== null) {
        /*
         * The error occurred after we created the child process handle.  We
         * need to make sure we clean up the child process.
         */

        s.on(self.pfsm_child, 'error', function (err) {
        });
    }
};

/*
 * State: SHUTDOWN
 *
 * In this state, we are attempting a controlled shutdown of the process.
 * This is somewhat PostgreSQL-specific, but that's the only child process
 * we're attempting to manage for now.
 *
 * An error at this point is likely because of a failure to send the
 * appropriate signal; e.g., due to EPERM.
 */
ProcessFSM.prototype.state_shutdown = function (s) {
    var self = this;

    s.on(self.pfsm_child, 'exit', function (code, signal) {
        mod_assert.strictEqual(self.pfsm_exit, null, 'exited already');
        self.pfsm_exit = { code: code, signal: signal, time: Date.now(),
          planned: true };

        s.gotoState('closing');
    });

    s.on(self.pfsm_child, 'error', function (err) {
        /*
         * There is not much we can usefully do about the kinds of errors that
         * will be reported here.  In particular, note that Node will already
         * silence an ESRCH failure for our kill(2) operations.
         *
         * The worst possible class of error would seem to be leaving
         * PostgreSQL partially running when we thought it was stopped, so
         * we'll abort and have SMF tear down our contract.
         */
        process.abort();
    });

    s.gotoState('shutdown.sigint');
};

ProcessFSM.prototype.state_shutdown.sigint = function (s) {
    var self = this;

    /*
     * Sending SIGINT to the PostgreSQL master process results in a "Fast
     * Shutdown".  According to the documentation, the server does not allow
     * new connections and aborts any transactions in flight.
     */
    self.pfsm_child.kill('SIGINT');

    s.timeout(self.pfsm_kill_timeout, function () {
        /*
         * If the process has not exited on its own by this stage, we want to
         * escalate to a more immediate shutdown mode.
         */
        s.gotoState('shutdown.sigquit');
    });
};

ProcessFSM.prototype.state_shutdown.sigquit = function (s) {
    var self = this;

    /*
     * Sending SIGQUIT to the PostgreSQL master process results in an
     * "Immediate Shutdown".  This is more heavy handed and will result in
     * recovery activity at next startup.
     */
    self.pfsm_child.kill('SIGQUIT');

    s.timeout(self.pfsm_kill_timeout, function () {
        /*
         * XXX If the process has not exited on its own at this point, we want
         * to treat it as effectively completely stuck and clean up as best we
         * can.
         */
        s.gotoState('shutdown.sigkill');
    });
};

ProcessFSM.prototype.state_shutdown.sigkill = function (s) {
    var self = this;

    /*
     * Sending SIGKILL to the PostgreSQL master process should result in
     * immediate termination, as with any process.  This is not ideal, with
     * particular respect to the question of what happens with any remaining
     * children of the master process.
     */
    self.pfsm_child.kill('SIGKILL');

    s.timeout(self.pfsm_kill_timeout, function () {
        /*
         * In this case, we are completely off the rails: either the child
         * process cannot be killed, or we have incorrectly handled notice of
         * its termination.  The only safe thing is to abort and have SMF
         * attempt to tear down our contract and restart.
         */
        process.abort();
    });
};




module.exports = {
};
