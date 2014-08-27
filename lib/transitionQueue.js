/**
 * @overview The queue used to execute Shard tranition events.
 *
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
var EventEmitter = require('events').EventEmitter;

var assert = require('assert-plus');
var util = require('util');
var verror = require('verror');

/**
 * Queue used to serialize state transitions. Transition objects look like:
 *
 * {
 *      invoke: function (ctx, cb),
 *      ctx: {}
 * }
 *
 * Where invoke is the function that gets run, and the ctx object is the
 * context associated with the invoked function.
 *
 * The queue executes objects serially and atomically. Requests, once executed,
 * can not be interrupted.
 *
 * @constructor
 * @augments EventEmitter
 *
 * @fires TransitionQueue#execError
 *
 * @param {object} options Options object.
 * @param {Bunyan} options.log Bunyan logger.
 *
 * @throws {Error} If the options object is malformed.
 */
function TransitionQueue(options) {
    assert.object(options, 'options');
    assert.object(options.log, 'options.log');

    EventEmitter.call(this);
    var self = this;

    /**
     * @type {array}
     * The underlying queue implementation, holds the transition objects
     */
    this._queue = [];

    /** @type {Bunyan} The bunyan log object */
    this._log = options.log;

    /**
     * @type {EventEmitter}
     * The local event emitter used for coordinating objects in the queue
     */
    this._emitter = new EventEmitter();

    /** @type {boolean} lock used to pause the queue */
    this._isPause = false;
    /** @type {boolean} lock used to ensure serial execution */
    this._isExec = false;

    /** @type {object} the request under execution */
    this._req = null;

    var log = self._log;
    self._emitter.on('push', function () {
        if (self._isExec) {
            log.info({
                queue_length: self._queue.length,
                curr_req: self._req.transition,
                new_req: self._queue[self._queue.length - 1].transition
            }, 'waiting for current transition to finish before ' +
            'executing pushed req');
            return;
        } else {
            self._exec();
        }
    });
}

module.exports = TransitionQueue;
util.inherits(TransitionQueue, EventEmitter);

/**
 * Push a object into the queue. If the queue is currently paused, the object
 * is dropped.
 * @param {object} req The object to push into the queue.
 */
TransitionQueue.prototype.push = function push(req) {
    assert.object(req, 'req');
    assert.func(req.invoke, 'req.invoke');
    assert.object(req.ctx, 'req.ctx');
    var self = this;
    var log = self._log;

    self._log.info({
        isPaused: self._isPause,
        isExec: self._isExec,
        queueLength: self._queue.length,
        req: req.transition
    }, 'TransitionQueue.push: entering');

    if (self._isPause) {
        log.info({req: req.transition},
            'TransitionQueue.push: queue paused, dropping request');
        return;
    }
    log.info({
        req: req.transition
    }, 'pushing request');
    self._queue.push(req);
    self._emitter.emit('push');
};

/**
 * Pause the queue. This will stop further execution of the queue. This will
 * also drop any objects pushed into the queue. If there is an object currently
 * executing, that object will finish.
 */
TransitionQueue.prototype.pause = function pause() {
    var self = this;
    self._log.info({
        isPaused: self._isPause,
        isExec: self._isExec,
        queueLength: self._queue.length
    }, 'TransitionQueue.pause: entering');
    self._isPause = true;
};

/**
 * Clear all objects from the transition queue. This does not stop the
 * currently executing object.
 */
TransitionQueue.prototype.clear = function clear() {
    var self = this;
    self._log.info({
        isPaused: self._isPause,
        isExec: self._isExec,
        queueLength: self._queue.length
    }, 'TransitionQueue.clear: entering');
    self._queue = [];
};

/**
 * Resume the queue from pause. Allow new objects to be pushed onto the queue,
 * and start executing objects if any exist in the queue.
 */
TransitionQueue.prototype.resume = function resume() {
    var self = this;
    self._log.info({
        isPaused: self._isPause,
        isExec: self._isExec,
        queueLength: self._queue.length
    }, 'TransitionQueue.resume: entering');
    self._isPause = false;
    self._exec();
};

/**
 * @return {boolean} Whether there is another element in the queue ready for
 * execution.  This doesn't take into account whether there is a currently
 * executing element.
 */
TransitionQueue.prototype.hasNext = function hasNext() {
    var self = this;
    return (self._queue.length >= 1);
};

/**
 * #@+
 * @private
 * @memberOf TransitionQueue
 */

TransitionQueue.prototype._exec = function () {
    var self = this;
    var log = self._log;
    if (self._isExec) {
        log.info('TransitionQueue.exec: already executing other req, exiting');
        return;
    }
    self._isExec = true;
    var req = self._queue.shift();
    self._req = req;
    if (!req) {
        log.info('TransitionQueue.exec: no request, exiting');
        self._isExec = false;
        return;
    }

    req.invoke(req.ctx, function (err) {
        log.info({
            transition: req.transition,
            err: err
        }, 'finished transition');
        if (err) {
            /**
             * Execution error event.
             *
             * @event TransitionQueue#execError
             * @type {verror.VError}
             */
            self.emit('execError',
                new verror.VError(err, 'transition queue exec error'));
        }
        if (self._queue.length > 0 && !self._isPause) {
            log.info({
                queue_length: self._queue.length,
                paused: self._isPause
            }, 'more elements in the queue, continuing exec');
            self._isExec = false;
            self._exec();

        } else {
            log.info({
                queue_length: self._queue.length,
                paused: self._isPause
            }, 'no more elements in queue or queue paused.');
            self._isExec = false;
        }
    });
};

/** #@- */
