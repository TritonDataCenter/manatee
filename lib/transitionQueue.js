var EventEmitter = require('events').EventEmitter;
var util = require('util');
var verror = require('verror');

/**
 * Queue used to serialize state transitions
 */
function TransitionQueue(options) {
    EventEmitter.call(this);
    var self = this;
    this._queue = [];
    this._log = options.log;
    this._emitter = new EventEmitter();

    this._isPause = false; /* lock used to pause the queue */
    this._isExec = false; /* lock used to ensure serial execution */

    var log = self._log;
    self._emitter.on('push', function() {
        if (self._isExec) {
            log.info({
                queue_length: self._queue.length,
                curr_req: req.transition,
                new_req: self._queue[self._queue.length - 1].transition
            }, 'waiting for current transition to finish before ' +
            'executing pushed req');
            return;
        } else {
            exec(self);
        }
    });
}

function exec(self) {
    var log = self._log;
    if (self._isExec) {
        log.info('TransitionQueue.exec: already executing other req, exiting');
        return;
    }
    self._isExec = true;
    var req = self._queue.shift();
    if (!req) {
        log.info('TransitionQueue.exec: no request, exiting');
        self._isExec = false;
        return;
    }

    req.invoke(req.ctx, function(err) {
        log.info({
            transition: req.transition,
            err: err
        }, 'finished transition');
        if (err) {
            self.emit('execError',
                new verror.VError(err, 'transition queue exec error'));
        }
        if (self._queue.length > 0 && !self._isPause) {
            log.info({
                queue_length: self._queue.length,
                paused: self._isPause
            }, 'more elements in the queue, continuing exec');
            exec(self);

        } else {
            log.info({
                queue_length: self._queue.length,
                paused: self._isPause
            }, 'no more elements in queue or queue paused.');
            self._isExec = false;
        }
    });

}

module.exports = TransitionQueue;
util.inherits(TransitionQueue, EventEmitter);

TransitionQueue.prototype.push = function push(req) {
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

TransitionQueue.prototype.pause = function pause() {
    var self = this;
    self._log.info({
        isPaused: self._isPause,
        isExec: self._isExec,
        queueLength: self._queue.length
    }, 'TransitionQueue.pause: entering');
    self._isPause = true;
};

TransitionQueue.prototype.clear = function clear() {
    var self = this;
    self._log.info({
        isPaused: self._isPause,
        isExec: self._isExec,
        queueLength: self._queue.length
    }, 'TransitionQueue.clear: entering');
    self._queue = [];
};

TransitionQueue.prototype.resume = function resume() {
    var self = this;
    self._log.info({
        isPaused: self._isPause,
        isExec: self._isExec,
        queueLength: self._queue.length
    }, 'TransitionQueue.resume: entering');
    self._isPause = false;
    exec(self);
};
