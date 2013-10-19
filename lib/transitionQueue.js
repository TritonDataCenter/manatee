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

    var log = self._log;
    var isPop = false;
    var req = null;
    self._emitter.on('push', function() {
        if (isPop) {
            log.info({
                queue_length: self._queue.length,
                curr_req: req.transition,
                new_req: self._queue[self._queue.length - 1].transition
            }, 'waiting for current transition to finish before ' +
            'executing pushed req');
            return;
        } else {
            exec();
        }
    });

    function exec() {
        isPop = true;
        req = self._queue.shift();
        req.invoke(req.ctx, function(err) {
            log.info({
                transition: req.transition,
                err: err
            }, 'finished transition');
            if (err) {
                self.emit('execError',
                    new verror.VError(err, 'transition queue exec error'));
            }
            if (self._queue.length > 0) {
                log.info({
                    queue_length: self._queue.length
                }, 'more elements in the queue,' +
                ' continuing exec');
            exec();
            } else {
                log.info({
                    queue_length: self._queue.length
                }, 'no more elements in queue');
                isPop = false;
            }
        });
    }
}

module.exports = TransitionQueue;
util.inherits(TransitionQueue, EventEmitter);

TransitionQueue.prototype.push = function push(req) {
    var self = this;
    var log = self._log;
    log.info({
        req: req.transition
    }, 'pushing request');
    self._queue.push(req);
    self._emitter.emit('push');
};

TransitionQueue.prototype.stop = function stop() {

};

TransitionQueue.prototype.clear = function clear() {

};
