var assert = require('assert-plus');
var EventEmitter = require('events').EventEmitter;
var util = require('util');

/**
* FIFO queue used to hold backup requests
*/
function BackupQueue(options) {
        assert.object(options, 'options');
        assert.object(options.log, 'options.log');
        EventEmitter.call(this);

        this.queue = [];
        this.log = options.log;
}

module.exports = BackupQueue;
util.inherits(BackupQueue, EventEmitter);

BackupQueue.prototype.push = function push(object) {
        this.log.info('pushed object %j into queue', object);
        this.queue.push(object);
        this.emit('push', object);
};


BackupQueue.prototype.pop = function pop(callback) {
        var object = this.queue.pop();
        this.log.info('popped object %j from queue', object);
        return callback(object);
};

BackupQueue.prototype.get = function get(uuid, callback) {
        var self = this;
        var log = self.log;
        log.info('getting backupjob with uuid: ' + uuid);
        var job;
        for (var i = 0; i < self.queue.length; i++) {
                var backupJob = self.queue[i];
                if (backupJob.uuid === uuid) {
                        log.info('found backup job', backupJob);
                        job = backupJob;
                        break;
                }
        }
        return callback(job);
};
