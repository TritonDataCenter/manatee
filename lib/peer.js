/**
 * @constructor
 *
 * Represents a peer in a Postgres shard. Emits a init event when initialized.
 * Convenience class to keep track of a peer's url and zk path.
 *
 * @param {string} path The path of the znode in ZK.
 * @param {object} session The ZK session.
 * @param {object} log The Bunyan log object.
 * @param {function} callback The function callback f(err, self).
 */
function Peer(path, session, log, callback) {
  log.info('Entering Peer constructor for path', path);

  /**
   * The path of the znode
   */
  this.path = path;

  /**
   * The peer's url
   */
  this.url = null;

  var self = this;
  // fetch the url from ZK
  log.info('fetching znode', this.path);
  session.a_get(this.path, false, function(rc, msg, stat, data) {
    log.debug('checking error', rc, msg);
    if (rc !== 0) {
      var err = {
        rc: rc,
        msg: msg,
        stat: stat
      };
      return callback(err);
    }

    self.url = data;
    log.debug('got url', stat, data);
    return callback(null, self);
  });
}

/**
 * A postgres peer in a shard.
 */
module.exports = {
  Peer: Peer,
  compare: compare
};

/**
 * Compares two lock paths.
 * @param {string} a the lock path to compare.
 * @param {string} b the lock path to compare.
 * @return {int} Positive int if a is bigger, 0 if a, b are the same, and
 * negative int if a is smaller.
 */
function compare(a, b) {
  a = a.path;
  b = b.path;
  var seqA = parseInt(a.substring(a.lastIndexOf('-') + 1), 10);
  var seqB = parseInt(b.substring(b.lastIndexOf('-') + 1), 10);

  return seqA - seqB;
}
