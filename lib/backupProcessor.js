var spawn = require('child_process').spawn;
var shell = require('shelljs');
var uuid = require('node-uuid');
var sprintf = util.format;

/**
 * Snapshots are created with timestamps. The latest one will have the largest
 * timestamp.
 */
function getLatestSnapshot(pathToSnaps, callback) {
  var snapshots = shell.ls('-a', pathToSnaps);
  // sort by descending order, grabbing the latest snapshot
  snapshots.sort(function(a , b) {
    return (b - a);
  });

  return snapshot[0];
}

function zfsSend(snapshot, ip, dataset) {
  var send = spawn('zfs', ['send', snapshot]);
  var recv = spawn('ssh', []);
}
