# Manatee
                       _.---.._
          _        _.-' \  \    ''-.
        .'  '-,_.-'   /  /  /       '''.
       (       _                     o  :
        '._ .-'  '-._         \  \-  ---]
                      '-.___.-')  )..-'
                               (_/

# Overview
Mantee is an automated fault monitoring and leader-election system for
strongly-consistent, highly-available writes to PostgreSQL.  It can tolerate
network partitions up to the loss of an entire node without loss of write (nor
read) capability.  Client configuration changes are minimal and failover is
completely free of operator intervention.  New shard members are automatically
replicated upon introduction.

# Features

* Automated liveliness detection, failover, and recovery. Reads are always
  available, even during a failover. Writes are available as soon as the
  failover is complete.

* Automated bootstrap. New peers will bootstrap and join the shard
  without human intervention.

* Data integrity. Built atop [ZFS](http://en.wikipedia.org/wiki/ZFS) and
  [PostgreSQL synchronous
  replication](http://www.postgresql.org/docs/9.2/static/warm-standby.html#SYNCHRONOUS-REPLICATION)
  for safe, reliable
  storage.

# Quick Start

## Server
Checkout the server
[user-guide](https://github.com/joyent/manatee/blob/master/docs/user-guide.md)
for details on server setup.

There is a sample Manatee [vm](http://mantalinktovm) with a provisioned Manatee
shard.

There are also [screen-casts](http://seacow.io/screencasts.htm) on Manatee
installation, setup, and administration.

## Client
Detailed client docs are [here](https://github.com/joyent/node-manatee).
```javascript
var manatee = require('node-manatee');

var client = manatee.createClient({
   "path": "/manatee/1",
   "zk": {
       "connectTimeout": 2000,
       "servers": [{
           "host": "172.27.10.97",
           "port": 2181
       }, {
           "host": "172.27.10.90",
           "port": 2181
       }, {
           "host": "172.27.10.101",
           "port": 2181
       }],
       "timeout": 20000
   }
});
client.once('ready', function () {
    console.log('manatee client ready');
});

client.on('topology', function (urls) {
    console.log({urls: urls}, 'topology changed');
});

client.on('error', function (err) {
    console.error({err: err}, 'got client error');
});
```
