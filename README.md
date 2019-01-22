<!--
    This Source Code Form is subject to the terms of the Mozilla Public
    License, v. 2.0. If a copy of the MPL was not distributed with this
    file, You can obtain one at http://mozilla.org/MPL/2.0/.
-->

<!--
    Copyright (c) 2018, Joyent, Inc.
-->

# Manatee
                       _.---.._
          _        _.-' \  \    ''-.
        .'  '-,_.-'   /  /  /       '''.
       (       _                     o  :
        '._ .-'  '-._         \  \-  ---]
                      '-.___.-')  )..-'
                               (_/

This repository is part of the Joyent SmartDataCenter project (SDC).  For
contribution guidelines, issues, and general documentation, visit the main
[SDC](http://github.com/joyent/sdc) project page.

# Overview

Manatee is an automated fault monitoring and leader-election system for
strongly-consistent, highly-available writes to PostgreSQL.  It can tolerate
network partitions up to the loss of an entire node without loss of write (nor
read) capability.  Client configuration changes are minimal and failover is
completely free of operator intervention.  New shard members are automatically
replicated upon introduction.

Check out the [user-guide](docs/user-guide.md) for details on server internals
and setup.

Problems? Check out the [Troubleshooting guide](docs/trouble-shooting.md).

Migrating from Manatee 1.0 to 2.0?  Check out the [migration
guide](docs/migrate-1-to-2.md).

Working on Manatee? Check out the [Working on Manatee
guide](docs/working-on-manatee.md).

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

## Client
Detailed client docs are [here](https://github.com/joyent/node-manatee).
```javascript
var manatee = require('node-manatee');

var client = manatee.createClient({
   "path": "/manatee/1",
   "zk": {
       "connStr": "172.27.10.97:2181,172.27.10.90:2181,172.27.10.101:2181",
       "opts": {
           "sessionTimeout": 60000,
           "spinDelay": 1000,
           "retries": 60
       }
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
