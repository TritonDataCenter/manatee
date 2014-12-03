<!--
    This Source Code Form is subject to the terms of the Mozilla Public
    License, v. 2.0. If a copy of the MPL was not distributed with this
    file, You can obtain one at http://mozilla.org/MPL/2.0/.
-->

<!--
    Copyright (c) 2014, Joyent, Inc.
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

Check out the
[user-guide](https://github.com/joyent/manatee/blob/master/docs/user-guide.md)
for details on server internals and setup.

Problems? Check out the [Troubleshooting
guide](https://github.com/joyent/manatee/blob/master/docs/trouble-shooting.md).

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

# Working on Manatee

When working on Manatee, it's convenient to be able to run multiple instances in
the same environment.  This won't allow you to test all possible failure modes,
but it works for most basic functionality.

This process involves:

* Deploying a zone with the right version of postgres available and with the
  ability to manage ZFS datasets.  Manatee needs to be able to run as root
  inside this zone.
* Installing postgres, git, gcc, and other tools required to build.
* Creating a ZFS dataset for each Manatee peer you want to run.  (We'll assume
  three instances in this guide.)
* Creating a configuration file for each Manatee peer you want to run.
* Starting each peer by hand.


## Summary

These steps assume you've already got ZooKeeper running somewhere.  In
the steps below, $ZK_CONN_STR is a connection string, or a comma-separated list
of IP:PORT pairs for the zookeeper cluster.

Run all of the following as root:

1. Provision a SmartOS zone using multiarch 13.3.1.  This is image
   4aec529c-55f9-11e3-868e-a37707fcbe86.  Be sure to provision the zone with a
   delegated ZFS dataset.
1. Log into the zone and run the following steps as root (or with sudo or
   as another privileged user).
1. Install packages:

        # pkgin -y in gmake scmgit gcc47 postgresql92-server-9.2.4nb1 \
            postgresql92-adminpack postgresql92-replicationtools \
            postgresql92-upgrade lz4-120

1. Get and build a local copy of this repo:

        # git clone https://github.com/joyent/manatee
        # cd manatee
        # git checkout MANATEE-188
        # make

1. Pick an IP address from "ifconfig -a".  We'll call this $SELF_IP.  The IP to
   use will depend on your configuration.  The Manatee services will bind to
   this IP, so don't pick a public IP unless that's really what you want.

1. Run the setup script

        # ./tools/mkdevsitters $SELF_IP $ZK_CONN_STR

1. For each peer ("1", "2", "3"), open up two terminals.  In the first, start
   the sitter:

        # node sitter.js -f devconfs/sitter1/sitter.json | bunyan

   In the second terminal, start the backup server:

        # node backupserver.js -f devconfs/sitter1/backupserver.json | bunyan

If you want to clean everything up (**note: this will destroy all data stored
in these peers!)**, run:

    # for peer in 1 2 3; do zfs destroy -R zones/$(zonename)/data/peer$peer; done

**This command is very destructive!  Be sure you're okay with destroying the
datasets, snapshots, and clones of all of the peers you created before you run
this command.**

Then run:

    # rm -rf devconfs

## Details

This section has more details about the above procedure.

### Provisioning a development zone

We develop Manatee in SmartOS zones running under SDC.  You should be able to
run on standalone SmartOS (i.e., not running under SDC), or even other systems
with ZFS and Postgres installed (e.g., BSD).  Manatee requires access to ZFS
datasets to create snapshots, send streams, and the like, and it also must
run as root.  The former currently rules out the Joyent Public Cloud as a
deployment option.

We deploy Manatee using the multiarch 13.3.1 image (equivalent to image
4aec529c-55f9-11e3-868e-a37707fcbe86).  For development, we recommend using a
zone based on that image, deployed on a network with a ZooKeeper instance
running.  On SDC, be sure to set `delegate_dataset=true` when provisioning.  On
standalone SmartOS, set `delegate_dataset=true` when you invoke "vmadm create".

### Installing packages

You'll need git, GNU make, a compiler toolchain, lz4, and the postgres client,
server, and tools.  On the above multiarch SmartOS zone, you can install these
with:

    # pkgin -y in gmake scmgit gcc47 postgresql92-server-9.2.4nb1 \
        postgresql92-adminpack postgresql92-replicationtools \
        postgresql92-upgrade lz4-120

### Creating ZFS datasets and configurations

There's a tool inside the repo called "mkdevsitters" which configures the local
system to run three Manatee peers.  You'll have to run the three peers by hand.
The script just creates configuration files and ZFS datasets.  The script must
be run as root.

To use the script, you'll need to know:

* The local IP address you intend to use for these Manatee peers.  If you don't
  know, you can run "ifconfig -a" and pick one.  The tool does not do this
  automatically because common develompent environments have multiple addresses,
  only one of which is correct for this purpose, and it's impossible for the
  script to know which to use.
* The IP address and port of a remote ZooKeeper server.  The port is usually
  2181.  The value you use here is actually a comma-separated list of IP:PORT
  pairs.

To use this script, as the root user, run:

    # ./tools/mkdevsitters MY_IP ZK_IPS

For example, if my local IP is 172.21.1.74 and there's a ZooKeeper server at
172.21.1.11, I might run this as root:

    # ./tools/mkdevsitters 172.21.1.74 172.21.1.11:2181

This does several things:

* Creates a directory called "devconfs" in the current directory.  "devconfs"
  will contain the configuration and data for each of the three test peers.
* Creates three ZFS datasets under zones/$(zonename)/data, called "peer1",
  "peer2", and "peer3".  The mountpoints for these datasets are in
  "devconfs/datasets".
* Creates configuration files for the Manatee sitter and Manatee backup server
  in "devconfs/sitterN".  Also creates a template postgres configuration file
  in the same directory.

The various services associated with each peer (postgres itself, the sitter's
status API, the backup server, and so on) are all configured to run on different
ports.  The first peer runs on the default ports; subsequent peers run on ports
numbered 10 more than the previous port.  The default postgres port is 5432, so
the first peer runs postgres on port 5432, the second peer runs postgres on port
5442, and the third peer runs postgres on port 5452.


### Running each peer

There are currently two components to run for each peer: the sitter (which also
starts postgres) and the backup server (which is used for bootstrapping
replication for new downstream peers).  To start the first peer, use:

    # node sitter.js -f devconfs/sitter1/sitter.json

You'll probably want to pipe this to bunyan.  Be sure to run this as root.  To
run other peers, replace "sitter1" with "sitter2" or "sitter3".

Similarly, to run the backupserver, use:

    # node backupserver.js -f devconfs/sitter1/backupserver.json

There's also a snapshotter, but running that for development is not yet
documented.
