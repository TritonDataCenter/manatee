<!--
    This Source Code Form is subject to the terms of the Mozilla Public
    License, v. 2.0. If a copy of the MPL was not distributed with this
    file, You can obtain one at http://mozilla.org/MPL/2.0/.
-->

<!--
    Copyright (c) 2014, Joyent, Inc.
-->

                       _.---.._
          _        _.-' \  \    ''-.
        .'  '-,_.-'   /  /  /       '''.
       (       _                     o  :
        '._ .-'  '-._         \  \-  ---]
                      '-.___.-')  )..-'
                               (_/

# Overview
Welcome to the Manatee guide. This guide is intended to give you a quick
introduction to Manatee, and to help you setup your own instance of Manatee.

We assume that you already know how to administer a replicated PostgreSQL(PG)
instance, and are familiar with general Unix concepts. For background
information on HA PostgreSQL, look
[here](http://www.postgresql.org/docs/9.2/static/high-availability.html).

## What is Manatee?
Manatee is an automated failover, fault monitoring and leader-election system
built for managing a set of replicated PG servers. It is written completely in
Node.js.

A traditional replicated PG shard is not resilient to the death or partition
of any of the nodes from the rest. This is generally solved by involving a
human in the loop, such as having monitoring systems which alerts an operator to
fix the shard manually when failure events occur.

Manatee automates failover by using a consensus layer (Zookeeper) and takes the
human out of the loop.  In the face of network partitions or death of any node,
Manatee will automatically detect these events and rearrange the shard topology
so that neither durability nor availability are compromised.  The loss of one
node in a Manatee shard will not impact system read/write availability, even if
it was the primary node in the shard. Reads are still available as long as
there is _at least one_ active node left in the shard.  Data integrity is never
compromised -- Manatee only allows save cluster transitions so that the primary
will always have the complete set of data.

Manatee also simplifies backups, restores and rebuilds. New nodes in the shard
are automatically bootstrapped from existing nodes. If a node becomes
unrecoverrable, it can be rebuilt by restoring from an existing node in the
shard.

## Architectural Components
### Shard Overview
This is the component diagram of a fully setup Manatee shard. Much of the
details of each Manatee node itself has been simplified, with detailed
descriptions in later sections.

![Manatee Architecture](http://us-east.manta.joyent.com/poseidon/public/manatee/docs/Manatee-Shard.jpg "Manatee Shard")

Each shard consists of 3 Manatee nodes, a primary(A), synchronous standby(B)
and asynchronous standby(C) setup in a daisy chain.

[PostgreSQL Cascading
replication](http://www.postgresql.org/docs/9.2/static/warm-standby.html#CASCADING-REPLICATION)
is used in the shard at (3). There is only one direct replication connection to
each node from the node immediately behind it. That is, only B replicates from
A, and only C replicates from B. C will never replicate from A as long as B is
alive.

The primary uses [PostgreSQL synchronous
replication](http://www.postgresql.org/docs/9.2/static/warm-standby.html#SYNCHRONOUS-REPLICATION)
to replicate to the synchronous standby. This ensures data written to Manatee
will be persisted to at least the primary and sync standby. Without this,
failovers of a node in the shard can cause data inconsistency between nodes.
The synchronous standby uses asynchronous replication to the async standby.

The topology of the shard is maintained in Zookeeper.  The primary (A) is
responsible for monitoring the manatee peers that join the cluster and adjusting
the topology to add them as sync or async peers.  The sync (B) is responsible
for adjusting the topology only if it detects via (2) that the primary has
failed.

### Manatee Node Detail
Here are the components encapsulated within a single Manatee node.

![Manatee Node](http://us-east.manta.joyent.com/poseidon/public/manatee/docs/Manatee-Node.jpg "Manatee Node")

#### Manatee Sitter
The sitter is the main process within Manatee. The PG process runs as the
child of the sitter. PG never runs unless the sitter is running. The sitter is
responsible for:

* Managing the underlying PG instance. The PG process runs as a child process
  of the sitter. This means that PG doesn't run unless the sitter is running.
  Additionally, the sitter initializes, restores, and configures the PG instance
  depending on its current role in the shard. (1)
* Managing cluster topology in ZK if it is the primary or the sync (5).
* ZFS is used by PG to persist data (4). The use of ZFS will be elaborated on
  in a later section.

### Manatee Snapshotter
The snapshotter takes periodic ZFS snapshots of the PG instance. (2) These
snapshots are used to backup and restore the database to other nodes in the
shard.

### Manatee Backup Server
The snapshots taken by the snapshotter are made available to other nodes by the
REST backupserver. The backupserver, upon receiving a backup request, will send
the snapshots to another node. (3) (6)

### ZFS
Manatee relies on the [ZFS](http://en.wikipedia.org/wiki/ZFS) file system.
Specifically, ZFS [snapshots](http://illumos.org/man/1m/zfs) are used to create
point-in-time snapshots of the PG database. The `zfs send` and `zfs recv`
utilities are used to restore or bootstrap other nodes in the shard.

# Supported Platforms

Manatee has been tested and runs in production on Joyent's
[SmartOS](http://www.joyent.com/technology/smartos), and should work on most
[Illumos](http://illumos.org) distributions such as OmniOS. Unix-like operating
systems that have ZFS support should work, but have not been tested.

# Dependencies

You must install the following dependencies before installing Manatee.

## Node.js
Manatee requires [Node.js 0.10.26](http://nodejs.org/download/) or later.

## Zookeeper
Manatee requires [Apache Zookeeper 3.4.x](http://zookeeper.apache.org/releases.html).

## PostgreSQL
Manatee requires [PostgreSQL 9.2.x](http://www.postgresql.org/download/) or later.

## ZFS
Manatee requires the [ZFS](http://en.wikipedia.org/wiki/ZFS) file system.

# Installation

Get the latest Manatee [source](https://github.com/joyent/manatee).
Alternatively, you can grab the release
[tarball](https://github.com/joyent/manatee/releases).

Install the package dependencies via npm install.
``` bash
[root@host ~/manatee]# npm install
```

# Configuration

## Setup Zookeeper

Setup a Zookeeper instance by following
[this guide](http://zookeeper.apache.org/doc/trunk/zookeeperAdmin.html).

## Manatee Configuration

Manatee comes with sample configs under
[the etc directory](https://github.com/joyent/manatee/tree/master/etc). Manatee
configuration files are JSON-formatted.

The default configuration files have been well tested and deployed in
production. Refrain from tuning them, especially the timeouts, unless you are
sure of the consequences.

## Configuring PostgreSQL

Manatee comes with a default set of PG
[configs](https://github.com/joyent/manatee/tree/master/etc). You'll want to
tune your `postgresql.conf` with parameters that suit your workload. Refer to
the [PostgreSQL documentation](www.postgres.com).

## SMF
The illumos [Service Management Facility](http://www.illumos.org/man/5/smf) can
be used as a process manager for the Manatee processes. SMF provides service
supervision and restarter functionality for Manatee, among other things. There
is a set of sample SMF manifests under
[the smf directory](https://github.com/joyent/manatee/tree/master/smf).

# Administration
This section assumes you are using SMF to manage the Manatee instances and are
running on SmartOS.

## Creating a new Manatee Shard
The physical location of each Manatee node in the shard is important. Each
Manatee node should be located on a different _physical_ host than the others
in the shard, and if possible in different data centers. In this way, failures
of a single physical host or DC will not impact the availability of your
Manatee shard.

### Setting up SMF
It's recommended that you use a service restarter, such as SMF, with Manatee.
Before running Manatee as an SMF service, you must first import the service
manifest.
``` bash
[root@host ~/manatee]# svccfg import ./smf/sitter.xml
[root@host ~/manatee]# svccfg import ./smf/backupserver.xml
[root@host ~/manatee]# svccfg import ./smf/snapshotter.xml
```

Check the svcs have been imported:
``` bash
[root@host ~]# svcs -a | grep manatee
disabled       17:33:13 svc:/manatee-sitter:default
disabled       17:33:13 svc:/manatee-backupserver:default
disabled       17:33:13 svc:/manatee-snapshotter:default
```

Once the manifests have been imported, you can start the services.
``` bash
[root@host ~/manatee]# svcadm enable manatee-sitter
[root@host ~/manatee]# svcadm enable manatee-backupserver
[root@host ~/manatee]# svcadm enable manatee-snapshotter
```

Repeat these steps on all Manatee nodes. You now have a highly available
automated failover PG shard.

## Adding a new Node to the Manatee Shard
Adding a new node is easy.
* Create a new node.
* Install and configure Manatee from the steps above.
* Setup and start the Manatee SMF services.

This new node will automatically determine its position in the shard and clone
its PG data from its leader. You do not need to manually build the new node.

## Moving Manatee Nodes
In the normal course of maintenance, you may need to move or upgrade the
Manatee nodes in the shard. You can do this by adding a new node to the shard,
waiting for it to catch up (using tools described in the following sectons),
and then removing the node you wanted to move/upgrade.

Additional nodes past the initial 3 will just be asynchronous standbys and will
not impact the shard's performance or availability.

## Shard Administration
Manatee provides the `manatee-adm` utility which gives visibility into the
status of the Manatee shard. One of the key subcommands is the `pg-status` command.

``` bash
$ manatee-adm pg-status 
ROLE     PEER     PG   REPL  SENT       WRITE      FLUSH      REPLAY     LAG   
primary  bb348824 ok   sync  0/671FEDF8 0/671FEDF8 0/671FEDF8 0/671FE9F0 -     
sync     a376df2b ok   async 0/671FEDF8 0/671FEDF8 0/671FEDF8 0/671FE9F0 -     
async    09957297 ok   -     -          -          -          -          0m00s 
```

This prints out the current topology of the shard.  The "PG" column indicates
that postgres is online.

The REPL field shows information about the downstream peer that's currently
connected to this peer for replication purposes.  If the REPL column is "sync"
or "async", it means that the downstream peer is caught up and streaming. If
REPL is "-", it means that there is no PG instance replication from this node.
This is expected with the last peer in the shard.

However, if there is another peer after the current one in the shard, and REPL
is "-", that indicates there is a problem with replication between the two
peers.  The "pg-status" command will explicitly report such issues.

## Shard History
You can query any past topology changes by using the `history` subcommand.
```bash
[root@host ~/manatee]# ./node_modules/manatee/bin/manatee-adm history -s '1' -z <zk_ips>
```

This will return a list of every single cluster state change sorted by time.

## Getting the Mantee version

```
[root@b35e12da (postgres) ~]$ manatee-adm version
2.0.0
```

## Freezing and Unfreezing Clusters

As an operator, there may be times when you want to "freeze" the topoplogy to
keep Manatee from failing over the Postgres sync to Primary.  For example, this
could be due to expected operational maintenance or because the cluster is in
the middle of a migration.  To freeze the cluster state so that it will perform
no transitions, use the `manatee-adm freeze -r [reason]` command.  The reason is
free-form and required.  The reason is meant for operators to consult before
unfreezing the cluster.  When the cluster is frozen, it is prominently displayed
in the output for `manatee-adm show`:

```
[root@b35e12da (postgres) ~]$ manatee-adm freeze -r 'By nate for CM-129'
Frozen.
[root@b35e12da (postgres) ~]$ manatee-adm show | head -6
zookeeper:   172.27.10.142
cluster:     1.moray.emy-10.joyent.us
generation:  1 (0/00000000)
mode:        normal
freeze:      frozen since 2015-03-18T18:40:11.095Z
freeze info: by dap for CM-129
```

To unfreeze the cluster, use the `manatee-adm unfreeze` command.

## Deposed manatees

When a sync takes over becoming the primary, there is a chance that the previous
primary's Postgres transaction logs have diverged.  There are many reasons this
can happen, and the known reasons are documented in the [transaction log
divergence](xlog-diverge.md) doc.  Deposed manatees have the "deposed" tag when
viewing `manatee-adm pg-status`:

```
ROLE     PEER     PG   REPL  SENT       WRITE      FLUSH      REPLAY     LAG   
primary  301e2d2c ok   sync  0/12345678 0/12345678 0/12345678 0/12345678 -     
sync     30219700 ok   async 0/12345678 0/12345678 0/12345678 0/12345678 -     
async    3022acc6 ok   -     -          -          -          -          5m42s 
deposed  30250e3a fail -     -          -          -          -          -     
```

To rebuild a deposed node, log onto the host, run `manatee-adm rebuild`, and
follow the prompts.  If your dataset is particularly large, this can take
a long time.  You should consider running the rebuild in a `screen` session.

To be on the safe side, any deposed primary should be rebuilt in order to rejoin
the cluster.  This may not be necessary in some cases, but is suggested unless
you can determine manually that the Postgres transaction logs (xlogs) haven't
diverged.

***WARNING:*** If you do not rebuild, the logs have diverged, and you
add the node back into the cluster (which will get picked up as an async), your
cluster is at risk of becoming wedged!  This is because the previously deposed
and diverged primary can be promoted to sync.  If that happens, your cluster
will not be able to accept writes.

## Singleton (One-Node-Write) Mode

One-node-write mode (ONWM) is a special mode for manatee clusters that have no
important data.  It allows a primary to accept writes without synchronously
replicating those transactions to a sync.  In fact, with ONWM, any peers that
attempt to join the cluster will shut down if they detect another peer is
already the primary by looking at the cluster state in zookeeper.

ONWM is usually enabled when setting up larger systems that will be moved to
"High Availability" (HA) configurations later.  To move from ONWM to HA requires
downtime.  An operator would:

1. Provision a new manatee peer and configure it as part of the primary's
   cluster.
2. Stop the primary manatee sitter process.
3. Edit configuration for the primary to remove one node write mode.
4. Run `# manatee-adm unfreeze`.
5. Start the primary manatee sitter process.

It is generally not supported or advised to go from an HA manatee cluster to
ONWM.
