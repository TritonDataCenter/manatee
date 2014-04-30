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

We assume that you already know how to administor a replicated PostgreSQL(PG)
instance, and are familiar with general Unix concepts. For background
information on HA PostgreSQL, look
[here](http://www.postgresql.org/docs/9.2/static/high-availability.html).

## What is Manatee?
Manatee is an automated fault monitoring and leader-election system built for
managing a set of replicated PG servers. It is written completely in Node.js.

A traditional replicated PG shard is not resilient to the death or partition
of any of the nodes from the rest. This is generally solved by involving a
human in the loop. e.g.  Having monitoring systems which alerts an operator to
fix the shard when failure events occur. Worse still, involving an operator
may lead to operator error, and can result in irrecoverable database
corruption.

Additionally, backups, restores and rebuilds of nodes in the shard are often
difficult, error prone, and time consuming.

Manatee automates failure detection and takes the human out of the loop.  In
the face of network partitions or node death, Manatee will automatically detect
these events, and rearrange the shard topology so that neither durability nor
availability is compromised. Manatee also simplifies backups, restores and
rebuilds. New nodes in the shard are automatically bootstrapped from existing
nodes. If a node becomes unrecoverrable, it can be rebuilt by restoring from an
existing node in the shard.

## Architectural Components
### Shard Overview
This is the component diagram of a fully setup Manatee Shard. Much of the
details of each Manatee node itself has been simplified, and the detailed
review will follow in later sections.

![Manatee Architecture](http://us-east.manta.joyent.com/yunong/public/manatee/docs/Manatee-Shard.jpg "Manatee Shard")

Each shard consists of 3 Manatee nodes, a Primary, Synchronous Standby and
Asynchronous Standby.  It's important to note the Shard is a daisy chain. Using
node A as an example, it is only aware of the node directly in front of it
(none) and the node directly behind it, B.

[PostgreSQL Cascading
replication](http://www.postgresql.org/docs/9.2/static/warm-standby.html#CASCADING-REPLICATION)
is used in the Shard at (3). There is only one direct replication connection to
each node from the node immediately behind it. e.g. only B replicates from A,
and only C replicates from B. C will never replicate from A as long as B is
alive.

The primary uses [PostgreSQL synchronous
replication](http://www.postgresql.org/docs/9.2/static/warm-standby.html#SYNCHRONOUS-REPLICATION)
to the synchronous standby. This ensures data written to Manatee will be
persisted to at least the primary and sync standby. Without this, failovers of
a node in the shard can cause data inconsistency between nodes. The
synchronous standby uses asynchronous replication to the async standby.

The topology of the Shard is maintained by (1) and (2). Leaders, i.e. who is
the node directly in front of self? are determined by (2) via Zookeeper(ZK).
Standbys are determined by the standby node itself, the standby in thei case B,
determines via (2) that its leader is A. And communicates its standby status to
A via (1).

### Manatee Node Detail
Here are the components encapsulated within a single Manatee node.

![Manatee Node](http://us-east.manta.joyent.com/yunong/public/manatee/docs/Manatee-Node.jpg "Manatee Node")

#### Manatee Sitter
The sitter is the main process in within Manatee. The PG process runs as the
child of the sitter. PG never runs unless the sitter is running. The sitter is
responsible for:

* Managing the underlying PG instance. The PG process runs as a child process
  of the sitter. This means that PG doesn't run unless the sitter is running.
  Additionally, the sitter initializes, restores, and configures the PG instance
  depending on its current role in the Shard. (1)
* Managing leadership status with ZK. Each sitter participates in a ZK
  election, and is notified when its leader node has changed. (7)
* Managing standby heartbeats from its standby if it exists. When a standby
  expires, the sitter will remove the standby from its replication list. (6)
* Heartbeating to its leader. This is the process by which the node in front is
  made aware of its standby. This doesn't occur if the node has no leader. (5)
* ZFS is used by PG to persist data (4). The use of ZFS will be elaborated on
  in a later section.

### Manatee Snapshotter
The snapshotter takes periodic ZFS snapshots of the PG instance. (2) These
snapshots are used to backup and restore the database to other nodes in the
shard.

### Manatee Backupserver
The snapshots taken by the snapshotter are made available to other nodes by the
REST backupserver. The backupserver, upon receiving a backup request, will send
the snapshots to another node. (3) (8)

### ZFS
Manatee relies on the [ZFS](http://en.wikipedia.org/wiki/ZFS) file system.
Specifically, ZFS [snapshots](http://illumos.org/man/1m/zfs) are used to create
point in time snapshots of the PG database. The ZFS send and recv utilites are
used to restore or bootstrap other nodes in the shard.

# Supported Platforms

Manatee has been tested and runs in production on Joyent's
[SmartOS](http://www.joyent.com/technology/smartos), and should work on most
[Illumos](http://illumos.org) distributions such as OmniOS. Unix like operating
systems that have ZFS support should work, but they haven't been tested.

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
[root@fa858d48-4cc5-6cd9-a6b0-9d07f5603265 ~/manatee]# npm install
```

# Configuration

## Setup Zookeeper

Setup a Zookeeper instance by following this
[guide](http://zookeeper.apache.org/doc/trunk/zookeeperAdmin.html).

## Manatee Configuration

Manatee comes with sample configs under the ```./etc``` directory. Manatee
configs are in JSON format.

The default configs have been well tested and deployed in production. Refrain
from tuning them, especially the timeouts, unless you are sure of the
consequences.

## Configuring PostgreSQL

Manatee comes with a default set of PG configs. You'll want to tune your
postgresql.conf with parameters that suit your workload. Refer to the
PostgreSQL [documentation](www.postgres.com).

## SMF
The Solaris [Service Management Facility](http://www.illumos.org/man/5/smf) can
be used as a process manager for the manatee processes. SMF provides restarter
functionality for Manatee, among other things. There is a set of sample SMF
manifests under the ```./smf``` directory.

# Administration
This section assumes you are using SMF to manage the Manatee instances and are
running on Joyent's SmartOS.

## Importing SMF Manifests
Before running Manatee as an SMF service, you must first import the service
manifest.
``` bash
[root@fa858d48-4cc5-6cd9-a6b0-9d07f5603265 ~/manatee]# svccfg import ./smf/sitter.xml
[root@fa858d48-4cc5-6cd9-a6b0-9d07f5603265 ~/manatee]# svccfg import ./smf/backupserver.xml
[root@fa858d48-4cc5-6cd9-a6b0-9d07f5603265 ~/manatee]# svccfg import ./smf/snapshotter.xml
```

Check the svcs have been imported:
``` bash
[root@manatee2 ~]# svcs -a | grep manatee
disabled       17:33:13 svc:/manatee-sitter:default
disabled       17:33:13 svc:/manatee-backupserver:default
disabled       17:33:13 svc:/manatee-snapshotter:default
```

Once the manifests have been imported, you can start the services.
``` bash
[root@fa858d48-4cc5-6cd9-a6b0-9d07f5603265 ~/manatee]# svcadm enable manatee-sitter
[root@fa858d48-4cc5-6cd9-a6b0-9d07f5603265 ~/manatee]# svcadm enable manatee-backupserver
[root@fa858d48-4cc5-6cd9-a6b0-9d07f5603265 ~/manatee]# svcadm enable manatee-snapshotter
```

## Shard Status
The node-manatee client provides the `manatee-stat` utility which gives
visibility into the status of the Manatee shard.
``` bash
[root@fa858d48-4cc5-6cd9-a6b0-9d07f5603265 ~/manatee]# ./node_modules/manatee/bin/manatee-stat -p <zk_ip>
{
"1": {
    "primary": {
        "ip": "172.27.4.12",
        "pgUrl": "tcp://postgres@172.27.4.12:5432/postgres",
        "zoneId": "b515b611-3b6e-4cc9-8d1f-3cf78e27bf24",
        "repl": {
            "pid": 23047,
            "usesysid": 10,
            "usename": "postgres",
            "application_name": "tcp://postgres@172.27.5.8:5432/postgres",
            "client_addr": "172.27.5.8",
            "client_hostname": "",
            "client_port": 42718,
            "backend_start": "2014-04-28T17:07:55.548Z",
            "state": "streaming",
            "sent_location": "E/D9D14780",
            "write_location": "E/D9D14780",
            "flush_location": "E/D9D14780",
            "replay_location": "E/D9D14378",
            "sync_priority": 1,
            "sync_state": "sync"
        }
    },
    "sync": {
        "ip": "172.27.5.8",
        "pgUrl": "tcp://postgres@172.27.5.8:5432/postgres",
        "zoneId": "d2de0030-986e-4dae-991d-7d85f2e333d9",
        "repl": {
            "pid": 95136,
            "usesysid": 10,
            "usename": "postgres",
            "application_name": "tcp://postgres@172.27.3.7:5432/postgres",
            "client_addr": "172.27.3.7",
            "client_hostname": "",
            "client_port": 53461,
            "backend_start": "2014-04-28T21:28:07.905Z",
            "state": "streaming",
            "sent_location": "E/D9D14780",
            "write_location": "E/D9D14780",
            "flush_location": "E/D9D14780",
            "replay_location": "E/D9D14378",
            "sync_priority": 0,
            "sync_state": "async"
        }
    },
    "async": {
        "ip": "172.27.3.7",
        "pgUrl": "tcp://postgres@172.27.3.7:5432/postgres",
        "zoneId": "70d44638-f4fb-4cbd-8611-0f7c83d8502f",
        "repl": {},
        "lag": {
            "time_lag": {}
        }
    },
    "registrar": {
        "type": "database",
        "database": {
            "primary": "tcp://postgres@172.27.4.12:5432/postgres",
            "ttl": 60
        }
    }
}
}
```
This prints out the current topology of the shard. The "repl" field shows the
standby that's currently connected to the node. It's the same set of fields
that is returned by the PG query ```select * from pg_stat_replication```. The
repl field is helpful in verifying the status of the PostgreSQL instances. If
there is no repl field, it means that the PG instance isn't running on that
node.

## Shard History
You can query any past topology changes by using the `manatee-history` tool.
```bash
[root@fa858d48-4cc5-6cd9-a6b0-9d07f5603265 ~/manatee]# ./node_modules/manatee/bin/manatee-history '1' <zk_ips>
{"time":"1394597418025","date":"2014-03-12T04:10:18.025Z","ip":"172.27.3.8","action":"AssumeLeader","role":"Leader","master":"","slave":"","zkSeq":"0000000000"}
{"time":"1394597438430","date":"2014-03-12T04:10:38.430Z","ip":"172.27.4.13","action":"NewLeader","role":"Standby","master":"172.27.3.8","slave":"","zkSeq":"0000000001"}
{"time":"1394597451091","date":"2014-03-12T04:10:51.091Z","ip":"172.27.3.8","action":"NewStandby","role":"Leader","master":"","slave":"5432","zkSeq":"0000000002"}
{"time":"1394597477394","date":"2014-03-12T04:11:17.394Z","ip":"172.27.5.9","action":"NewLeader","role":"Standby","master":"172.27.4.13","slave":"","zkSeq":"0000000003"}
{"time":"1394597473513","date":"2014-03-12T04:11:13.513Z","ip":"172.27.4.13","action":"NewStandby","role":"Standby","master":"172.27.3.8","slave":"5432","zkSeq":"0000000004"}
{"time":"1395446372034","date":"2014-03-21T23:59:32.034Z","ip":"172.27.5.9","action":"NewLeader","role":"Standby","master":"172.27.3.8","slave":"","zkSeq":"0000000005"}
{"time":"1395446374071","date":"2014-03-21T23:59:34.071Z","ip":"172.27.3.8","action":"NewStandby","role":"Leader","master":"","slave":"5432","zkSeq":"0000000006"}
{"time":"1395449937712","date":"2014-03-22T00:58:57.712Z","ip":"172.27.4.13","action":"NewLeader","role":"Standby","master":"172.27.5.9","slave":"","zkSeq":"0000000007"}
{"time":"1395449938742","date":"2014-03-22T00:58:58.742Z","ip":"172.27.5.9","action":"NewStandby","role":"Standby","master":"172.27.3.8","slave":"5432","zkSeq":"0000000008"}
{"time":"1395450121000","date":"2014-03-22T01:02:01.000Z","ip":"172.27.5.9","action":"NewLeader","role":"Standby","master":"172.27.4.13","slave":"","zkSeq":"0000000009"}
{"time":"1395450121005","date":"2014-03-22T01:02:01.005Z","ip":"172.27.5.9","action":"NewStandby","role":"Standby","master":"172.27.4.13","slave":"5432","zkSeq":"0000000010"}
{"time":"1395450121580","date":"2014-03-22T01:02:01.580Z","ip":"172.27.4.13","action":"NewLeader","role":"Standby","master":"172.27.3.8","slave":"","zkSeq":"0000000011"}
{"time":"1395450122033","date":"2014-03-22T01:02:02.033Z","ip":"172.27.4.13","action":"NewStandby","role":"Standby","master":"172.27.3.8","slave":"5432","zkSeq":"0000000012"}
```

This will return a list of every single manatee flip sorted by time. The node
is identified by the "ip" field. The type of transition is identified by the
"action" field. The current role of the node is identified by the "role" field.
Only the first element of the daisy chain, i.e. the Primary node, will ever
have the role of Leader. All other nodes will be standbys.

 There are 3 different types of manatee flips.
* AssumeLeader. This means the node has become the primary of the Shard. This
  might mean that the primary has changed. Verify against the last AssumeLeader
  event to see if the primary has changed.
* NewLeader. This means the current node's leader may have changed. the
  "master" field represents its new leader. Note that its leader may not
  neccessarily be the primary of the shard, it only represents the node directly
  in front of the current node.
* NewStandby. This means the current node's standby may have changed. The
  "slave" field represents the new standby node.

This tool is invaluable when trying to reconstruct the timeline of flips in a
Manatee shard, especially when the shard is in safe mode.

## Safe Mode
You can check that the shard is in safe mode via `manatee-stat`.
``` bash
[root@fa858d48-4cc5-6cd9-a6b0-9d07f5603265 ~/manatee]# ./node_modules/manatee/bin/manatee-stat -p <zk_ip>
{
    "1": {
        "error": {
            "primary": "172.27.4.12",
            "zoneId": "8372b732-007c-400c-9642-9eb63d169cf2"
        }
    }
}
```

When in safe mode, manatee will be only available for reads but you will not be
able to write to it until it is manually cleared by an operator.

Before we run through how to bring a manatee back up from safe mode, it's
important to discuss why safe mode exists.

There are situations through a series of manatee flips where the Shard's data
could potentially become inconsistent. Consider the following scenario. We have
a Manatee shard at rest with nodes A, B, and C. Refer to the Shard Overview
section for the diagram of this topology.

Now imagine that all 3 nodes restart at around the same time. If C enters the
shard first, it becomes the primary of the Shard. Because it was previously the
async, its PG xlog may not be completely up to date. If C starts taking writes
at this point then the data between C, and A/B will become forked and the
shard has become inconsistent.

Manatee will not allow this to happen. The shard will enter safe mode if
detects a mismatch between the primary and its standby's xlogs.

In practice this situation could occur frequently, e.g. in the face of network
partitions. Instead of taking a 33% chance each time this happens that the
shard will enter safe mode (if the async gets promoted to primary), Manatee
proactively tries to avoid this by persisting information about the previous
tolographical state. Only the previous primary or sync can be promoted to the
primary.

However, due to some limitations in PG itself, this is sometimes not
sufficient. In this case, the Shard will detect that it can no longer take
writes and put itself into safe mode.

### Clearing a Shard from Safe Mode
In order to clear a shard, you must first figure out who the last primary is.
The correct potential primary is the node with the largest PG current xlog
location. Follow these stesp to figure out who has the largest current xlog
location. On each node:

* Shutdown the manatee-sitter process. `svcadm disable manatee-sitter`
* Remove `recovery.conf` from the postgres data directory. `rm
  <path_to_data_dir>/recovery.conf`. You can't query the current xlog location
  if a node is in standby mode.
* Start up PG manually `sudo -u postgres postgres -D <path_to_data_dir>`
* Query the latset xlog location

```
[root@fa858d48-4cc5-6cd9-a6b0-9d07f5603265 ~/manatee]# sudo -u postgres psql
postgres=# select * from pg_current_xlog_location();
 pg_current_xlog_location
--------------------------
 6F/E3C53568
(1 row)
```
* Stop PG. `kill -2 <postgres pid>`

Once you've figured out the node that has the largest xlog location, then that
node will become the new primary of the shard. Follow these steps:

* You'll want to delete the file pointed to by the config key
  `SyncStateCheckerCfg.cookielocation` that's in the sitter config on disk
  first.
* Ensure that all other manatee nodes in the shard are down. You should have
  done this as part of querying for the latest xlog location.
* Clear the shard from error node by running the `manatee-clear` tool. This
  removes the `error` node from ZK.
* Restart manatee on this node. `svcadm enable manatee-sitter`.
* Verify this node is up and running. Check via `manatee-stat` to see if you
  see a `primary` entry. And by attempting to login to the database via `sudo
  -u postgres psql`.
* Enable the manatee-siter on the other two nodes via `svcadm enable
  manatee-sitter`
* Check via `manatee-stat` and `psql` as before to verify the shard is up and
  running.


