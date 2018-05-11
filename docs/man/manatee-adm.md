# manatee-adm(1) -- Administration tools for Manatee


## SYNOPSIS

`manatee-adm COMMAND [OPTIONS...] [ARGS...]`


## DESCRIPTION

The `manatee-adm` command is used to inspect and administer a Manatee cluster.

Most of the commands here require the following options to identify the
ZooKeeper cluster and cluster name (shard name):

-z, --zk `ZK_IPS` (or environment variable `ZK_IPS`)
    ZooKeeper connection string.  This should be a list of comma-
    separated IP:PORT pairs.  For example: `10.0.1.1:2181,10.0.1.2:2181`.

-s, --shard `SHARD` (or environment variable `SHARD`)
    Cluster (shard) name.  manatee-adm expects to find cluster state
    at path /manatee/`SHARD` in the ZooKeeper namespace.

**Important note for programmatic users:** Except as noted below, the output
format for this command is subject to change at any time.  The only subcommands
whose output is considered committed are:

* `manatee-adm peers`, only when used with the "-o" option
* `manatee-adm pg-status`, only when used with the "-o" option
* `manatee-adm verify`, only when used without the "-v" option

The output for any other commands may change at any time.  Documented
subcommands, options, and arguments are committed, and you can use the exit
status of the program to determine success or failure.

Commands are divided into six groups:

* Meta commands: help, version
* Status commands: show, peers, pg-status, history, verify
* Cluster maintenance commands: freeze, unfreeze, rebuild, reap
* Upgrade commands: state-backfill
* Developer commands: set-onwm, zk-state, zk-active
* Deprecated and internal commands: check-lock, status

## META COMMANDS

### help

Show usage information for a given subcommand.

### version

Show the version of this Manatee client.  In a typical deployment, you run
"manatee-adm" from a given peer, in which case this reports the version number
for the peer's Manatee software as well.


## STATUS COMMANDS

### show [-v | --verbose]

Show basic information about the cluster, including the ZooKeeper IPs, shard
name, current generation number, whether the cluster is frozen, whether the
cluster is configured for singleton or normal mode, and the list of peers and
their Postgres status (similar to "manatee-adm pg-status").

If there are any issues with the cluster, they will also be reported (similar to
"manatee-adm verify").

-v, --verbose
    Show identifying information about all peers (similar to
    "manatee-adm peers").


### peers [-H | --omitHeader] [-o | --columns COLNAME[,...]] [-r | --role ROLE]

Show a table of basic information about peers assigned in the cluster (the
primary, the sync, any asyncs, and any deposed peers).  Peers connected to the
cluster but not assigned a role are not shown.

-H, --omitHeader
    Do not print the header containing the column labels.

-o, --columns `COLNAME,...`
    Only print the named columns.  See below for the list of column
    names.

-r, --role `ROLE`
    Only show peers with role ROLE (e.g., "primary")

Supported columns include:

* `peername`: the full name of each peer (usually a uuid)
* `peerabbr`: the short name of each peer (usually the first 8 characters of the
  uuid)
* `role`: the peer's role in the cluster ("primary", "sync", "async", or
  "deposed")
* `ip`: the peer's reported IP address

### pg-status [-w | --wide] [-H | --omitHeader] [-o | --columns COLNAME[,...]] [-r | --role ROLE] [PERIOD [COUNT]]

Show a table of Postgres status information for assigned peers in the cluster.
This is very similar to "manatee-adm peers", but supports additional columns for
showing Postgres status.

-H, --omitHeader
    Do not print the header containing the column labels.

-o, --columns `COLNAME,...`
    Only print the named columns.  See below for the list of column
    names.

-r, --role `ROLE`
    Only show peers with role ROLE (e.g., "primary")

-w, --wide
    Use default columns except show full peernames instead of shortened
    peernames.  This output may exceed 80 columns.

If `PERIOD` is specified, repeatedly fetches and displays status every `PERIOD`
seconds.  If `COUNT` is also specified, exits after doing this `COUNT` times.

Supported columns include all the columns supported by "manatee-adm peers",
plus:

* `pg-online` (Postgres status): "ok" if Postgres on the specified peer was
  successfully contacted and "fail" otherwise
* `pg-repl` (replication status): If replication is established from the given
  peer to a downstream peer, then this corresponds to the "sync\_state" field in
  Postgres's "pg\_stat\_replication" view.
* `pg-sent`: If downstream replication is established, this corresponds to the
  "sent\_location" field in Postgres's "pg\_stat\_replication" view.
* `pg-write`: If downstream replication is established, this corresponds to the
  "write\_location" field in Postgres's "pg\_stat\_replication" view.
* `pg-flush`: If downstream replication is established, this corresponds to the
  "flush\_location" field in Postgres's "pg\_stat\_replication" view.
* `pg-replay`: If downstream replication is established, this corresponds to the
  "replay\_location" field in Postgres's "pg\_stat\_replication" view.
* `pg-lag`: If upstream replication is established, this corresponds to the
  difference between now and "pg\_last\_xact\_replay\_timestamp()".  This is
  intended to be a measure of how far replication is lagging, but it's only
  useful for that purpose if data is actually being written upstream.

**Example output**

Here's a healthy cluster:

    # manatee-adm pg-status
    ROLE     PEER     PG   REPL  SENT       WRITE      FLUSH      REPLAY     LAG   
    primary  09957297 ok   sync  0/79E8E0A0 0/79E8E0A0 0/79E8E0A0 0/79E8DCA0 -     
    sync     a376df2b ok   async 0/79E8E0A0 0/79E8E0A0 0/79E8E0A0 0/79E8DCA0 -     
    async    bb348824 ok   -     -          -          -          -          0m00s

Here's a cluster that's experiencing non-critical issues:

    # manatee-adm pg-status
    ROLE     PEER     PG   REPL  SENT       WRITE      FLUSH      REPLAY     LAG   
    primary  09957297 ok   sync  0/79E88D28 0/79E88D28 0/79E88D28 0/79E884B0 -     
    sync     a376df2b ok   -     -          -          -          -          -     
    async    bb348824 fail -     -          -          -          -          -     

    warning: peer "a376df2b": downstream replication peer not connected

Here's a cluster that's experiencing a critical issue that's affecting service:

    # manatee-adm pg-status
    ROLE     PEER     PG   REPL  SENT       WRITE      FLUSH      REPLAY     LAG   
    primary  bb348824 fail -     -          -          -          -          -     
    sync     09957297 ok   async 0/79DE6478 0/79DE6478 0/79DE6478 0/79DE6478 -     
    async    a376df2b ok   -     -          -          -          -          0m33s 

    error: cannot query postgres on primary: peer "bb348824": ECONNREFUSED
    error: peer "bb348824": downstream replication peer not connected

### history [-j | --json] [-s | --sort SORTFIELD] [-v | -verbose]

Show the history of Manatee state transitions.  Each time a Manatee peer writes
cluster state, a copy is put under /history in ZooKeeper.  This tool shows these
state transitions in human-readable form.

-j, --json
    Show output in newline-separated JSON suitable for programmatic
    consumption.

-s, --sort `SORTFIELD`
    Sorts events by `SORTFIELD`, which must be either "zkSeq" (the
    default) or "time".  This is rarely useful, but can be important
    in cases where the ZooKeeper sequence number does not match
    chronological order (which generally indicates a serious bug or
    misconfiguration).

-v, --verbose
    Show a human-readable summary for each state transition.

**Example output**

    # manatee-adm history
    TIME                     G# MODE  FRZ PRIMARY  SYNC     ASYNC    DEPOSED 
    2015-03-12T22:14:08.681Z  1 multi -   bb348824 a376df2b -        -       
    2015-03-12T22:14:24.594Z  1 multi -   bb348824 a376df2b 09957297 -       
    2015-03-19T18:15:52.011Z  1 multi -   bb348824 a376df2b -        -       
    2015-03-19T18:16:26.587Z  1 multi -   bb348824 a376df2b 09957297 -       
    2015-03-19T18:18:16.024Z  2 multi -   bb348824 09957297 -        -       
    2015-03-19T18:18:41.639Z  2 multi -   bb348824 09957297 a376df2b -       
    2015-03-19T18:19:50.016Z  3 multi -   09957297 a376df2b -        bb348824
    2015-03-19T18:21:29.033Z  3 multi -   09957297 a376df2b bb348824 -       

or, with annotations:

    # manatee-adm history -v
    TIME                     G# MODE  FRZ PRIMARY  SYNC     ASYNC    DEPOSED  SUMMARY
    2015-03-12T22:14:08.681Z  1 multi -   bb348824 a376df2b -        -        cluster setup for normal (multi-peer) mode
    2015-03-12T22:14:24.594Z  1 multi -   bb348824 a376df2b 09957297 -        async "09957297" added
    2015-03-19T18:15:52.011Z  1 multi -   bb348824 a376df2b -        -        async "09957297" removed
    2015-03-19T18:16:26.587Z  1 multi -   bb348824 a376df2b 09957297 -        async "09957297" added
    2015-03-19T18:18:16.024Z  2 multi -   bb348824 09957297 -        -        primary (bb348824) selected new sync (was a376df2b, now 09957297)
    2015-03-19T18:18:41.639Z  2 multi -   bb348824 09957297 a376df2b -        async "a376df2b" added
    2015-03-19T18:19:50.016Z  3 multi -   09957297 a376df2b -        bb348824 sync (09957297) took over as primary (from bb348824)
    2015-03-19T18:21:29.033Z  3 multi -   09957297 a376df2b bb348824 -        async "bb348824" added, "bb348824" no longer deposed

When using "-j", the output is newline separated JSON where each line is the
time and updated cluster state.  Note that history objects for Manatee 1.0 will
also be included in the output.

For Manatee v2.0 events, each line contains the following fields:

* `time` ISO 8601 timestamp of the event
* `state` The cluster state object at that time.
* `zkSeq` The ZooKeeper sequence number for this event

For Manatee v1.0 events, each line contains the following fields.

* `time` MS since epoch of the transition event.
* `date` Time in UTC of the transition event.
* `ip` IP address of the peer.
* `action` Transition event type, one of
    * `AssumeLeader`, the peer has become the primary of this shard.
    * `NewLeader`, the peer has a new leader it's replicating from.
    * `NewStandby`, the peer has a new standby it's replicating to.
    * `ExpiredStandby`, the peer's current standby has expired from the shard.
* `role` Current role of the peer, one of `Leader` or `Standby`. The primary of
  the shard will be `Leader`, and all other peers will be `Standby`.
* `master` Peer we are replicating from.
* `slave` Peer we are replicating to.
* `zkSeq` Internal tracker of the number of state transitions.

### verify [-v | --verbose]

Fetches the full status of the cluster and diagnoses common issues.  Issues are
divided into errors, which are critical and usually indicate that service is
down, and warnings, which usually indicate that the cluster is providing service
but administrative attention is still required.

The output is one of these issues per line, prefixed with either "error:" or
"warning:" depending on the severity of the issue.  The command exits 0 if there
are no issues and exits non-zero if there are any issues reported.

-v, --verbose
    Explicitly report when there are no issues.  Normally, the command
    outputs nothing when no issues were found.


## CLUSTER MAINTENANCE COMMANDS

### freeze (-r | --reason REASON)

Freezes the cluster so that no state transitions (e.g., takeover operations)
will be carried out.  This is typically used for disruptive maintenance
operations where the operator would prefer that the system not attempt to react
to peer failures (at the possible expense of availability).  `REASON` must be
provided, but it's only a note for operators.  `REASON` is shown by the
"manatee-adm show" command.

-r, --reason `REASON`
    The reason the operator is freezing this shard.

### unfreeze

Unfreezes the shard so that takeover operations may be carried out in response
to peer failures.  See "freeze" above.


### rebuild [-c | --config CONFIG_FILE] [-y | --ignorePrompts]

Rebuild a peer (typically a deposed peer).  In the event that this peer is
unable to join the cluster (usually due to being a deposed peer, but also as a
result of unexpected Postgres xlog divergence), this command will attempt a full
rebuild of the peer from the primary peer.  This can take a long time, depending
on the size of the database.

Use this tool carefully.  This command completely removes the local copy of the
database, so it should only be run when you're sure that there are enough copies
elsewhere to satisfy your durability requirements and when you know that there
is no important data only stored in this copy.  If the peer is actually deposed
and the cluster is functioning, then Manatee guarantees this peer will not have
any unique committed data.

-c, --config `CONFIG_FILE` (or environment variable `MANATEE_SITTER_CONFIG`)
    Path to Manatee sitter config file.  The default is
    `/opt/smartdc/manatee/etc/sitter.cfg`.

-y, --ignorePrompts
    Skip confirmation prompts.


### reap [-c | --config CONFIG_FILE] [-i | --ip IP] [-n | --zonename ZONENAME]

Removes a non-existent peer from the list of deposed peers.

**This is only to be used for peers that have been permanently decommissioned.
If you want to bring a deposed peer back into service, use the "manatee-adm
rebuild" command.**

This operation is rarely necessary.  It is only used when the primary fails in a
way that will never be recovered (e.g, if the physical system has failed
catastrophically and permanently).  As part of normal cluster operations, such a
peer will become deposed, and the cluster will wait for an operator to rebuild
that peer (see "manatee-adm rebuild").  But if the peer is permanently gone,
that will never happen.  This command simply removes the peer from the deposed
list.

This operation only applies to deposed peers, since other peers are
automatically removed from the cluster when they're absent.  Deposed peers are
the only peers which remain in the cluster state when they're absent.  This is
an important safety feature, since deposed peers generally cannot rejoin the
cluster successfully.  If you reap a peer that is not actually gone and it
subsequently rejoins the cluster, subsequent replication to that peer may fail
and the cluster may be unable to maintain service when all peers ahead of that
peer have failed.

You can either use an IP address or a zonename to identify the peer to reap. If
neither a zonename nor an IP address is specified, the current zone's zonename
will be used.

-c, --config `CONFIG_FILE` (or environment variable `MANATEE_SITTER_CONFIG`)
    Path to Manatee sitter config file.  The default is
    `/opt/smartdc/manatee/etc/sitter.cfg`.

-i, --ip `IP`
    The IP address of the peer to remove.

-n, --zonename `ZONENAME`
    The zonename of the peer to remove.


### promote [-n | --zonename ZONENAME] [-r | --role ROLE] [-i | --asyncIndex INDEX]

Initiate a request to promote the specified peer to the next applicable position
in the topology.  The primary is responsible for acting on a promotion request
in most cases, with the only exception being where the sync is to be promoted
which will result in deposing the primary, which subsequently will require a
rebuild.  It's possible for a cluster to ignore our promotion request (see
"clear-promote" for details).

The impact of this request varies depending on what peer is promoted.
Initiating a promotion request using this subcommand reduces the time it takes
for a cluster to take action on a planned takeover but can still result in data
path downtime.  The type of downtime can be expected to be the same as if the
cluster experienced a takeover naturally and is outlined below.

- sync promotion: deposed primary, read downtime for duration of async to sync
  transition, write downtime for duration of sync to primary transition
  (including time taken to establish synchronous replication)
- first async promoted: no read downtime, write downtime for duration of async
  transition to sync
- other async promoted: no impact

When promoting the sync or the async in a cluster with only one async, only
`--role` and `--zonename` are required.  For clusters with more than one async,
`--asyncIndex` is required in order to determine which async is to be promoted.
These are required in order to prevent race conditions in the event that the
cluster changes topology while we are composing our promotion request.

Any warnings or errors reported by the cluster will result in a failed
promotion request.  It is possible to ignore these warnings interactively at a
prompt, but the reported warnings should be carefully reviewed before ignoring
them.

Example usage:

Requesting the promotion of the sync peer:

    # manatee-adm promote --role=sync \
        --zonename=4e27e2a9-2ff9-4c22-afd1-6cc9909c056c

Requesting the promotion of the second async peer:

    # manatee-adm promote --role=async \
        --zonename=83f16f67-3c08-4022-8435-8bd0c65262eb \
        --asyncIndex=1

-n, --zonename `ZONENAME`
    The zonename of the peer to promote.

-r, --role `ROLE`
    The current role of the peer to promote.

-i, --asyncIndex `INDEX`
    The zero-indexed position of the peer to be promoted's position in
    the async chain (if applicable).

### clear-promote

If a promotion request is not acted upon by the cluster and is still present in
the cluster's state object then this subcommand can be used to clear it.

An example of an ignored request would be where an operator has requested the
promotion of the sync, but immediately after initiating the request the async
was removed from the cluster.  In this case, the cluster has no replacement for
the sync, so the request will be ignored.  The request will still be present in
the cluster's state if this happens and will not affect the ongoing function of
the cluster, but an operator might make use of this subcommand to clear the
request in the interest of tidiness.

## UPGRADE COMMANDS

### state-backfill

Migration tool for moving from Manatee 1.0 to 2.0.  Please see the Manatee
documentation on migration for the appropriate use of this tool.


## DEVELOPER COMMANDS

These commands fetch or modify internal data structures and should only be used
by developers or as part of documented procedures.

### set-onwm [-y | --ignorePrompts] (-m | --mode (on | off))

Toggles the singleton ("one-node-write") mode property of the cluster state.
This is generally not required, and is not the normal way to enable
one-node-write mode.  See the documentation for details, and use with caution.

-m, --mode on|off
    Set one-node-write mode on or off.

-y, --ignorePrompts
    Skip confirmation prompts.

### zk-state

Fetches the raw cluster state from ZooKeeper and show it.  This is the canonical
object that all Manatee peers use to configure themselves.

### zk-active

Fetches the list of active Manatee peers currently connected to ZooKeeper.  This
may include duplicates, since there's one object reported per active ZooKeeper
session.


## DEPRECATED AND INTERNAL COMMANDS

### status [-l | --legacyOrderMode] [-s | --shard SHARD]

Show a JSON representation of the status of Manatee shards. By default, the
status for every shard is returned.

This command is deprecated.  Operators should use the "pg-status" command
instead.  Programs should use the "verify" command instead.

-l, --legacyOrderMode
    Show the topology based on Manatee v1.0 semantics, which is based
    on node order in `/election` in ZooKeeper rather than a carefully-
    managed cluster state.

-s, --shard `SHARD`
    Show status for the specified shard only.

The output encapsulates the state of the Manatee shard.  Each peer in the shard
is denoted by its role in the shard, which will be either `primary`, `sync`, or
`async`. If there are greater than 3 peers in the shard, each additional peer
will be denoted `asyncN`, where N is an integer starting at 1.

The "online" field indicates if Postgres is currently running.

The output also indicates whether the topology is frozen for that shard and also
lists any deposed peers.  Deposed peers are named similar to how asyncs are
named.

The `repl` field contains Postgres replication information of the next peer
in the shard. On the primary, this would be the `sync` peer, and on the `sync`
this would be the `async` peer.


### check-lock (-p | --path LOCK_PATH)

Check the existence of a path in ZooKeeper, which is used as a boolean
configuration flag.  Exits with status 1 if the lock exists and 0 if it does
not.

-p, --path `LOCK_PATH`
    Lock path in ZooKeeper. (e.g., `/my_special_lock`)


## ENVIRONMENT

`ZK_IPS`
    In place of `-z, --zookeeper`

`SHARD`
    In place of `-s, --shard`

`MANATEE_SITTER_CONFIG`
    In place of `-c, --config`

`LOG_LEVEL`
    Sets the node-bunyan logging level. Defaults to fatal.


## COPYRIGHT

Copyright (c) 2018 Joyent Inc., All rights reserved.
