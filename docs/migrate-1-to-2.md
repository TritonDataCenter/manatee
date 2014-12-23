# Performing a Migration from Manatee 1.0 to 2.0

Moving from Manatee 1.0 to 2.0 is a relatively simple process, which is outlined
in detail in this document.  The major difference between Manatee 1.0 and 2.0 is
that in Manatee 1.0 the cluster topology is determined by the order in which
manatee peers connect to the zookeeper cluster and add to the `election` path.
In Manatee 2.0 the topology is kept in a persistent node called `state` in
zookeeper, and the `election` path is only used to determine the set of active
nodes.

The overall steps to migrating a manatee cluster from 1.0 to 2.0 is as the
topology are as follows:

1. Upgrade Morays
1. Reprovision Async
1. Backfill Cluster State (`async$ manatee-adm state-backfill`)
1. Reprovision Sync
1. Unfreeze Cluster State (`async$ manatee-adm unfreeze`)
1. Reprovision Primary
1. Rebuild Deposed Primary

Details for each step are below, specifically things you can check to make sure
your migration is running smoothly.  It is assumed that:

1. You are running a shard with three nodes
2. You are familiar with running the `manatee-adm` command
3. You are familiar with reprovisioning zones
4. You are familiar with where to find logs

## Upgrade Morays

Newer versions of Morays are forward-compatible with either way of determining
topology (either via the `/election` or `/state` path) and can switch between
using either method as the `/state` appears and disappears.  First make sure
that the Morays have been upgraded to a version that is compatible.

To verify:

In SDC or Manta, get the image version and validate that the version timestamp
is greater than `20141204T233537Z`.  For example:

```
[root@headnode (coal) ~]# sdc-imgapi /images/$(sdc-vmapi /vms/$(vmadm lookup alias=~moray) | json -Ha image_uuid) | json -Ha version
master-20141204T233537Z-g4861422
```

Outside of SDC and Manta, you'll need to determine the git sha of the code you
are running and verify it is past this commit:

```
commit 486142204a3347d9bd356647a39c7ea113a62fc3
Date:   Thu Dec 4 23:34:19 2014 +0000
```

## Reprovision Async

First take note of your current topology:

```
async$ manatee-adm status | json
```

As an example, this is the topology we'll be using as an example:

```
f29499ea-b50c-431e-9975-e4bf760fb5e1 primary
4afba482-7670-4cfe-b11f-9df7f558106a sync
d0c715ab-1d55-43cd-88f2-f6bfe3960683 async
```

Verify that all members of your shard are up and operational.  Take note of the
topology, specifically which zone is the primary, the sync and the async.
Reprovision the async to a manatee version that uses a persisted state object
for the topology.

When the async zone is provisioned, you can look at the logs and verify that
it thinks it is in 'migration':

```
[2014-12-02T18:27:15.984Z] DEBUG: manatee-sitter/cluster/99642 on d0c715ab-1d55-
43cd-88f2-f6bfe3960683 (/opt/smartdc/manatee/node_modules/manatee/node_modules/m
anatee-state-machine/lib/manatee-peer.js:385 in ManateePeer.evalClusterState): c
luster not yet setup
[2014-12-02T18:27:15.984Z]  INFO: manatee-sitter/cluster/99642 on d0c715ab-1d55-
43cd-88f2-f6bfe3960683 (/opt/smartdc/manatee/node_modules/manatee/node_modules/m
anatee-state-machine/lib/manatee-peer.js:306 in ManateePeer.assumeUnassigned): a
ssuming role (role=unassigned)
```

The newly upgraded async will remain in this state until the cluster state is
backfilled.

## Backfill Cluster State

Run the backfill from the async.  Before accepting the configuration, verify:

1. The primary is listed as the primary
1. The original async is listed as the *sync*
1. The original sync is listed as the *async*
1. That there is a "freeze" member to the configuration.

```
[root@d0c715ab (postgres) ~]$ manatee-adm state-backfill
Computed new cluster state:
{ primary:
   { zoneId: 'f29499ea-b50c-431e-9975-e4bf760fb5e1',
     ip: '10.77.77.47',
     pgUrl: 'tcp://postgres@10.77.77.47:5432/postgres',
     backupUrl: 'http://10.77.77.47:12345',
     id: '10.77.77.47:5432:12345' },
  sync:
   { zoneId: 'd0c715ab-1d55-43cd-88f2-f6bfe3960683',
     ip: '10.77.77.49',
     pgUrl: 'tcp://postgres@10.77.77.49:5432/postgres',
     backupUrl: 'http://10.77.77.49:12345',
     id: '10.77.77.49:5432:12345' },
  async:
   [ { zoneId: '4afba482-7670-4cfe-b11f-9df7f558106a',
       ip: '10.77.77.48',
       pgUrl: 'tcp://postgres@10.77.77.48:5432/postgres',
       backupUrl: 'http://10.77.77.48:12345',
       id: '10.77.77.48:5432:12345' } ],
  generation: 0,
  initWal: '0/0000000',
  freeze:
   { date: '2014-12-02T18:31:35.394Z',
     reason: 'manatee-adm state-backfill' } }
is this correct(y/n)
prompt: yes:  yes
Ok.
```

When you accept the backfill, the original async will reconfigure itself as a
sync to the original primary.  You can see this in the logs of the original
async:

```
[2014-12-02T18:35:38.863Z]  INFO: manatee-sitter/cluster/99642 on d0c715ab-1d55-
43cd-88f2-f6bfe3960683 (/opt/smartdc/manatee/node_modules/manatee/node_modules/m
anatee-state-machine/lib/manatee-peer.js:889): (nretries=0)
    pg: applied config { role: 'sync',
      upstream:
       { zoneId: 'f29499ea-b50c-431e-9975-e4bf760fb5e1',
         ip: '10.77.77.47',
         pgUrl: 'tcp://postgres@10.77.77.47:5432/postgres',
         backupUrl: 'http://10.77.77.47:12345',
         id: '10.77.77.47:5432:12345' },
      downstream: null }
```

The cluster is now in a configuration where the the original sync and async are
both slaving from the original primary.  You can check that the cluster state
is correct by running `async$ manatee-adm state | json`.

Note that since this is a non-standard configuration `manatee-adm status` will
**not** look like a "healthy" manatee shard:

```
[root@4afba482 (postgres) ~]$ manatee-adm status | json
{
  "1.moray.coal.joyent.us": {
    "primary": {
      "zoneId": "f29499ea-b50c-431e-9975-e4bf760fb5e1",
      "ip": "10.77.77.47",
      "pgUrl": "tcp://postgres@10.77.77.47:5432/postgres",
      "repl": {
        "pid": 96837,
        "usesysid": 10,
        "usename": "postgres",
        "application_name": "tcp://postgres@10.77.77.48:5432/postgres",
        "client_addr": "10.77.77.48",
        "client_hostname": "",
        "client_port": 45461,
        "backend_start": "2014-12-02T18:08:12.582Z",
        "state": "streaming",
        "sent_location": "0/174A270",
        "write_location": "0/174A270",
        "flush_location": "0/174A270",
        "replay_location": "0/174A270",
        "sync_priority": 1,
        "sync_state": "sync"
      }
    },
    "sync": {
      "zoneId": "4afba482-7670-4cfe-b11f-9df7f558106a",
      "ip": "10.77.77.48",
      "pgUrl": "tcp://postgres@10.77.77.48:5432/postgres",
      "repl": {}
    },
    "async": {
      "zoneId": "d0c715ab-1d55-43cd-88f2-f6bfe3960683",
      "ip": "10.77.77.49",
      "pgUrl": "tcp://postgres@10.77.77.49:5432/postgres",
      "backupUrl": "http://10.77.77.49:12345",
      "repl": {},
      "lag": {
        "time_lag": null
      }
    }
  }
}
```

## Reprovision Sync

Now reprovision the sync to the same version of software that the original async
was upgraded to.  Since the cluster state has been backfilled, the sync will
take its place as the async, slaving from the original async (now the sync).
You can see this in the original sync's logs:

```
[2014-12-02T18:47:39.927Z]  INFO: manatee-sitter/cluster/629 on 4afba482-7670-4c
fe-b11f-9df7f558106a (/opt/smartdc/manatee/node_modules/manatee/node_modules/man
atee-state-machine/lib/manatee-peer.js:287 in ManateePeer.assumeAsync): assuming
 role (role=async, which=0)
[2014-12-02T18:47:39.929Z] DEBUG: manatee-sitter/cluster/629 on 4afba482-7670-4c
fe-b11f-9df7f558106a (/opt/smartdc/manatee/node_modules/manatee/node_modules/man
atee-state-machine/lib/manatee-peer.js:837 in pgReconfig):
    pg.reconfigure { role: 'async',
      upstream:
       { zoneId: 'd0c715ab-1d55-43cd-88f2-f6bfe3960683',
         ip: '10.77.77.49',
         pgUrl: 'tcp://postgres@10.77.77.49:5432/postgres',
         backupUrl: 'http://10.77.77.49:12345',
         id: '10.77.77.49:5432:12345' },
      downstream: null }
```

At this point `manatee-adm status should look "normal".  From an upgraded
manatee, the "__FROZEN__" property will be present.  From an older manatee
(only the primary at this point), that property wouldn't exist:

```
[root@d0c715ab (postgres) ~]$ manatee-adm status | json
{
  "1.moray.coal.joyent.us": {
    "__FROZEN__": "2014-12-02T18:31:35.394Z: manatee-adm state-backfill",
    "primary": {
      "zoneId": "f29499ea-b50c-431e-9975-e4bf760fb5e1",
      "ip": "10.77.77.47",
      "pgUrl": "tcp://postgres@10.77.77.47:5432/postgres",
      "backupUrl": "http://10.77.77.47:12345",
      "id": "10.77.77.47:5432:12345",
      "online": true,
      "repl": {
        "pid": 30,
        "usesysid": 10,
        "usename": "postgres",
        "application_name": "tcp://postgres@10.77.77.49:5432/postgres",
        "client_addr": "10.77.77.49",
        "client_hostname": "",
        "client_port": 38457,
        "backend_start": "2014-12-02T18:35:37.928Z",
        "state": "streaming",
        "sent_location": "0/174A438",
        "write_location": "0/174A438",
        "flush_location": "0/174A438",
        "replay_location": "0/174A438",
        "sync_priority": 1,
        "sync_state": "sync"
      }
    },
    "sync": {
      "zoneId": "d0c715ab-1d55-43cd-88f2-f6bfe3960683",
      "ip": "10.77.77.49",
      "pgUrl": "tcp://postgres@10.77.77.49:5432/postgres",
      "backupUrl": "http://10.77.77.49:12345",
      "id": "10.77.77.49:5432:12345",
      "online": true,
      "repl": {
        "pid": 644,
        "usesysid": 10,
        "usename": "postgres",
        "application_name": "tcp://postgres@10.77.77.48:5432/postgres",
        "client_addr": "10.77.77.48",
        "client_hostname": "",
        "client_port": 40786,
        "backend_start": "2014-12-02T18:47:40.335Z",
        "state": "streaming",
        "sent_location": "0/174A438",
        "write_location": "0/174A438",
        "flush_location": "0/174A438",
        "replay_location": "0/174A438",
        "sync_priority": 0,
        "sync_state": "async"
      }
    },
    "async": {
      "zoneId": "4afba482-7670-4cfe-b11f-9df7f558106a",
      "ip": "10.77.77.48",
      "pgUrl": "tcp://postgres@10.77.77.48:5432/postgres",
      "backupUrl": "http://10.77.77.48:12345",
      "id": "10.77.77.48:5432:12345",
      "online": true,
      "repl": {},
      "lag": {
        "time_lag": null
      }
    }
  }
}
```

## Unfreeze Cluster State

Unfreezing the cluster state allows the new sync to take over as the primary
when the primary is reprovisioned.  This is done by:

```
async$ manatee-adm unfreeze
```

If you forget this step, when the primary is reprovisioned the sync will emit
warnings that it should have taken over, but couldn't due to the cluster being
frozen.  For example:

```
[2014-12-02T19:00:24.173Z] DEBUG: manatee-sitter/cluster/99642 on d0c715ab-1d55-
43cd-88f2-f6bfe3960683 (/opt/smartdc/manatee/node_modules/manatee/node_modules/m
anatee-state-machine/lib/manatee-peer.js:576 in ManateePeer.startTakeover): prep
aring for new generation (primary gone)
[2014-12-02T19:00:24.173Z]  WARN: manatee-sitter/cluster/99642 on d0c715ab-1d55-
43cd-88f2-f6bfe3960683 (/opt/smartdc/manatee/node_modules/manatee/node_modules/m
anatee-state-machine/lib/manatee-peer.js:673): backing off
    ClusterFrozenError: cluster is frozen
        at Array.takeoverCheckFrozen [as 0] (/opt/smartdc/manatee/node_modules/m
        at Object.waterfall (/opt/smartdc/manatee/node_modules/manatee/node_modu
        at ManateePeer.startTakeover (/opt/smartdc/manatee/node_modules/manatee/
        at ManateePeer.evalClusterState (/opt/smartdc/manatee/node_modules/manat
        at null._onTimeout (/opt/smartdc/manatee/node_modules/manatee/node_modul
        at Timer.listOnTimeout [as ontimeout] (timers.js:110:15)
```

## Reprovision Primary

Now reprovision the primary.  At this point one of two things will happen.  If
the reprovision took longer than the zookeeper node timeout, the sync will take
over as primary.  If the primary finished reprovisioning before the timeout, the
primary will remain the primary.  In our case, the reprovision took longer, so
we can see the original async taking over as the primary:

```
[2014-12-02T19:01:52.451Z]  INFO: manatee-sitter/cluster/99642 on d0c715ab-1d55-
43cd-88f2-f6bfe3960683 (/opt/smartdc/manatee/node_modules/manatee/node_modules/m
anatee-state-machine/lib/manatee-peer.js:693): declared new generation
[2014-12-02T19:01:52.451Z]  INFO: manatee-sitter/cluster/99642 on d0c715ab-1d55-
43cd-88f2-f6bfe3960683 (/opt/smartdc/manatee/node_modules/manatee/node_modules/m
anatee-state-machine/lib/manatee-peer.js:250 in ManateePeer.assumePrimary): assu
ming role (role=primary)
[2014-12-02T19:01:52.452Z] DEBUG: manatee-sitter/cluster/99642 on d0c715ab-1d55-
43cd-88f2-f6bfe3960683 (/opt/smartdc/manatee/node_modules/manatee/node_modules/m
anatee-state-machine/lib/manatee-peer.js:837 in pgReconfig):
    pg.reconfigure { role: 'primary',
      upstream: null,
      downstream:
       { zoneId: '4afba482-7670-4cfe-b11f-9df7f558106a',
         ip: '10.77.77.48',
         pgUrl: 'tcp://postgres@10.77.77.48:5432/postgres',
         backupUrl: 'http://10.77.77.48:12345',
         id: '10.77.77.48:5432:12345' } }
```

In this case you can see that the cluster state has declared a new generation
(generation 1):

```
[root@d0c715ab (postgres) ~]$ manatee-adm state | json
{
  "generation": 1,
  "primary": {
    "id": "10.77.77.49:5432:12345",
    "ip": "10.77.77.49",
    "pgUrl": "tcp://postgres@10.77.77.49:5432/postgres",
    "zoneId": "d0c715ab-1d55-43cd-88f2-f6bfe3960683",
    "backupUrl": "http://10.77.77.49:12345"
  },
  "sync": {
    "zoneId": "4afba482-7670-4cfe-b11f-9df7f558106a",
    "ip": "10.77.77.48",
    "pgUrl": "tcp://postgres@10.77.77.48:5432/postgres",
    "backupUrl": "http://10.77.77.48:12345",
    "id": "10.77.77.48:5432:12345"
  },
  "async":[],
  "deposed": [
    {
      "id": "10.77.77.47:5432:12345",
      "zoneId": "f29499ea-b50c-431e-9975-e4bf760fb5e1",
      "ip": "10.77.77.47",
      "pgUrl": "tcp://postgres@10.77.77.47:5432/postgres",
      "backupUrl": "http://10.77.77.47:12345"
    }
  ],
  "initWal": "0/174A4D0"
}
```

## Rebuild Deposed Primary

This step is optional depending on whether the sync took over as primary or not.
If the sync did take over as primary (as in the previous step), you should
see a manatee peer tagged with "deposed" when running `manatee-adm status`.

This deposed peer will need to be rebuilt.  To do so, log onto the host, run
`manatee-adm rebuild` and follow the prompts.  Once the rebuild is complete,
you should see the deposed peer become an async.

Your cluster should now look "healthy".  You can also see that `manatee-adm
history` reflects the progression of changes:

```
[root@d0c715ab (postgres) ~]$ manatee-adm history
{"time":"1417543665280","date":"2014-12-02T18:07:45.280Z","ip":"10.77.77.47:5432","action":"AssumeLeader","role":"Leader","master":"","slave":"","zkSeq":"0000000000"}
{"time":"1417543693514","date":"2014-12-02T18:08:13.514Z","ip":"10.77.77.47:5432","action":"NewStandby","role":"leader","master":"","slave":"10.77.77.48:5432","zkSeq":"0000000001"}
{"time":"1417543693593","date":"2014-12-02T18:08:13.593Z","ip":"10.77.77.48:5432","action":"NewLeader","role":"Standby","master":"10.77.77.47:5432","slave":"","zkSeq":"0000000002"}
{"time":"1417544064976","date":"2014-12-02T18:14:24.976Z","ip":"10.77.77.49:5432","action":"NewLeader","role":"Standby","master":"10.77.77.48:5432","slave":"","zkSeq":"0000000003"}
{"time":"1417545337553","date":"2014-12-02T18:35:37.553Z","state":{"primary":{"zoneId":"f29499ea-b50c-431e-9975-e4bf760fb5e1","ip":"10.77.77.47","pgUrl":"tcp://postgres@10.77.77.47:5432/postgres","backupUrl":"http://10.77.77.47:12345","id":"10.77.77.47:5432:12345"},"sync":{"zoneId":"d0c715ab-1d55-43cd-88f2-f6bfe3960683","ip":"10.77.77.49","pgUrl":"tcp://postgres@10.77.77.49:5432/postgres","backupUrl":"http://10.77.77.49:12345","id":"10.77.77.49:5432:12345"},"async":[{"zoneId":"4afba482-7670-4cfe-b11f-9df7f558106a","ip":"10.77.77.48","pgUrl":"tcp://postgres@10.77.77.48:5432/postgres","backupUrl":"http://10.77.77.48:12345","id":"10.77.77.48:5432:12345"}],"generation":0,"initWal":"0/0000000","freeze":{"date":"2014-12-02T18:31:35.394Z","reason":"manatee-adm state-backfill"}},"zkSeq":"0000000004"}
{"time":"1417546098034","date":"2014-12-02T18:48:18.034Z","ip":"10.77.77.47:5432","action":"NewStandby","role":"leader","master":"","slave":"10.77.77.49:5432","zkSeq":"0000000005"}
{"time":"1417546912449","date":"2014-12-02T19:01:52.449Z","state":{"generation":1,"primary":{"id":"10.77.77.49:5432:12345","ip":"10.77.77.49","pgUrl":"tcp://postgres@10.77.77.49:5432/postgres","zoneId":"d0c715ab-1d55-43cd-88f2-f6bfe3960683","backupUrl":"http://10.77.77.49:12345"},"sync":{"zoneId":"4afba482-7670-4cfe-b11f-9df7f558106a","ip":"10.77.77.48","pgUrl":"tcp://postgres@10.77.77.48:5432/postgres","backupUrl":"http://10.77.77.48:12345","id":"10.77.77.48:5432:12345"},"async":[],"initWal":"0/174A4D0"},"zkSeq":"0000000006"}
{"time":"1417547498986","date":"2014-12-02T19:11:38.986Z","state":{"generation":1,"primary":{"id":"10.77.77.49:5432:12345","ip":"10.77.77.49","pgUrl":"tcp://postgres@10.77.77.49:5432/postgres","zoneId":"d0c715ab-1d55-43cd-88f2-f6bfe3960683","backupUrl":"http://10.77.77.49:12345"},"sync":{"zoneId":"4afba482-7670-4cfe-b11f-9df7f558106a","ip":"10.77.77.48","pgUrl":"tcp://postgres@10.77.77.48:5432/postgres","backupUrl":"http://10.77.77.48:12345","id":"10.77.77.48:5432:12345"},"async":[],"deposed":[{"id":"10.77.77.47:5432:12345","zoneId":"f29499ea-b50c-431e-9975-e4bf760fb5e1","ip":"10.77.77.47","pgUrl":"tcp://postgres@10.77.77.47:5432/postgres","backupUrl":"http://10.77.77.47:12345"}],"initWal":"0/174A4D0"},"zkSeq":"0000000007"}
{"time":"1417547623781","date":"2014-12-02T19:13:43.781Z","state":{"generation":1,"primary":{"id":"10.77.77.49:5432:12345","ip":"10.77.77.49","pgUrl":"tcp://postgres@10.77.77.49:5432/postgres","zoneId":"d0c715ab-1d55-43cd-88f2-f6bfe3960683","backupUrl":"http://10.77.77.49:12345"},"sync":{"zoneId":"4afba482-7670-4cfe-b11f-9df7f558106a","ip":"10.77.77.48","pgUrl":"tcp://postgres@10.77.77.48:5432/postgres","backupUrl":"http://10.77.77.48:12345","id":"10.77.77.48:5432:12345"},"async":[{"id":"10.77.77.47:5432:12345","zoneId":"f29499ea-b50c-431e-9975-e4bf760fb5e1","ip":"10.77.77.47","pgUrl":"tcp://postgres@10.77.77.47:5432/postgres","backupUrl":"http://10.77.77.47:12345"}],"initWal":"0/174A4D0"},"zkSeq":"0000000008"}
```

At this point your cluster is upgraded.  Notes below on two node and single-node
updates as well as rollback.

## Rollback

At any point before the primary is reprovisioned:

1. Roll back current async.
1. Roll back current sync.
1. Delete the state in zookeeper by logging onto one of the binder zones and:

```
[root@9a5bcf34 (nameservice) ~]$ zkCli.sh
Connecting to localhost:2181
Welcome to ZooKeeper!
JLine support is enabled

WATCHER::

WatchedEvent state:SyncConnected type:None path:null
[zk: localhost:2181(CONNECTED) 0] ls /manatee
[1.moray.coal.joyent.us]
[zk: localhost:2181(CONNECTED) 1] ls /manatee/1.moray.coal.joyent.us
[state, history, election]
[zk: localhost:2181(CONNECTED) 2] delete /manatee/1.moray.coal.joyent.us/state
```

After the primary is reprovisioned:

1. Take note of the current topology.
1. Disable all manatee-sitters in the shard.
1. Delete the state object (as above)
1. Roll back the primary.
1. Roll back the sync.
1. Roll back the async.

## One-node shard upgrade

1. Reprovision the primary
2. Backfill the cluster state: `primary$ manatee-adm state-backfill`
3. Restore One Node Write Mode: `primary$ manatee-adm set-onwm -m on`
4. Unfreeze Cluster State: `primary$ manatee-adm unfreeze`

## Two-node shard upgrade

1. Reprovision the sync
2. Backfill the cluster state: `sync$ manatee-adm state-backfill`
3. Reprovision the primary
