<!--
    This Source Code Form is subject to the terms of the Mozilla Public
    License, v. 2.0. If a copy of the MPL was not distributed with this
    file, You can obtain one at http://mozilla.org/MPL/2.0/.
-->

<!--
    Copyright (c) 2014, Joyent, Inc.
-->

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](http://doctoc.herokuapp.com/)*

- [Troubleshoot a Manatee Shard in Distress.](#troubleshoot-a-manatee-shard-in-distress)
  - [Warning](#warning)
- [Manatee-adm](#manatee-adm)
- [Healthy Manatee](#healthy-manatee)
- [Symptoms](#symptoms)
  - [manatee-adm status Shows Only an Error Object.](#manatee-adm-status-shows-only-an-error-object)
  - [manatee-adm status Shows Only a Primary peer.](#manatee-adm-status-shows-only-a-primary-peer)
  - [manatee-adm status Shows No Async peer. (Only Primary and Sync are Visible)](#manatee-adm-status-shows-no-async-peer-only-primary-and-sync-are-visible)
  - [manatee-adm status Shows No peers.](#manatee-adm-status-shows-no-peers)
  - [manatee-adm status Shows No Replication Information on the Sync.](#manatee-adm-status-shows-no-replication-information-on-the-sync)
  - [Manatee Runs out of Space](#manatee-runs-out-of-space)
    - [Solution](#solution)
- [Useful Manatee Commands](#useful-manatee-commands)
  - [Find the set of manatee peers in a DC](#find-the-set-of-manatee-peers-in-a-dc)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

**Table of Contents**
# Troubleshoot a Manatee Shard in Distress.
This guide will list the most common failure scenarios of Manatee and steps to
resucscitate the Shard.

## Warning
You should read the user guide first to ensure you understand
how Manatee works. Some of the recovery steps could potentially be destructive
and result in data loss if performed incorrectly.

# Manatee-adm
The primary way an operator should be interacting with manatee is via the
`manatee-adm` CLI.

# Healthy Manatee
The output of `# manatee-adm status` of a healthy manatee shard will look like
this.
```json
{
    "sdc": {
        "primary": {
            "ip": "172.25.3.65",
            "pgUrl": "tcp://postgres@172.25.3.65:5432/postgres",
            "zoneId": "4243db81-4453-42b3-a241-f26395712d19",
            "repl": {
                "pid": 15324,
                "usesysid": 10,
                "usename": "postgres",
                "application_name": "tcp://postgres@172.25.3.16:5432/postgres",
                "client_addr": "172.25.3.16",
                "client_hostname": "",
                "client_port": 46276,
                "backend_start": "2014-07-25T19:37:52.026Z",
                "state": "streaming",
                "sent_location": "E/293522B0",
                "write_location": "E/293522B0",
                "flush_location": "E/293522B0",
                "replay_location": "E/2934F6D0",
                "sync_priority": 1,
                "sync_state": "sync"
            }
        },
        "sync": {
            "ip": "172.25.3.16",
            "pgUrl": "tcp://postgres@172.25.3.16:5432/postgres",
            "zoneId": "8372b732-007c-400c-9642-9eb63d169cf2",
            "repl": {
                "pid": 18377,
                "usesysid": 10,
                "usename": "postgres",
                "application_name": "tcp://postgres@172.25.3.59:5432/postgres",
                "client_addr": "172.25.3.59",
                "client_hostname": "",
                "client_port": 41581,
                "backend_start": "2014-07-25T19:37:53.961Z",
                "state": "streaming",
                "sent_location": "E/293522B0",
                "write_location": "E/293522B0",
                "flush_location": "E/293522B0",
                "replay_location": "E/2934F6D0",
                "sync_priority": 0,
                "sync_state": "async"
            }
        },
        "async": {
            "ip": "172.25.3.59",
            "pgUrl": "tcp://postgres@172.25.3.59:5432/postgres",
            "zoneId": "cce218f8-6ad9-45c6-bd98-d9d0b840b56a",
            "repl": {},
            "lag": {
                "time_lag": {}
            }
        }
    }
}
```
There are 3 peers in this shard, a `primary`, `sync`, and `async`. Pay close
attention to ther `repl` field of each peer. The repl field represents the
replication state of its immediate standby. For example, the `primary.repl` field
represents the replication state of the synchronous peer. A primary peer with
an empty `repl` field means replication is offline between the primary and the
sync. A sync peer with an empty `repl` field means that replication is offline
between the sync and the async.

A manatee shard is healthy only if there are 3 peers in the shard,
`primary.repl.sync_state === 'sync'`, and `sync.repl.sync_state === 'async'`

# Symptoms
Here are some commonly seen outputs of `manatee-adm status` when the shard is
unhealthy. Before you start: click [here](http://calmingmanatee.com/).

## manatee-adm status Shows Only an Error Object.
```json
    {
        "sdc": {
            "error": {
                "object": {
                    "primary": "10.99.99.39",
                    "zoneId": "9f8483e6-572f-422b-8af7-a8d3db6a7d81",
                    "standby": "10.99.99.16"
                }
            }
        }
    }
```
1. [Check that no manatee peers are out of disk
space](#manatee-runs-out-of-space).
1. Take note of the primary and standby IPs.
1. Disable manatee-sitter on all manatee-peers. `# svcadm disable manatee-sitter`
1. Run `# manatee-adm clear` on any manatee peer.
1. On the primary, enable manatee-sitter. `# svcadm enable manatee-sitter`
1. On the standby, run `# manatee-adm rebuild`.
1. [Find the async manatee peer](#find-the-set-of-manatee-peers-in-a-dc). The
async will be the peer that is not in the results of `manatee-adm status`
1. On the async, re-enable manatee-sitter. `# svcadm enable manatee-sitter`
1. Check via `manatee-adm status` that you see all 3 peers with replication
information. Depending on the output of `# manatee-adm status` at this point,
you'll want to follow the steps described in the other troubleshooting sections
of this document.

## manatee-adm status Shows Only a Primary peer.
```json
{
    "sdc": {
        "primary": {
            "ip": "10.99.99.16",
            "pgUrl": "tcp://postgres@10.99.99.16:5432/postgres",
            "zoneId": "ebe96dfb-2533-459b-964b-a5ad7fb3b566",
            "repl": {}
        }
    }
}
```
This assumes you have an HA manatee shard setup. Some installations of SDC such
as COAL by default only come with 1 manatee peer. In those cases, this is the
expected output of `# manatee-adm status`.

1. [Check that no manatee peers are out of disk
space](#manatee-runs-out-of-space).
1. [Find the set of manatee peers in the
DC](#find-the-set-of-manatee-peers-in-a-dc). If this returns only 1 peer, then
you do not have an HA manatee setup.
1. Log on to a non primary peer.
1. Restart the manatee-sitter process. `# svcadm disable manatee-sitter; svcadm enable manatee-sitter`
1. Check via `# manatee-adm status` that you see 2 peers with replication information.
If this peer is unable replicate e.g. the sitter service goes back into
maintenance, postgres dumps core, or you still don't see replication state,
Then you'll need to rebuild this peer via `# manatee-adm rebuild`
1. Continue to [here](#manatee-adm-status-shows-no-async-peer-only-primary-and-sync-are-visible)

## manatee-adm status Shows No Async peer. (Only Primary and Sync are Visible)
```json
{
    "sdc": {
        "primary": {
            "ip": "10.1.0.140",
            "pgUrl": "tcp://postgres@10.1.0.140:5432/postgres",
            "zoneId": "45d023a7-116e-44c9-ae4e-28c7b10202ce",
            "repl": {
                "pid": 97110,
                "usesysid": 10,
                "usename": "postgres",
                "application_name": "tcp://postgres@10.1.0.144:5432/postgres",
                "client_addr": "10.1.0.144",
                "client_hostname": "",
                "client_port": 57443,
                "backend_start": "2014-07-24T17:49:05.149Z",
                "state": "streaming",
                "sent_location": "F2/B4489EB8",
                "write_location": "F2/B4489EB8",
                "flush_location": "F2/B4489EB8",
                "replay_location": "F2/B4488B78",
                "sync_priority": 1,
                "sync_state": "sync"
            }
        },
        "sync": {
            "ip": "10.1.0.144",
            "pgUrl": "tcp://postgres@10.1.0.144:5432/postgres",
            "zoneId": "72e76247-7e13-40e2-8c44-6f47a2b37829",
            "repl": {}
        }
    }
}
```

1. [Check that no manatee peers are out of disk
space](#manatee-runs-out-of-space).
1. [Find the set of manatee peers in the DC](#find-the-set-of-manatee-peers-in-a-dc) and verify that an async peer exists.
You should see 3 instances -- the missing async is the instance that is missing from `# manatee-adm status`
1. Log on to the async peer.
1. Restart the manatee-sitter process. `# svcadm disable manatee-sitter; svcadm enable manatee-sitter`
1. Check via `# manatee-adm status` that you see 3 peers with replication information.
If this peer is unable replicate e.g. the sitter service goes back into
maintenance, postgres dumps core, or you still don't see replication state,
Then you'll need to rebuild this peer via `# manatee-adm rebuild`

## manatee-adm status Shows No peers.
```json
{
    "sdc": {}
}
```

1. [Check that no manatee peers are out of disk
space](#manatee-runs-out-of-space).
1. [Find the set of manatee peers in the DC](#find-the-set-of-manatee-peers-in-a-dc).
If this returns nothing, then no manatee peers are deployed.
1. Log on to any manatee peer.
1. Check for the most recent primary of the shard.
```
[root@8372b732-007c-400c-9642-9eb63d169cf2 (staging-1:manatee0) ~]# manatee-history  | grep AssumeLeader | tail -1
{"time":"1406317072941","date":"2014-07-25T19:37:52.941Z","ip":"172.25.3.65:5432","action":"AssumeLeader","role":"Leader","master":"","slave":"172.25.3.16:5432","zkSeq":"0000000219
```
The `ip` field is the the IP address of the primary peer.

1. Log on to the mantaee zone identified by the IP address.
1. Promote the zone to primary. `# manatee-primary`.
1. Check `# manatee-adm status` and ensure you see the primary field and that
primary.zoneId corresponds to this zone. e.g.
```json
{
    "sdc": {
        "primary": {
            "ip": "10.99.99.16",
            "pgUrl": "tcp://postgres@10.99.99.16:5432/postgres",
            "zoneId": "ebe96dfb-2533-459b-964b-a5ad7fb3b566",
            "repl": {}
        }
    }
}
```
Don't be alarmed by the empty repl field, since there are no other peers, the
field should be empty. Often at this point though, the other peers will have
recovered on their own.

Depending on the output of `# manatee-adm status` at this point, you'll want to follow
the steps described in the other troubleshooting sections of this document.


## manatee-adm status Shows No Replication Information on the Sync.
```
{
    "sdc": {
        "primary": {
            "ip": "10.1.0.140",
            "pgUrl": "tcp://postgres@10.1.0.140:5432/postgres",
            "zoneId": "45d023a7-116e-44c9-ae4e-28c7b10202ce",
            "repl": {
                "pid": 97110,
                "usesysid": 10,
                "usename": "postgres",
                "application_name": "tcp://postgres@10.1.0.144:5432/postgres",
                "client_addr": "10.1.0.144",
                "client_hostname": "",
                "client_port": 57443,
                "backend_start": "2014-07-24T17:49:05.149Z",
                "state": "streaming",
                "sent_location": "F2/B4489EB8",
                "write_location": "F2/B4489EB8",
                "flush_location": "F2/B4489EB8",
                "replay_location": "F2/B4488B78",
                "sync_priority": 1,
                "sync_state": "sync"
            }
        },
        "sync": {
            "ip": "10.1.0.144",
            "pgUrl": "tcp://postgres@10.1.0.144:5432/postgres",
            "zoneId": "72e76247-7e13-40e2-8c44-6f47a2b37829",
            "repl": {}
        },
        "async": {
            "ip": "10.1.0.139",
            "pgUrl": "tcp://postgres@10.1.0.139:5432/postgres",
            "zoneId": "852a2e33-93f0-4052-b2e4-c61ea6c8b0fd",
            "repl": {},
            "lag": {
                "time_lag": null
            }
        }
    }
}
```
1. [Check that no manatee peers are out of disk
space](#manatee-runs-out-of-space).
1. Log on to the async.
1. Rebuild via `# manatee-adm rebuild`
1. Depending on the output of `# manatee-adm status` at this point, you'll want to
follow the steps described in the other troubleshooting sections of this
document.

## Manatee Runs out of Space
Another really common scenario we've seen in the past is where the manatee
zones runs out of space.

The symptoms are usually:

* You can't login to the zone at all.
>```
>[Connected to zone 'e3ab01c5-d5d0-423c-97de-2c71183302d9' pts/2]
>No utmpx entry. You must exec "login" from the lowest level "shell".
>
>[Connection to zone 'e3ab01c5-d5d0-423c-97de-2c71183302d9' pts/2 closed]
>```

* Manatee logs show errors with `ENOSPC`
>```
>[2014-05-16T14:51:32.957Z] TRACE: manatee-sitter/ZKPlus/10377 on 5878969f-de0f-4538-b883-bdf416888136 (/opt/smartdc/manatee/node_modules/manatee/node_modules/zkplus/lib/client.js:361): mkdirp: completed
>Uncaught Error: ENOSPC, no space left on device
>
>FROM
>Object.fs.writeSync (fs.js:528:18)
>SyncWriteStream.write (fs.js:1740:6)
>Logger._emit (/opt/smartdc/manatee/node_modules/manatee/node_modules/bunyan/lib/bunyan.js:742:22)
>Logger.debug (/opt/smartdc/manatee/node_modu
>```

You can check that the zone is indeed out of memory by checking the space left
on the zone's ZFS dataset in the GZ. Here we assume the manatee zone's uuid is
`e3ab01c5-d5d0-423c-97de-2c71183302d9`
```
[root@headnode (emy-10) ~]# zfs list | grep e3ab01c5-d5d0-423c-97de-2c71183302d9
zones/cores/e3ab01c5-d5d0-423c-97de-2c71183302d9                 138M  99.9G   138M  /zones/e3ab01c5-d5d0-423c-97de-2c71183302d9/cores
zones/e3ab01c5-d5d0-423c-97de-2c71183302d9                       302M      0   986M  /zones/e3ab01c5-d5d0-423c-97de-2c71183302d9
zones/e3ab01c5-d5d0-423c-97de-2c71183302d9/data                 66.5M      0    31K  /zones/e3ab01c5-d5d0-423c-97de-2c71183302d9/data
zones/e3ab01c5-d5d0-423c-97de-2c71183302d9/data/manatee         66.5M      0  42.0M  /manatee/pg
```
Notice that the zone's delegated dataset has no more space.

### Solution

1. Increase the zone's ZFS dataset. In the GZ, run:
>```
># zfs set quota=1G zones/e3ab01c5-d5d0-423c-97de-2c71183302d9
>```
>Verify there's free space in the dataset.
>```
>[root@headnode (emy-10) ~]# zfs list | grep e3ab01c5-d5d0-423c-97de-2c71183302d9
>zones/cores/e3ab01c5-d5d0-423c-97de-2c71183302d9                 138M  99.9G   138M  /zones/e3ab01c5-d5d0-423c-97de-2c71183302d9/cores
>zones/e3ab01c5-d5d0-423c-97de-2c71183302d9                       302M   722M   986M  /zones/e3ab01c5-d5d0-423c-97de-2c71183302d9
>zones/e3ab01c5-d5d0-423c-97de-2c71183302d9/data                 66.5M   722M    31K  /zones/e3ab01c5-d5d0-423c-97de-2c71183302d9/data
>zones/e3ab01c5-d5d0-423c-97de-2c71183302d9/data/manatee         66.5M   722M  42.0M  /manatee/pg
>```
>You should be able to login to the zone at this point.

1. Update the manatee service's quota in SAPI. Once you've made the local
changes to the ZFS quota, you'll want to update the manatee service in SAPI
such that future manatee zones will be deployed with the quota you've set. You
can do this on the HN via sapiadm. In this example the quota size is set to
500G.
>```
># sapiadm update $(sdc-sapi services?name=manatee | json -Ha uuid)
>params.quota=500
>```

# Useful Manatee Commands
## Find the set of manatee peers in an SDC deployment
This lists the manatee peers in each DC by `zone_uuid, alias, CN_uuid, zone_ip`.
From the headnode, run:
```
[root@headnode (staging-1) ~]# sdc-sapi /instances?service_uuid=$(sdc-sapi /services?name=manatee | json -Ha uuid) | json -Ha uuid params.alias params.server_uuid metadata.PRIMARY_IP
4243db81-4453-42b3-a241-f26395712d19 manatee2 445aab6c-3048-11e3-9816-002590c3f3bc 172.25.3.65
8372b732-007c-400c-9642-9eb63d169cf2 manatee0 cf4414d0-3047-11e3-8545-002590c3f2d4 172.25.3.16
cce218f8-6ad9-45c6-bd98-d9d0b840b56a manatee1 aac3c402-3047-11e3-b451-002590c57864 172.25.3.59
```
## Find the set of manatee peers in a Manta deployment
This lists the manatee peers in each DC in Manta.  If you have a multi-dc
deployment, this will not find x-dc peers.  From the headnode, run:
```
[root@headnode (staging-1) ~]# manta-adm show postgres
SERVICE        SH ZONENAME                             GZ ADMIN IP
postgres        1 70d44638-f4fb-4cbd-8611-0f7c83d8502f 172.25.3.38
postgres        2 a5223321-600b-43eb-b66d-ebc0ab500046 172.25.3.38
postgres        3 ef318383-c5fb-4381-8416-a1636d8efa95 172.25.3.38
```
