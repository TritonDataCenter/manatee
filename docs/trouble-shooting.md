# Troubleshoot a Manatee Shard in Distress.

This guide will list the most common failure scenarios of Manatee and steps to
resucscitate the Shard. *BIG FAT WARNING* You should read the user guide first
to ensure you understand how Manatee works. Some of the recovery steps could
potentially be destructive and result in data loss if performed incorrectly.

# Healthy Manatee
The output of `manatee-stat` of a health manatee shard will look like this.
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
        },
        "registrar": {
            "type": "database",
            "database": {
                "primary": "tcp://postgres@172.25.3.65:5432/postgres",
                "ttl": 60
            }
        }
    }
}
```
There are 3 peers in this shard, a `primary`, `sync`, and `async`. Pay close
attention to ther `repl` field of each node. The repl field of each node
represents the replication state of its immediate standby. For example, the
primary.repl field represents the replication state of the synchronous peer.

A manatee shard is healthy only if there are 3 peers in the shard,
`primary.sync.sync_state=sync`, and `sync.repl.sync_state=async`

# Symptoms
Here are some commonly seen outputs of `manatee-stat` where the shard is
unhealthy.

## `manatee-stat` Shows Only an Error Object.
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
1. Take note of the primary and standby IPs.
1. Disable manatee-sitter on all manatee-nodes. `svcadm disable manatee-sitter`
1. Run `manatee-clear` on a manatee node.
1. Enable manatee-sitter on the primary.
1. On the standby, run `manatee-rebuild`. Wait for this step to finish.
1. On the async, re-enable manatee-sitter.
1. Check via manatee-stat that you see all 3 peers with replication information.

## `manatee-stat` Shows Only a Primary Node.
```json
{
    "sdc": {
        "primary": {
            "ip": "10.99.99.16",
            "pgUrl": "tcp://postgres@10.99.99.16:5432/postgres",
            "zoneId": "ebe96dfb-2533-459b-964b-a5ad7fb3b566",
            "repl": {}
        },
        "registrar": {
            "type": "database",
            "database": {
                "primary": "tcp://postgres@10.99.99.16:5432/postgres",
                "ttl": 60
            }
        }
    }
}
```
This assumes you have an HA manatee shard setup. Some installations of SDC such
as COAL by default only come with 1 manatee node. In those cases, this is the
expected output of `manatee-stat`.
1. Get the set of manatee peers.
```bash
[root@headnode (staging-1) ~]# sdc-sapi /instances?service_uuid=$(sdc-sapi /services?name=manatee | json -Ha uuid) | json -Ha uuid params.alias params.server_uuid metadata.PRIMARY_IP
4243db81-4453-42b3-a241-f26395712d19 manatee2 445aab6c-3048-11e3-9816-002590c3f3bc 172.25.3.65
8372b732-007c-400c-9642-9eb63d169cf2 manatee0 cf4414d0-3047-11e3-8545-002590c3f2d4 172.25.3.16
cce218f8-6ad9-45c6-bd98-d9d0b840b56a manatee1 aac3c402-3047-11e3-b451-002590c57864 172.25.3.59
```
1. Log on to a non primary peer.
1. Restart the manatee-sitter process. `svcadm disable manatee-sitter; svcadm enable manatee-sitter`
1. Check via `manatee-stat` that you see 2 peers with replication information.
If this peer is unable replicate e.g. the sitter service goes back into
maintenance, postgres dumps core, or you still don't see replication stater,
Then [rebuild this peer](#rebuild-manatee-peer). Continue when the rebuild is
finished.
1. Log on to the othe non primary peer.
1. Restart the manatee-sitter process. `svcadm disable manatee-sitter; svcadm enable manatee-sitter`
1. Check via `manatee-stat` that you see 3 peers with replication information.
If this peer is unable replicate e.g. the sitter service goes back into
maintenance, postgres dumps core, or you still don't see replication stater,
Then [rebuild this peer](#rebuild-manatee-peer).

## `manatee-stat` Shows No Async Node. (Only Primary and Sync is Visible)
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
        },
        "registrar": {
            "type": "database",
            "database": {
                "primary": "tcp://postgres@10.1.0.140:5432/postgres",
                "ttl": 60
            }
        }
    }
}
```
1. Find and verify an async peer exists.
```bash
[root@headnode (staging-1) ~]# sdc-sapi /instances?service_uuid=$(sdc-sapi /services?name=manatee | json -Ha uuid) | json -Ha uuid params.alias params.server_uuid metadata.PRIMARY_IP
4243db81-4453-42b3-a241-f26395712d19 manatee2 445aab6c-3048-11e3-9816-002590c3f3bc 172.25.3.65
8372b732-007c-400c-9642-9eb63d169cf2 manatee0 cf4414d0-3047-11e3-8545-002590c3f2d4 172.25.3.16
cce218f8-6ad9-45c6-bd98-d9d0b840b56a manatee1 aac3c402-3047-11e3-b451-002590c57864 172.25.3.59
```
You should see 3 instances -- the missing async is the instance that is missing from `manatee-stat`
1. Log on to the async peer.
1. Restart the manatee-sitter process. `svcadm disable manatee-sitter; svcadm enable manatee-sitter`
1. Check via `manatee-stat` that you see 3 peers with replication information.
If this peer is unable replicate e.g. the sitter service goes back into
maintenance, postgres dumps core, or you still don't see replication state,
Then [rebuild this peer](#rebuild-manatee-peer).

## `manatee-stat` Shows No Nodes.
```
{
    "sdc": {}
}
```
## `manatee-stat` Shows No Replication Information on the Sync.
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
        },
        "registrar": {
            "type": "database",
            "database": {
                "primary": "tcp://postgres@10.1.0.140:5432/postgres",
                "ttl": 60
            }
        }
    }
}
```
1. [Rebuild the async](#rebuild-manatee-peer)

## PostgreSQL Does Not Start or is Unable to Replicate from the Primary.

The usual symptoms of this is the lack of a

Sometimes the Postgres DB on a peer will become corrupted or stale. This re

## Manatee Runs out of Space
# Operational Procedures

## Rebuild Manatee Peer

