# Determine which vnodes to move
Run this command on the primary of the to determine which vnodes need to be moved:

        sudo -u postgres psql moray -c 'select _vnode, count(*)  from manta group by _vnode order by count desc;'

This will give you a sum of keys on each vnode sorted by count.

        moray=# select _vnode, count(*)  from manta group by _vnode order by count desc;
        _vnode | count
        --------+-------
        0   |   195
        106 |    36
        943 |    28
        689 |    18
        501 |    16
        39  |    16
        428 |    15
        556 |    15
        769 |    13
        997 |    12
        402 |    12
        783 |    11
        807 |    11
        623 |    10
        116 |     7
        44  |     7
        956 |     7
        859 |     7
        59  |     6
        114 |     6
        539 |     6

In this trivial example, we are going to move shards 0, 105, 943 to a new shard.
# Create a canary file
We want to create a canary file such that we can use it to test out the re-shard process. We create a dir that resides on the shard which we want to remap.

        mmkdir -p /poseidon/stor/re-shard-canary-dir/`uuid`

Since manta hashes on directories, create the uuid dirs until you find one which maps to the shard we want to remap. You can check the location of the file vial mlocate on a muskie host:

        [root@309d10e2 (webapi) ~]$ /opt/smartdc/muskie/bin/mlocate -f /opt/smartdc/muskie/etc/config.json /poseidon/stor/re-shard-canary-dir/2178ed56-ed9b-11e2-8370-d70455cbcdc2
            {
                "dirname": "/5884dd74-949a-40ec-8d13-dd3151897343/stor/re-shard-canary-dir",
                "key": "/5884dd74-949a-40ec-8d13-dd3151897343/stor/re-shard-canary-dir/2178ed56-ed9b-11e2-8370-d70455cbcdc2",
                "headers": {},
                "mtime": 1373926131745,
                "name": "2178ed56-ed9b-11e2-8370-d70455cbcdc2",
                "owner": "5884dd74-949a-40ec-8d13-dd3151897343",
                "type": "directory",
                "_key": "/poseidon/stor/re-shard-canary-dir/2178ed56-ed9b-11e2-8370-d70455cbcdc2",
                "_moray": "tcp://electric-moray.coal.joyent.us:2020",
                "_node": {
                    "pnode": "tcp://1.moray.coal.joyent.us:2020",
                    "vnode": "689",
                    "data": null
                }
            }

Notice the vnode of 689, we add 689 to the set of vnodes we want to remap. We also create a test file under the dir:

        echo 'test' | mput /poseidon/stor/re-shard-canary-dir/2178ed56-ed9b-11e2-8370-d70455cbcdc2/test

# Setting up a new async peer
First we'll need to setup a new async peer to this shard, and we can do that from the manta-zone.

        manta-deploy -s <cn_uuid> postgres -z <current_shard>

Next we'll need to determine whether the new shard has caught up. We can check via manatee stat the pg xlog numbers on the new async, and it's master like so:

        manatee-stat | json <shard-name>

Be sure to escape any special characters such as '.' in the shard-name. You should get results back like so:

        {
            "1.moray.coal.joyent.us": {
                "primary": {
                    "ip": "10.99.99.47",
                        "pgUrl": "tcp://postgres@10.99.99.47:5432/postgres",
                        "zoneId": "eab078a6-63a9-446e-ab21-6ef57b3cdb43",
                        "repl": {
                            "pid": 59054,
                            "usesysid": 10,
                            "usename": "postgres",
                            "application_name": "tcp://postgres@10.99.99.60:5432/postgres",
                            "client_addr": "10.99.99.60",
                            "client_hostname": "",
                            "client_port": 59566,
                            "backend_start": "2013-07-15T21:01:46.620Z",
                            "state": "streaming",
                            "sent_location": "0/F65C640",
                            "write_location": "0/F65C640",
                            "flush_location": "0/F65C640",
                            "replay_location": "0/F65C578",
                            "sync_priority": 1,
                            "sync_state": "sync"
                        }
                },
                    "sync": {
                        "ip": "10.99.99.60",
                        "pgUrl": "tcp://postgres@10.99.99.60:5432/postgres",
                        "zoneId": "c3d2f8ce-9bfe-4ff1-bc84-b04b1b938e5f",
                        "repl": {
                            "pid": 53232,
                            "usesysid": 10,
                            "usename": "postgres",
                            "application_name": "tcp://postgres@10.99.99.61:5432/postgres",
                            "client_addr": "10.99.99.61",
                            "client_hostname": "",
                            "client_port": 53483,
                            "backend_start": "2013-07-15T20:52:16.549Z",
                            "state": "streaming",
                            "sent_location": "0/F65C640",
                            "write_location": "0/F65C640",
                            "flush_location": "0/F65C640",
                            "replay_location": "0/F65C578",
                            "sync_priority": 0,
                            "sync_state": "async"
                        }
                    },
                    "async": {
                        "ip": "10.99.99.61",
                        "pgUrl": "tcp://postgres@10.99.99.61:5432/postgres",
                        "zoneId": "c5030336-c1c2-4596-bff8-5f5fa00ba929",
                        "repl": {
                            "pid": 58999,
                            "usesysid": 10,
                            "usename": "postgres",
                            "application_name": "tcp://postgres@10.99.99.46:5432/postgres",
                            "client_addr": "10.99.99.46",
                            "client_hostname": "",
                            "client_port": 65317,
                            "backend_start": "2013-07-15T21:01:44.250Z",
                            "state": "streaming",
                            "sent_location": "0/F65C640",
                            "write_location": "0/F65C640",
                            "flush_location": "0/F65C640",
                            "replay_location": "0/F65C578",
                            "sync_priority": 0,
                            "sync_state": "async"
                        },
                        "lag": {
                            "time_lag": {
                                "seconds": 1
                            }
                        }
                    },
                    "async1": {
                        "ip": "10.99.99.46",
                        "pgUrl": "tcp://postgres@10.99.99.46:5432/postgres",
                        "zoneId": "1bc27c6d-ea05-4e9b-9404-748d788eefc9",
                        "repl": {},
                        "lag": {
                            "time_lag": {
                                "seconds": 1
                            }
                        }
                    },
                    "registrar": {
                        "type": "database",
                        "database": {
                            "primary": "tcp://postgres@10.99.99.47:5432/postgres",
                            "ttl": 60
                        }
                    }
            }
        }

Pay close attention to async.repl and primary.repl. The repl field indicates the replication state of the current peer's slave. In our case, we want to see if async1 has caught up, so we compare the `replay_location` on the new async1 peer to the `sent_location`. If the numbers are close, say, at least the fourth least signigificat digit matches, then this means the shard has some what caught up, and it's time to re-shard.

# Put the re-sharded nodes in RO
First, put the relevant vnodes in r/o mode on all electric-moray hosts. Check out the electric-moray repo from mo. Then, use the `node-fash` cli to set the nodes to r/o

        ~/workspace/electric-moray/node_modules/.bin/fash add_data  -f ~/workspace/electric-moray/etc/prod.ring.json -v '0 105 943 689' -d 'ro' > ~/workspace/electric-moray/etc/ro.prod.ring.json
        mv ~/workspace/electric-moray/etc/ro.prod.ring.josn ~/workspace/electric-moray/etc/prod.ring.json

git commit, push and build the new image. Run manta-init from within the manta zone to pick up the new image.

        manta-init -e manta+jpc@joyent.com -s production -c 10 -r us-east | bunyan

Reprovision electric-moray on the hn such that it picks up the new ring config.  **Note once the this step is complete, we will be returning 500s on any requests that maps to these vnodes! It's important that we minimize any downtime here. Also start the reprovision at the top of each hour to ensure that manatee dumps are not interrupted.**

        sapiadm reprovision <electric-moray-zone-uuid> <image_uuid>

Check that electric-moray is indeed in r/o mode by smoke testing a few known keys that map to the vnodes. Generally I do this by first creating a file in manta, then finding its vnode, and moving that setting that vnode to r/o as a canary.

        echo 'test' | mput /poseidon/stor/re-shard-canary-dir/2178ed56-ed9b-11e2-8370-d70455cbcdc2/test

This command should 500 since our canary vnode is in r/o.

Then check that the new shard peer has caught up for just the relevant reshard nodes by running this command on both the primary and the async1:

        sudo -u postgres psql moray -c 'select _id from manta where _vnode in (0, 105, 943, 689) order by _id  desc limit 10;'

wait until the _id returned by both queries are the same.

# Take a lock to prevent Postgresql Dumps
It's important to prevent Postgresql dumps before the next step, so we take the lock in zookeeper from any nameservice zone via:

        zkCli.sh create /pg_dump_lock foo

Check that the lock exists by

        zkCli.sh get /pg_dump_lock foo

This prevents Postgresql dumps from running while this lock exists. Both Metering and Garbage collection require that pg dumps accurately reflect the current shard topology, so while we are in this limbo state, we do not take dumps.

# Ensure manatee dumps are not currently running
Even though we've just taken the pg dump lock, we still need to ensure that there aren't any currently running dumps. So we'll need to check on the last node in all manatee shards whether dumps are running.
Get the last node in each shard with manantee_stat

        manatee-stat

log on to each node, and ensure pg_dump.sh is not running:

        ptree | grep pg_dump.sh

If dumps are running, then wait until they are finished.

# Move the new peer into the new shard

Now, we can remove the new shard peer from the old shard, and provision it in the new shard.

First disable the manatee-sitter on the new shard peer

        svcadm disable manatee-sitter

Next, update the sapi metadata for the zone to add it the new shard. In this example we are moving the new peer from shard 1 to shard 2

        sapiadm update <zone_uuid> metadata.SERVICE_NAME="2.moray.coal.joyent.us"
        sapiadm update <zone_uuid> metadata.SHARD=2
        sapiadm update <zone_uuid> metadata.MANATEE_SHARD_PATH='/manatee/2.moray.coal.joyent.us'

now get the object one more time to ensure that all params are correct

        sapiadm get <zone_uuid>

now bounce the zone

        vmadm reboot <zone_uuid>

log into the zone and check via manatee-stat that manatee is back up and running and in the new shard.

        manatee-stat

now deploy 2 more postgres zones under shard 2

        manta-deploy -s <cn_uuid> -z 2 postgres  | bunyan

        manta-deploy -s <cn_uuid> -z 2 postgres  | bunyan

Verify that the new shard is healhty via manatee-stat. Ensure you see replication state for both the primary, and async.

Finally deploy new morays for this new shard

        manta-depoy -s <cn_uuid> -z 2 moray| bunyan

# Move the relevant vnodes to the new shard, and set them to r/w
move the relevant vnodes to the new shard in the topology.

        ~/workspace/electric-moray/node_modules/.bin/fash remap_vnode -f ~/workspace/electric-moray/etc/prod.ring.json -v '0 105 943 689' -p 'tcp://2.moray.coal.joyent.us:2020' > ~/workspace/electric-moray/remapped_ring.json

now set them to write mode

        ~/workspace/electric-moray/node_modules/.bin/fash add_data -f ~/workspace/electric-moray/remapped_ring.json -v '0 105 943 689' > ~/workspace/electric-moray/etc/prod.ring.json

now check this into git, and manta-init and redeploy to all electric-morays.

# Verify changes are correct.
Make changes to a file that belongs to your canary object.

        echo 'foo' | mput /poseidon/stor/re-shard-canary-dir/2178ed56-ed9b-11e2-8370-d70455cbcdc2/test

Verify that its _id increments on the new shard and not the old one. Run this on both the primary of the old shard, and the primary of the new shard.

        sudo -u postgres psql moray -c 'select _id from manta where _key = /<user_uuid>/stor/re-shard-canary-dir/2178ed56-ed9b-11e2-8370-d70455cbcdc2/test;

Ensure that the _id from the new shard is > than the _id from the old shard.

# Upload the new shard topology to manta
On any electric-moray zone, run:

        /opt/smartdc/electric-moray/bin/upload-topology.sh

This will upload the updated topology to manta, so metering and GC can pick it up.

# Re-enable the pg dumps
Now we can re-enable the postgresql dumps by removing the lock from zk:

        zkCli.sh rmr /pg_dump_lock

We may have missed some pg_dumps if this process took longer than an hour. You can check by listing for pg dumps in poseidon:

        mls /poseidon/stor/manatee_backups/<date>

Look for any missing hours. If hours are missing, re-mount the zfs dataset and upload the dumps. **TODO** add docs about re-uploading dumps.

# Drop rows from the old DB
Now we'll want to clean up any rows in shard 1 that still maps to the re-mapped vnodes. On the primary, run:

        sudo -u postgres psql moray -c 'delete from manta where _vnode in (0, 105, 943, 689)'

Grab your favourite beverage, do a dance, and you're done!

