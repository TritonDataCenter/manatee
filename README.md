# Components
Manatee consists of 3 seperate node processes, the manatee-sitter,
manatee-snapshotter, and manatee-backupservice

The sitter is the agent that spawns the postgres process and monitors the
liveliness of other other nodes in the shard.

The backupservice is a REST server that sends the latest zfs snapshot of the
postgres data directory to clients. Generally this is used when a new node
joins the shard and needs a copy of the current postgres dataset.

The snapshotter is a process that is configured to take a zfs snapshot of the
postgres data directory periodically.

# Requirements
## Node
Manatee utilizes [Node.js](http://nodejs.org). It's been tested to work with
version 0.8.x.

## Zookeeper
Manatee utilizes [Apache Zookeeper](http://zookeeper.apache.org/). It's been
tested to work with version 3.4.3. Ensure there's a zookeeper running.

## Postgres
Manatee requires [PostgreSQL](http://www.postgresql.org/), it's been tested
with version 9.2.4 64 bit.

## ZFS
Manatee heavily utilizes [ZFS](http://wiki.illumos.org/display/illumos/ZFS).

# Running Manatee
## Configuring Postgres user
Postgres requires a non-root user for any interactions with it, thus, first
create the postgres user and give it perms.

        # useradd postgres
        # echo "postgres    ALL=(ALL) NOPASSWD: /usr/bin/chown, /usr/bin/chmod, /opt/local/bin/chown, /opt/local/bin/chmod" >> /opt/local/etc/sudoers
        # zfs allow -ld postgres create,destroy,diff,hold,release,rename,setuid,rollback,share,snapshot,mount,promote,send,receive,clone,mountpoint,canmount $PARENT_DATASET

## Starting Manatee
Run the manatee-sitter as follows, and of course edit the config file with parameters that match your system.

        #sudo -u postgres node sitter.js -v -f ./etc/manatee-sitter.json.in

Then start the snapshotter

        #sudo -u postgres node snapshotter.js -v -f ./etc/snapshotter.json.in

Finally, start the backupserver
        #sudo -u postgres node backupserver.js -v -f etc/backupserver.json.in

# Tools
Manatee comes with a set of tools to check the status of the shard. Notably
./bin/manatee-stat which prints out the topology of the shard as well as the
postgres replication stats. The output of which look like:

    [root@704e8c73 (postgres) ~]$ manatee-stat
    "1.moray.bh1.joyent.us": {
        "primary": {
            "ip": "10.2.211.97",
            "pgUrl": "tcp://postgres@10.2.211.97:5432/postgres",
            "zoneId": "546af229-85d5-407a-8b89-b460351e6585",
            "repl": {
                "pid": 85326,
                "usesysid": 10,
                "usename": "postgres",
                "application_name": "tcp://postgres@10.2.211.99:5432/postgres",
                "client_addr": "10.2.211.99",
                "client_hostname": "",
                "client_port": 60404,
                "backend_start": "2013-08-21T22:23:17.036Z",
                "state": "streaming",
                "sent_location": "5/80000000",
                "write_location": "5/80000000",
                "flush_location": "5/80000000",
                "replay_location": "5/7F0000B8",
                "sync_priority": 0,
                "sync_state": "async"
            }
        },
        "sync": {
            "ip": "10.2.211.99",
            "pgUrl": "tcp://postgres@10.2.211.99:5432/postgres",
            "zoneId": "704e8c73-43cd-48ce-9bb2-bd64e96052e1",
            "repl": {}
        },
        "registrar": {
            "type": "database",
            "database": {
                "primary": "tcp://postgres@10.2.211.97:5432/postgres",
                "ttl": 60
            }
        }


