# Postgres Transaction Log Divergence

Problems arise in Manatee clusters when Postgres peers have transaction log
divergence.  Here are a few known ways to cause transaction log (xlog)
divergence:

1. If the primary dies mid-transaction.  This is outlined in the first section
   below.
2. Secondary (or any slave) is started in non-recovery mode.  This is the
   easiest way to cause divergence and specific commands to cause this
   divergence is below.  This is because Postgres writes to the xlog on startup.
3. Primary is shut down cleanly, writes a shutdown checkpoint to xlogs.  If the
   sync is promoted to primary, the deposed primary cannot reenter the
   replication chain (the sync never received the shutdown checkpoint).  For
   this reason manatee *never* lets postgres shut down cleanly.

# Primary dies mid-transaction

This is perhaps the most subtle of the known ways logs can diverge.  For some
background, synchronous replicated writes follow this pattern:

1. Primary writes transaction to log
2. Log is streamed to sync
3. Sync writes transaction log
4. Sync acks to primary
5. Primary commits transaction

If the primary dies while Postgres hasn't finished flushing transactions from
the primary to the sync, and the sync is promoted to primary, xlogs have
diverged and the primary can't rejoin the replication chain without a rebuild.
In the above example, it would be a reconfiguration during step 2.

# Any Postgres is started in non-recovery mode

tl;dr: This causes divergence:
```
sync$ mv /manatee/pg/data/recovery.conf /var/tmp/. && \
      sudo -u postgres /opt/local/bin/postgres -D /manatee/pg/data
```

It is assumed that the reader is familiar with Manatee administration commands.
The initial setup is a two node primary/sync deployment.  First note that
replication is set up between them and that writes to primary are replicated to
the sync.

```
# Sync
[root@0100a2cb (postgres) ~]$ psql -c 'select pg_last_xlog_receive_location();'
 pg_last_xlog_receive_location
-------------------------------
 0/474BC30
(1 row)
# Primary
[root@3a6593ef (postgres) ~]$ psql -c 'select pg_current_xlog_location();'
 pg_current_xlog_location
--------------------------
 0/474C040
(1 row)
# Stat
[root@0100a2cb (postgres) ~]$ manatee-stat | json
{
  "1.moray.coal.joyent.us": {
    "primary": {
      "zoneId": "3a6593ef-9496-4799-b6ad-1015a8b47e2d",
      "ip": "10.77.77.21",
      "pgUrl": "tcp://postgres@10.77.77.21:5432/postgres",
      "repl": {
        "pid": 82354,
        "usesysid": 10,
        "usename": "postgres",
        "application_name": "tcp://postgres@10.77.77.26:5432/postgres",
        "client_addr": "10.77.77.26",
        "client_hostname": "",
        "client_port": 61917,
        "backend_start": "2014-10-31T19:23:22.839Z",
        "state": "streaming",
        "sent_location": "0/474C428",
        "write_location": "0/474C428",
        "flush_location": "0/474C428",
        "replay_location": "0/474C040",
        "sync_priority": 1,
        "sync_state": "sync"
      }
    },
    "sync": {
      "zoneId": "0100a2cb-9867-43bb-bb6b-c3dd573f589d",
      "ip": "10.77.77.26",
      "pgUrl": "tcp://postgres@10.77.77.26:5432/postgres",
      "repl": {}
    }
  }
}
```

The above were run in the order presented.  Note that all the positions are
increasing between commands:
```
0/474BC30 #Sync    (0100a2cb)
0/474C040 #Primary (3a6593ef)
0/474C428 #Primary sent location in Manatee stat
```

Run those commands again in order and you should see them increasing.  Now shut
down the Manatees, first the sync, then the primary:
```
[root@0100a2cb (postgres) ~]$ svcadm disable manatee-sitter
[root@3a6593ef (postgres) ~]$ svcadm disable manatee-sitter
```

Now we can use some commands to start postgres "manually" and query the xlog
positions:
```
# Primary
[root@3a6593ef (postgres) ~]$ sudo -u postgres pg_ctl start -D /manatee/pg/data -w; psql -c 'select pg_current_xlog_location();'; sudo -u postgres pg_ctl stop -D /manatee/pg/data -w
pg_ctl: another server might be running; trying to start server anyway
waiting for server to start.... done
server started
 pg_current_xlog_location
--------------------------
 0/4761B68
(1 row)

waiting for server to shut down.... done
server stopped
# Sync
[root@0100a2cb (postgres) ~]$ sudo -u postgres pg_ctl start -D /manatee/pg/data -w; psql -c 'select pg_last_xlog_replay_location(), pg_last_xlog_receive_location();'; sudo -u postgres pg_ctl stop -D /manatee/pg/data -w
waiting for server to start.... done
server started
 pg_last_xlog_replay_location | pg_last_xlog_receive_location
------------------------------+-------------------------------
 0/4761720                    | 0/4000000
(1 row)

waiting for server to shut down.... done
server stopped
```

Note the different commands used for the primary and sync.  Also note what
happens when you look for one on the other:
```
# Primary
[root@3a6593ef (postgres) ~]$ sudo -u postgres pg_ctl start -D /manatee/pg/data -w; psql -c 'select pg_last_xlog_replay_location(), pg_last_xlog_receive_location();'; sudo -u postgres pg_ctl stop -D /manatee/pg/data -w
waiting for server to start.... done
server started
 pg_last_xlog_replay_location | pg_last_xlog_receive_location
------------------------------+-------------------------------
                              |
(1 row)

waiting for server to shut down.... done
server stopped
# Sync
[root@0100a2cb (postgres) ~]$ sudo -u postgres pg_ctl start -D /manatee/pg/data -w; psql -c 'select pg_current_xlog_location();'; sudo -u postgres pg_ctl stop -D /manatee/pg/data -w
waiting for server to start.... done
server started
ERROR:  recovery is in progress
HINT:  WAL control functions cannot be executed during recovery.
waiting for server to shut down.... done
server stopped
```

Also note that running the primary command multiple times on the primary *increases the xlog*:
```
[root@3a6593ef (postgres) ~]$ sudo -u postgres pg_ctl start -D /manatee/pg/data -w; psql -c 'select pg_current_xlog_location();'; sudo -u postgres pg_ctl stop -D /manatee/pg/data -w
waiting for server to start.... done
server started
 pg_current_xlog_location
--------------------------
 0/4761C28
(1 row)

waiting for server to shut down.... done
server stopped
[root@3a6593ef (postgres) ~]$ sudo -u postgres pg_ctl start -D /manatee/pg/data -w; psql -c 'select pg_current_xlog_location();'; sudo -u postgres pg_ctl stop -D /manatee/pg/data -w
waiting for server to start.... done
server started
 pg_current_xlog_location
--------------------------
 0/4761C88
(1 row)

waiting for server to shut down.... done
server stopped
```

This does *not* happen on the sync:
```
[root@0100a2cb (postgres) ~]$ sudo -u postgres pg_ctl start -D /manatee/pg/data -w; psql -c 'select pg_last_xlog_replay_location(), pg_last_xlog_receive_location();'; sudo -u postgres pg_ctl stop -D /manatee/pg/data -w
waiting for server to start.... done
server started
 pg_last_xlog_replay_location | pg_last_xlog_receive_location
------------------------------+-------------------------------
 0/4761720                    | 0/4000000
(1 row)

waiting for server to shut down.... done
server stopped
[root@0100a2cb (postgres) ~]$ sudo -u postgres pg_ctl start -D /manatee/pg/data -w; psql -c 'select pg_last_xlog_replay_location(), pg_last_xlog_receive_location();'; sudo -u postgres pg_ctl stop -D /manatee/pg/data -w
waiting for server to start.... done
server started
 pg_last_xlog_replay_location | pg_last_xlog_receive_location
------------------------------+-------------------------------
 0/4761720                    | 0/4000000
(1 row)

waiting for server to shut down.... done
server stopped
```

So noticing that the xlog is written to when a postgres is started as a master,
all we need to do to make the logs diverge on the sync is to *start it up as
a primary*.  It's as simple as moving the recovery config file and starting up
postgres manually:
```
[root@0100a2cb (postgres) ~]$ mv /manatee/pg/data/recovery.conf /var/tmp/.
[root@0100a2cb (postgres) ~]$ sudo -u postgres pg_ctl start -D /manatee/pg/data -w; psql -c 'select pg_current_xlog_location();'; sudo -u postgres pg_ctl stop -D /manatee/pg/data -w
waiting for server to start.... done
server started
 pg_current_xlog_location
--------------------------
 0/4761780
(1 row)

waiting for server to shut down.... done
server stopped
```

See that the new xlog advanced with just that one command:
```
0/4761720 # As sync
0/4761780 # First startup without recovery.conf
```

Also note that moving the recover.conf back in place and running it as sync again,
the logs are still diverged:
```
[root@0100a2cb (postgres) ~]$ mv /var/tmp/recovery.conf /manatee/pg/data/.
[root@0100a2cb (postgres) ~]$ sudo -u postgres pg_ctl start -D /manatee/pg/data -w; psql -c 'select pg_last_xlog_replay_location(), pg_last_xlog_receive_location();'; sudo -u postgres pg_ctl stop -D /manatee/pg/data -w
waiting for server to start.... done
server started
 pg_last_xlog_replay_location | pg_last_xlog_receive_location
------------------------------+-------------------------------
 0/4761840                    | 0/4000000
(1 row)

waiting for server to shut down.... done
server stopped
```

And now, no matter which way you start up Manatee replication will always fail:
```
# Enable primary, then sync
[root@0100a2cb (postgres) ~]$ manatee-stat | json
{
  "1.moray.coal.joyent.us": {
    "primary": {
      "zoneId": "3a6593ef-9496-4799-b6ad-1015a8b47e2d",
      "ip": "10.77.77.21",
      "pgUrl": "tcp://postgres@10.77.77.21:5432/postgres",
      "repl": {}
    },
    "sync": {
      "zoneId": "0100a2cb-9867-43bb-bb6b-c3dd573f589d",
      "ip": "10.77.77.26",
      "pgUrl": "tcp://postgres@10.77.77.26:5432/postgres",
      "repl": {}
    }
  }
}
# Enable sync, then primary
[root@0100a2cb (postgres) ~]$ manatee-stat | json
{
  "1.moray.coal.joyent.us": {
    "primary": {
      "zoneId": "3a6593ef-9496-4799-b6ad-1015a8b47e2d",
      "ip": "10.77.77.21",
      "pgUrl": "tcp://postgres@10.77.77.21:5432/postgres",
      "repl": {}
    },
    "sync": {
      "zoneId": "0100a2cb-9867-43bb-bb6b-c3dd573f589d",
      "ip": "10.77.77.26",
      "pgUrl": "tcp://postgres@10.77.77.26:5432/postgres",
      "error": "{\"name\":\"error\",\"length\":97,\"severity\":\"FATAL\",\"code\":\"57P03\",\"file\":\"postmaster.c\",\"line\":\"1764\",\"routine\":\"ProcessStartupPacket\"}"
    }
  }
}
```

We have two postgres instances that are diverged.  One will have to be rebuilt
to get the cluster back online.
