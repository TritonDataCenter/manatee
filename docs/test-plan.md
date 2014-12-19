# Sample test plan

This document covers a sample Manatee test plan.

This test plan covers a number of different cases, including:

* transient, relatively clean component failure
* adding new peers and removing existing peers
* network partitions
* hard component crashes (simulated with SIGKILL)
* hard system crashes (simulated with an unclean reboot)

Note that system crashes (e.g., OS panic) are not the same as component
crashes, particularly since TCP connections are typically cleanly terminated
when a process dies, but not when the OS panics.  If a component is offline,
connection attempts typically receive a TCP reset, while if the system is
offline, those attempts receive no response.

For all these tests, if you're doing it in the context of a Manta deployment,
you can use the [mlive](https://github.com/joyent/manta-mlive) tool to monitor
service interruptions.


## Disabling/enabling manatee sitter

These test cases exercise basic operation when individual Manatee peers are
stopped using "svcadm disable".

- Disable the primary, watch it become deposed when the sync takes over.
  Then rebuild the former primary and watch it come back as an async.  A service
  interruption is expected here.
- Repeat again.  This case tests a node moving from async to sync (in the above
  test) and then to primary.  A service interruption is expected here.
- Repeat again.  This case tests the original, formerly deposed primary coming
  around back to primary again.  A service interruption is expected here.
- Disable the sync and watch it swap positions with the async.  Bring it back
  up and verify the cluster status.  A brief service interruption is expected
  here.
- Disable the async and watch it be removed from the cluster.  Then bring it
  back and watch it rejoin the cluster as an async.  There should be no service
  interruption.

## Provisioning tests

- Deprovison async and watch it be removed from the cluster.  There should be
  service impact.
- Provision a new peer and watch it come up as an async.  There should be
  service impact.
- Provision new manatee peer (4 total now) and watch it come up as an async.
  There should be service impact.
- Perform a bunch of failovers (using "svcadm disable" on successive primaries)
  until the 4th peer is promoted to primary.  There should be three service
  interruptions.
- Deprovision 4th peer and manually remove it from deposed using "manatee-adm
  remove-deposed".  There should be a service interruption for the takeover, but
  not for the remove-deposed operation.

## Testing network partitions with ipdadm

- Primary
  - Drop traffic briefly to primary.  Before ZK expiration, let it come back.
    There should be no change to cluster state, but requests may be stalled by
    however long the primary was partitioned.
  - Drop traffic to primary until the sync takes over.  There will be a service
    interruption.
  - Remove the partition and see that the primary has marked itself deposed
    (without restarting).  Rebuild it.  There should be no service interruption.
- Sync
  - Drop traffic briefly to sync.  Before ZK expiration, let it come back.
    There should be no change to cluster state, but *write* requests may be
    stalled by however long the sync was partitioned.
  - Drop traffic to sync for a while until the primary assigns a new sync.
    There will be a service interruption.
  - Remove the partition and see the sync peer come back as an async.  No
    rebuild will be required, and there should be no service interruption.
- Async
  - Drop traffic briefly to async.  Before ZK expiration, let it come back.
    There should be no change to the cluster state or service.
  - Drop traffic for a while until the async is removed from the cluster.  There
    should be no service interruption.
  - Remove the partition and watch the peer come back as async.

## SIGKILL tests

- kill -9 (SIGKILL) the manatee sitter, see that it is restarted, it restarts
  postgres, and all is well.  There should be no change to the cluster state,
  and only a short duration during which client *write* requests take a few
  seconds.
- kill -9 (SIGKILL) postgres.  See that manatee-sitter crashes and restarts.
  (This behavior relies on smf(5) on illumos systems.)  There should be no
  change to the cluster state, and only a short duration during which client
  *write* requests take a few seconds.

## System crash tests

- Reboot a CN where the primary lives and see that it comes back as deposed.
  On illumos, we reboot the system with "uadmin 2 1", which syncs filesystems
  but does not shut down processes cleanly.  You might consider trying a chassis
  power reset as well.
- Run the same test for a system running a sync peer and make sure it comes back
  online as an async peer.
- Run the same test for a system running an async peer and make sure it comes
  back online as an async peer.

## ZooKeeper crashes

- Kill the ZooKeeper leader (using svcadm disable).  See that Manatee stays up.
  Repeat many times.
- Disable *all* ZK nodes and see that Manatee stays up.  Then enable all ZK
  nodes again.
- Disable *all* ZK nodes, disable manatee primary, bring up *two* ZK, see that
  cluster recovers and deposes the primary.
