TODO: add docs about acquiring locks when that work has been done.
Run this command on the primary to determine which vnodes need to be moved:

select _vnode, count(*)  from manta group by _vnode order by count desc;

Then, on the new shard and the current primary, run:

select _id from manta order by _id desc limit 10;

to see if the new shard has some what caught up. If the _ids are close, then it's time to begin reshard.

First, put  the relevant vnodes in r/o mode on all electric-moray hosts.

/opt/smartdc/electric-moray/node_modules/.bin/fash add_data  -f /opt/smartdc/electric-moray/etc/ring.json -v '712 41 890' -d 'ro' > /var/tmp/ro_ring.json

check the new ring into electric-moray:/etc/prod.ring.json

Run manta-init to pick up the new image.

reprovision electric-moray  such that it picks up the new ring config.

Check that electric-moray is indeed in r/o mode by smoke testing a few known keys that map to the vnodes. Generally I do this by first creating a file in manta, then finding its vnode, and moving that settting that vnode to r/o as a canary.

Then check that the new shard peer has caught up for just the relevant reshard nodes by running this command on both the primary and the new shard peer:

select _id from manta where _vnode = 712 or _vnode = 41 or _vnode = 890 order by _id  desc limit 10;

Now, we can remove the new shard peer from the old shard, and provision it in the new shard.

First disable the manatee-sitter on the new shard peer

svcadm disable manatee-sitter

Next, update the sapi metadata for the zone to add it the new shard

sapiadm update 9fbcfe30-f3fa-45ac-9d63-bbd52bc2f3ec metadata.SERVICE_NAME="2.moray.coal.joyent.us"
sapiadm update 9fbcfe30-f3fa-45ac-9d63-bbd52bc2f3ec metadata.SHARD=2
sapiadm update 9fbcfe30-f3fa-45ac-9d63-bbd52bc2f3ec metadata.MANATEE_SHARD_PATH='/manatee/2.moray.coal.joyent.us'

now get the object one more time to ensure that all params are correct
sapiadm get 9fbcfe30-f3fa-45ac-9d63-bbd52bc2f3ec

now bounce the zone
vmadm reboot 9fbcfe30-f3fa-45ac-9d63-bbd52bc2f3ec

log into the zone and check via manatee-stat that manatee is back up and running.

manatee-stat

now deploy 2 more postgres zones under shard 2

manta-deploy -s 564ddd86-5b00-c91f-937d-7fef94a580d5 -z 2 postgres  | bunyan

manta-deploy -s 564ddd86-5b00-c91f-937d-7fef94a580d5 -z 2 postgres  | bunyan

also deploy new morays for this new shard
manta-depoy -s UUID -z 2 moray| bunyan

now move the relevant vnodes to the new shard in the topology.

/opt/smartdc/electric-moray/node_modules/.bin/fash remap_vnode -f /opt/smartdc/electric-moray/etc/ring.json -v '712 41 890' -p 'tcp://2.moray.coal.joyent.us:2020' > /var/tmp/remapped_ring.json

now set them to write mode

/opt/smartdc/electric-moray/node_modules/.bin/fash add_data -f /var/tmp/remapped_ring.json -v '712 41 890' > /opt/smartdc/electric-moray/etc/ring.json

now check this into git, and manta-init and redeploy to all electric-morays.

now make changes to a file that belongs to your canary vnode, and verify that its _id increments on the new shard and not the old one.
