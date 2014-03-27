BASE_DS=zones/$(zonename)/data
export PARENT_ZFS_DS=zones/$(zonename)/data/manatee_integ_tests

useradd postgres
echo "postgres    ALL=(ALL) NOPASSWD: /usr/bin/chown, /usr/bin/chmod, /opt/local/bin/chown, /opt/local/bin/chmod" >> /opt/local/etc/sudoers
zfs allow -ld postgres create,destroy,diff,hold,release,rename,setuid,rollback,share,snapshot,mount,promote,send,receive,clone,mountpoint,canmount $BASE_DS

mkdir -p /var/pg
chown postgres /var/pg

/usr/bin/ctrun -l child -o noorphan node ../node_modules/.bin/nodeunit ./integ.test.js 2>&1 | bunyan
