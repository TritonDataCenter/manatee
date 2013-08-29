#!/usr/bin/bash

export PS4='[\D{%FT%TZ}] ${BASH_SOURCE}:${LINENO}: ${FUNCNAME[0]:+${FUNCNAME[0]}(): }'
set -o xtrace

PATH=/opt/smartdc/manatee/build/node/bin:/opt/local/bin:/usr/sbin/:/usr/bin:$PATH
PROFILE=/root/.bashrc
REGISTRAR_CFG=/opt/smartdc/registrar/etc/config.json
alias mbunyan="bunyan -c \"this.component !== 'ZKPlus'\" -c 'level >= 30' -c 'this.src=undefined; true' --strict"
alias manatee_stat="/opt/smartdc/manatee/bin/manatee_stat.js -p `mdata-get nameservers`"

function update_env {
    echo "" >>$PROFILE
    echo "export PATH=\$PATH:/opt/smartdc/registrar/build/node/bin:/opt/smartdc/registrar/node_modules/.bin:/opt/smartdc/manatee/lib/tools" >>$PROFILE
}

# vars used by manatee-* tools
ZONE_UUID=$(json -f /var/tmp/metadata.json ZONE_UUID)
DATASET=zones/$ZONE_UUID/data/manatee
PARENT_DATASET=zones/$ZONE_UUID/data
DATASET_MOUNT_DIR=/manatee/pg
PG_DIR=/manatee/pg/data
PG_LOG_DIR=/var/pg
ZONE_IP=$(json -f /var/tmp/metadata.json ADMIN_IP)
SHARD=$(json -f /var/tmp/metadata.json manatee_shard)
BINDER_ADMIN_IPS=$(json -f /var/tmp/metadata.json binder_admin_ips)

# create manatee dataset
echo "creating manatee dataset"
zfs create -o canmount=noauto $DATASET

# create postgres user
echo "creating postgres user"
useradd postgres

# grant postgres user chmod chown privileges with sudo
echo "postgres    ALL=(ALL) NOPASSWD: /usr/bin/chown, /usr/bin/chmod, /opt/local/bin/chown, /opt/local/bin/chmod" >> /opt/local/etc/sudoers

# give postgres user zfs permmissions.
echo "grant postgres user zfs perms"
zfs allow -ld postgres create,destroy,diff,hold,release,rename,setuid,rollback,share,snapshot,mount,promote,send,receive,clone,mountpoint,canmount $PARENT_DATASET

# change dataset perms such that manatee can access the dataset and mount/unmount
mkdir -p $DATASET_MOUNT_DIR
chown -R postgres $DATASET_MOUNT_DIR
chmod 700 -R $DATASET_MOUNT_DIR

# set mountpoint
zfs set mountpoint=$DATASET_MOUNT_DIR $DATASET

# mount the dataset
zfs mount $DATASET

# make the pg data dir
echo "creating $PG_DIR"
mkdir -p $PG_DIR
chown postgres $PG_DIR
chmod 700 $PG_DIR

# make .zfs dir visible for snapshots
echo "make snapshot dir visible"
zfs set snapdir=visible $DATASET

# add pg log dir
mkdir -p $PG_LOG_DIR
chown -R postgres $PG_LOG_DIR
chmod 700 $PG_LOG_DIR

# update .bashrc
echo "Updating ~/.bashrc"
update_env

# Add Log rotation
echo "Adding log rotation"
tempLogConf=/tmp/`uuid`
echo "manatee-sitter -C 48 -c -p 1h /var/svc/log/sds-application-manatee-sitter:default.log" > $tempLogConf
echo "manatee-postgres -C 48 /var/pg/postgresql*.log" >> $tempLogConf
echo "manatee-snapshotter -C 48 -c -p 1h /var/svc/log/sds-application-manatee-snapshotter:default.log" >> $tempLogConf
echo "manatee-backupserver -C 48 -c -p 1h /var/svc/log/sds-application-manatee-backupserver:default.log" >> $tempLogConf
cat /etc/logadm.conf >> $tempLogConf
rm /etc/logadm.conf
mv $tempLogConf /etc/logadm.conf

# prevent local postgres core dumps. MANTA-600
ulimit -c 0

# import services
echo "Starting snapshotter"
svccfg import /opt/smartdc/manatee/smf/manifests/snapshotter.xml
svcadm enable manatee-snapshotter

echo "Starting backupserver"
svccfg import /opt/smartdc/manatee/smf/manifests/backupserver.xml
svcadm enable manatee-backupserver

svccfg import /opt/smartdc/manatee/smf/manifests/sitter.xml
disableSitter=$(json disableSitter < /opt/smartdc/manatee/etc/sitter.json)
if [[ -n ${disableSitter} && ${disableSitter} == "true" ]]; then
    # HEAD-1327 we want to be able to disable the sitter on the 2nd manatee we
    # create as part of the dance required to go from 1 -> 2+ nodes. This should
    # only ever be set for the 2nd manatee.
    echo "Disabing sitter per /opt/smartdc/manatee/etc/sitter.json"
    svcadm disable manatee-sitter
else
    echo "Starting sitter"
    svcadm enable manatee-sitter
fi

#functions
echo "mbunyan() { bunyan -c \"this.component !== 'ZKPlus'\"  -c 'level >= 30'; }" >> $PROFILE
echo "manatee-history(){ /opt/smartdc/manatee/bin/manatee-history '$SHARD' '$BINDER_ADMIN_IPS'; }" >> $PROFILE
echo "manatee-stat() { /opt/smartdc/manatee/bin/manatee-stat -p '$BINDER_ADMIN_IPS'; }" >> $PROFILE
echo "manatee-clear(){ /opt/smartdc/manatee/bin/manatee-clear '$SHARD' '$ZONE_IP' '$BINDER_ADMIN_IPS'; }" >> $PROFILE
echo "manatee-snapshots(){ /opt/smartdc/manatee/bin/manatee-snapshots '$DATASET'; }" >> $PROFILE
echo "msitter(){ tail -f \`svcs -L manatee-sitter\` | mbunyan; }" >> $PROFILE
echo "mbackupserver(){ tail -f \`svcs -L manatee-backupserver\` | mbunyan; }" >> $PROFILE
echo "msnapshotter(){ tail -f \`svcs -L manatee-snapshotter\` | mbunyan; }" >> $PROFILE
