#!/usr/bin/bash
# -*- mode: shell-script; fill-column: 80; -*-

set -o xtrace

SOURCE="${BASH_SOURCE[0]}"
if [[ -h $SOURCE ]]; then
    SOURCE="$(readlink "$SOURCE")"
fi
DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
PROFILE=/root/.bashrc
SVC_ROOT=/opt/smartdc/manatee

source ${DIR}/scripts/util.sh
source ${DIR}/scripts/services.sh


export PATH=$SVC_ROOT/build/node/bin:/opt/local/bin:/usr/sbin/:/usr/bin:$PATH
echo "alias mbunyan=\"bunyan -c \"this.component !== 'ZKPlus'\"  -c 'level >= 30'\""
echo "alias manatee_stat=\"/opt/smartdc/manatee/bin/manatee_stat.js -p \`mdata-get nameservers\`\"" >> $PROFILE


function manta_setup_manatee_env {
    mkdir -p /var/log/manatee

    #.bashrc
    echo "export PATH=\$PATH:/opt/smartdc/manatee/lib/tools:/opt/smartdc/manatee/lib/pg_dump/" >>/root/.bashrc
    echo "alias psql='sudo -u postgres psql'" >>/root/.bashrc

    #cron
    local crontab=/tmp/.manta_manatee_cron
    crontab -l > $crontab

    echo "0 * * * * /opt/smartdc/manatee/lib/pg_dump/pg_dump.sh >> /var/log/manatee/pgdump.log 2>&1" >> $crontab
    [[ $? -eq 0 ]] || fatal "Unable to write to $crontab"
    crontab $crontab
    [[ $? -eq 0 ]] || fatal "Unable import crons"

    manta_add_logadm_entry "manatee-sitter"
    manta_add_logadm_entry "manatee-backupserver"
    manta_add_logadm_entry "manatee-snapshotter"
    manta_add_logadm_entry "postgresql" "/var/pg"
    manta_add_logadm_entry "pgdump" "/var/log/manatee"
}


# Mainline

# XXX See MANTA-1613.  These manifests are shipped for SDC but aren't relevant
# for the manta image, so remove them until the situation with SDC/manta
# manifests is resolved.
rm -rf $SVC_ROOT/sapi_manifests/registrar
rm -rf $SVC_ROOT/sapi_manifests/sitter

echo "Running common setup scripts"
manta_common_presetup

echo "Adding local manifest directories"
manta_add_manifest_dir "/opt/smartdc/manatee"

manta_common_setup "manatee"
manta_setup_manatee_env

ZONE_UUID=`zoneadm list -p | cut -d ':' -f2`
DATASET=zones/$ZONE_UUID/data/manatee
PARENT_DATASET=zones/$ZONE_UUID/data
DATASET_MOUNT_DIR=/manatee/pg
PG_DIR=/manatee/pg/data
PG_LOG_DIR=/var/pg
ZONE_IP=`ifconfig net0 | nawk '{if ($1 == "inet") print $2}'`
SITTER_CFG_FILE=/opt/smartdc/manatee/etc/sitter.json
SNAPSHOT_CFG_FILE=/opt/smartdc/manatee/etc/snapshotter.json
BACKUP_CFG_FILE=/opt/smartdc/manatee/etc/backupserver.json

# create dataset
echo "create dataset $DATASET"
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

# ZK configs
ZK_TIMEOUT=30000

# XXX I think I actually want this
manta_ensure_zk

# prevent local postgres core dumps. MANTA-600
ulimit -c 0

# import services
echo "Starting snapshotter"
svccfg import /opt/smartdc/manatee/smf/manifests/snapshotter.xml
svcadm enable manatee-snapshotter

echo "Starting backupserver"
svccfg import /opt/smartdc/manatee/smf/manifests/backupserver.xml
svcadm enable manatee-backupserver

echo "Starting sitter"
svccfg import /opt/smartdc/manatee/smf/manifests/sitter.xml
svcadm enable manatee-sitter

manta_common_setup_end

ZK_IPS=$(json -f ${METADATA} ZK_SERVERS | json -a host)
#functions
echo "mbunyan() { bunyan -c \"this.component !== 'ZKPlus'\"  -c 'level >= 30'; }" >> $PROFILE
echo "manatee-clear(){ /opt/smartdc/manatee/bin/manatee-clear '$SHARD' '$ZONE_IP' '$ZK_IPS'; }" >> $PROFILE
echo "manatee-history(){ /opt/smartdc/manatee/bin/manatee-history '$SHARD' '$ZK_IPS'; }" >> $PROFILE
echo "manatee-stat() { /opt/smartdc/manatee/bin/manatee-stat -p '$ZK_IPS'; }" >> $PROFILE
echo "manatee-snapshots(){ /opt/smartdc/manatee/bin/manatee-snapshots '$DATASET'; }" >> $PROFILE
echo "msitter(){ tail -f \`svcs -L manatee-sitter\` | mbunyan; }" >> $PROFILE
echo "mbackupserver(){ tail -f \`svcs -L manatee-backupserver\` | mbunyan; }" >> $PROFILE
echo "msnapshotter(){ tail -f \`svcs -L manatee-snapshotter\` | mbunyan; }" >> $PROFILE
