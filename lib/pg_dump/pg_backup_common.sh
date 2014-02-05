#!/bin/bash

# Postgres backup script. This script takes a snapshot of the current postgres
# data dir, then mounts said snapshot, and dumps all of the tables from
# postgres, and uploads them to manta

echo ""   # blank line in log file helps scroll btwn instances
source /root/.bashrc # source in the manta configs such as the url and credentials
export PS4='[\D{%FT%TZ}] ${BASH_SOURCE}:${LINENO}: ${FUNCNAME[0]:+${FUNCNAME[0]}(): }'
set -o xtrace
set -o pipefail

PATH=/opt/smartdc/manatee/build/node/bin:/opt/local/bin:/usr/sbin/:/usr/bin:/usr/sbin:/usr/bin:/opt/smartdc/registrar/build/node/bin:/opt/smartdc/registrar/node_modules/.bin:/opt/smartdc/manatee/lib/tools:/opt/smartdc/manatee/lib/pg_dump/

FATAL=
CFG=/opt/smartdc/manatee/etc/backup.json
DATASET=
DATE=
DUMP_DATASET=
DUMP_DIR=/var/tmp/upload/$(uuid)
MANATEE_LOCK=/opt/smartdc/manatee/bin/manatee-lock
MANATEE_STAT=/opt/smartdc/manatee/bin/manatee-stat
MANTA_DIR_PREFIX=/poseidon/stor/manatee_backups
MMKDIR=/opt/smartdc/manatee/node_modules/manta/bin/mmkdir
MPUT=/opt/smartdc/manatee/node_modules/manta/bin/mput
MY_IP=
LOCK_PATH=/pg_dump_lock
PG_DIR=
PG_PID=
SHARD_NAME=
PG_START_TIMEOUT=$2 || 10
PG_START_MAX_TRIES=50
PG_START_TRIES=0
UPLOAD_SNAPSHOT=
ZFS_CFG=/opt/smartdc/manatee/etc/snapshotter.json
ZFS_SNAPSHOT=$1
ZK_IP=

function finish {
    if [[ $FATAL -ne 1 ]]; then
        rm -rf $DUMP_DIR
    fi
    kill -9 $PG_PID
    zfs destroy -R $DUMP_DATASET
}
trap finish EXIT

function fatal
{
    FATAL=1
    echo "$(basename $0): fatal error: $*"
    kill -9 $PG_PID
    zfs destroy -R $DUMP_DATASET
    exit 1
}

function check_lock
{
    $MANATEE_LOCK $LOCK_PATH $ZK_IP
    [[ $? -eq 0 ]] || fatal "lock either exists or unable to check lock"
}

function take_zfs_snapshot
{
    echo "take a snapshot"
    ZFS_SNAPSHOT=$DATASET@$(date +%s)000
    zfs snapshot $ZFS_SNAPSHOT
    [[ $? -eq 0 ]] || fatal "Unable to create a snapshot"
}

function upload_zfs_snapshot
{
    # only upload the snapshot if the flag is set
    if [[ $UPLOAD_SNAPSHOT -eq 1 ]]; then
        local snapshot_size=$(zfs list -Hp -o refer -t snapshot $ZFS_SNAPSHOT)
        [[ $? -eq 0 ]] || return "Unable to retrieve snapshot size"
        # pad the snapshot_size by 5% since there's some zfs overhead, note the
        # last bit just takes the floor of the floating point value
        local snapshot_size=$(echo "$snapshot_size * 1.05" | bc | cut -d '.' -f1)
        [[ -n "$snapshot_size" ]] || return "Unable to calculate snapshot size"
        local dir=$MANTA_DIR_PREFIX/$SHARD_NAME/$(date -u +%Y/%m/%d/%H)
        $MMKDIR -p -u $MANTA_URL -a $MANTA_USER -k $MANTA_KEY_ID $dir
        [[ $? -eq 0 ]] || return "unable to create backup dir"
        echo "sending snapshot $ZFS_SNAPSHOT to manta"
        local snapshot_manta_name=$(echo $ZFS_SNAPSHOT | gsed -e 's|\/|\-|g')
        zfs send $ZFS_SNAPSHOT | $MPUT $dir/$snapshot_manta_name -H "max-content-length: $snapshot_size"
        [[ $? -eq 0 ]] || return "unable to send snapshot $ZFS_SNAPSHOT"

        echo "successfully backed up snapshot $ZFS_SNAPSHOT to manta file $dir/$snapshot_manta_name"
    fi

    return 0
}

function mount_data_set
{
    # destroy the dump dataset if it already exists
    zfs destroy -R $DUMP_DATASET
    # clone the current snapshot
    zfs clone $ZFS_SNAPSHOT $DUMP_DATASET
    [[ $? -eq 0 ]] || fatal "unable to clone snapshot"
    echo "successfully mounted dataset"
    # remove recovery.conf so this pg instance does not become a slave
    rm -f $PG_DIR/recovery.conf
    # remove postmaster.pid
    rm -f $PG_DIR/postmaster.pid

    ctrun -o noorphan sudo -u postgres postgres -D $PG_DIR -p 23456 &
    PG_PID=$!
    [[ $? -eq 0 ]] || fatal "unable to start postgres"

    wait_for_pg_start
}

function wait_for_pg_start
{
    echo "waiting $PG_START_TIMEOUT seconds for PG to start"
    PG_START_TRIES=$(($PG_START_TRIES + 1))
    if [[ $PG_START_TRIES -gt $PG_START_MAX_TRIES ]]; then
        fatal "PG start tries exceeded, did not start in time"
    fi
    sleep $PG_START_TIMEOUT
    # check and see if pg is up.
    sudo -u postgres psql -p 23456 moray -c 'select current_time'
    if [[ $? -eq 0 ]]; then
        echo "PG has started"
    else
        echo "PG not started yet, waiting again"
        wait_for_pg_start
    fi
}

function backup
{
    local date
    if [[ -z "$DATE" ]]; then
        date=$(date -u +%Y-%m-%d-%H)
    else
        date=$DATE
    fi

    mkdir $DUMP_DIR

    echo "getting db tables"
    schema=$DUMP_DIR/$date'_schema'
    # trim the first 3 lines of the schema dump
    sudo -u postgres psql -p 23456 moray -c '\dt' | sed -e '1,3d' > $schema
    [[ $? -eq 0 ]] || (rm $schema; fatal "unable to read db schema")
    for i in `sed 'N;$!P;$!D;$d' $schema | tr -d ' '| cut -d '|' -f2`
    do
        local time=$(date -u +%F-%H-%M-%S)
        local dump_file=$DUMP_DIR/$date'_'$i-$time.gz
        sudo -u postgres pg_dump -p 23456 moray -a -t $i | gsed 's/\\\\/\\/g' | sqlToJson.js | gzip -1 > $dump_file
        [[ $? -eq 0 ]] || fatal "Unable to dump table $i"
    done
    # dump the entire moray db as well for manatee backups.
    full_dump_file=$DUMP_DIR/$date'_'moray-$time.gz
    sudo -u postgres pg_dump -p 23456 moray | gzip -1 > $full_dump_file
    [[ $? -eq 0 ]] || fatal "Unable to dump full moray db"
    rm $schema
    [[ $? -eq 0 ]] || fatal "unable to remove schema"
}

function upload_pg_dumps
{
    local upload_error=0;
    for f in $(ls $DUMP_DIR); do
        local year=$(echo $f | cut -d _ -f 1 | cut -d - -f 1)
        local month=$(echo $f | cut -d _ -f 1 | cut -d - -f 2)
        local day=$(echo $f | cut -d _ -f 1 | cut -d - -f 3)
        local hour=$(echo $f | cut -d _ -f 1 | cut -d - -f 4)
        local name=$(echo $f | cut -d _ -f 2-)
        local dir=$MANTA_DIR_PREFIX/$SHARD_NAME/$year/$month/$day/$hour
        $MMKDIR -p $dir
        if [[ $? -ne 0 ]]; then
            echo "unable to create backup dir"
            upload_error=1
            continue;
        fi
        echo "uploading dump $f to manta"
        $MPUT -f $DUMP_DIR/$f $dir/$name
        if [[ $? -ne 0 ]]; then
            echo "unable to upload dump $DUMP_DIR/$f"
            upload_error=1
        else
            echo "removing dump $DUMP_DIR/$f"
            rm $DUMP_DIR/$f
        fi
    done

    return $upload_error
}

function get_self_role
{
    # s/./\./ to 1.moray.us.... for json
    read -r shard_name_delim< <(echo $SHARD_NAME | gsed -e 's|\.|\\.|g')

    # figure out if we are the peer that should perform backups.
    local shard_info=$($MANATEE_STAT $ZK_IP:2181 -s $SHARD_NAME)
    [[ -n $shard_info ]] || fatal "Unable to retrieve shardinfo from zookeeper"

    local async=$(echo $shard_info | json $shard_name_delim.async.ip)
    [[ -n $async ]] || echo "warning: unable to parse async peer"
    local sync=$(echo $shard_info | json $shard_name_delim.sync.ip)
    [[ -n $sync ]] || echo "warning: unable to parse sync peer"
    local primary=$(echo $shard_info | json $shard_name_delim.primary.ip)
    [[ -n $primary ]] || fatal "unable to parse primary peer"

    local continue_backup=0
    if [ "$async" = "$MY_IP" ]; then
        continue_backup=1
    elif [[ -z "$async"  &&  "$sync" = "$MY_IP" ]]; then
        continue_backup=1
    elif [[ -z "$sync"  &&  -z "$async"  &&  "$primary" = "$MY_IP" ]]; then
        continue_backup=1
    elif [ -z "$sync" ] && [ -z "$async" ]; then
        fatal "not primary but async/sync dne, exiting 1"
    fi

    return $continue_backup
}

function cleanup
{
    kill -9 $PG_PID
    [[ $? -eq 0 ]] || fatal "unable to kill postgres"
    zfs destroy -R $DUMP_DATASET
    [[ $? -eq 0 ]] || fatal "unable destroy dataset"
}
