#!/bin/bash

# Postgres backup script. This script takes a snapshot of the current postgres
# data dir, then mounts said snapshot, and dumps all of the tables from
# postgres, and uploads them to manta

source ./pg_backup_common.sh

PG_START_TIMEOUT=$1
PG_START_MAX_TRIES=50
PG_START_TRIES=0
DATASET=
DUMP_DATASET=
PG_DIR=
UPLOAD_SNAPSHOT=
MY_IP=
SHARD_NAME=
ZK_IP=

# mainline

if [[ -z "$1" ]]
    then
        PG_START_TIMEOUT=300
    else
        PG_START_TIMEOUT=$1
fi

DATASET=$(cat $ZFS_CFG | json dataset)
[[ -n "$DATASET" ]] || fatal "unable to retrieve DATASET"
DUMP_DATASET=zones/$(zonename)/data/pg_dump
PG_DIR=/$DUMP_DATASET/data
UPLOAD_SNAPSHOT=$(cat $CFG | json -a upload_snapshot)
MY_IP=$(mdata-get sdc:nics.0.ip)
[[ -n "$MY_IP" ]] || fatal "Unable to retrieve our own IP address"
SHARD_NAME=$(cat $CFG | json -a service_name)
[[ -n "$SHARD_NAME" ]] || fatal 'Unable to retrieve $SHARD_NAME'
ZK_IP=$(cat $CFG | json -a zkCfg.servers.0.host)
[[ -n "$ZK_IP" ]] || fatal "Unable to retrieve nameservers from metadata"

get_self_role
if [[ $? = '1' ]]; then
    take_zfs_snapshot
    check_lock
    mount_data_set
    backup
    for tries in {1..5}; do
        echo "upload attempt $tries"
        upload_pg_dumps
        if [[ $? -eq 0 ]]; then
            echo "successfully finished uploading attempt $tries"
            cleanup
            exit 0
        else
            echo "attempt $tries failed"
        fi
    done

    fatal "unable to upload all pg dumps"
else
    echo "not performing backup, not lowest peer in shard"
    exit 0
fi
