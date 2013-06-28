#!/bin/bash
echo ""   # blank line in log file helps scroll btwn instances
source /root/.bashrc # source in the manta configs such as the url and credentials
export PS4='[\D{%FT%TZ}] ${BASH_SOURCE}:${LINENO}: ${FUNCNAME[0]:+${FUNCNAME[0]}(): }'
set -o xtrace
set -o pipefail

PATH=/opt/smartdc/manatee/build/node/bin:/opt/local/bin:/usr/sbin/:/usr/bin:/usr/sbin:/usr/bin:/opt/smartdc/registrar/build/node/bin:/opt/smartdc/registrar/node_modules/.bin:/opt/smartdc/manatee/lib/tools:/opt/smartdc/manatee/lib/pg_dump/

CFG=/opt/smartdc/manatee/etc/backup.json
ZFS_CFG=/opt/smartdc/manatee/etc/snapshotter.json
PG_PID=

function fatal
{
    echo "$(basename $0): fatal error: $*"
    kill -9 $PG_PID
    zfs destroy -R $DUMP_DATASET
    exit 1
}

MANATEE_STAT=/opt/smartdc/manatee/bin/manatee-stat
DATASET=$(cat $ZFS_CFG | json dataset)
[[ -n "$DATASET" ]] || fatal "unable to retrieve DATASET"
DUMP_DATASET=zones/$(zonename)/data/pg_dump
PG_DIR=/$DUMP_DATASET/data
UPLOAD_SNAPSHOT=$(cat $CFG | json -a upload_snapshot)
my_ip=$(mdata-get sdc:nics.0.ip)
[[ -n "$my_ip" ]] || fatal "Unable to retrieve our own IP address"
svc_name=$(cat $CFG | json -a service_name)
[[ -n "$svc_name" ]] || fatal "Unable to retrieve service name"
zk_ip=$(cat $CFG | json -a zkCfg.servers.0.host)
[[ -n "$zk_ip" ]] || fatal "Unable to retrieve nameservers from metadata"
dump_dir=/var/tmp/upload
mkdir $dump_dir

mmkdir='/opt/smartdc/manatee/node_modules/manta/bin/mmkdir'
mput='/opt/smartdc/manatee/node_modules/manta/bin/mput'

function upload_zfs_snapshot
{
    echo "take a snapshot"
    local zfs_snapshot=$DATASET@$(date +%s)000
    zfs snapshot $zfs_snapshot
    [[ $? -eq 0 ]] || fatal "Unable to create a snapshot"
    echo "getting latest snapshot"
    snapshot=$(zfs list -Hp -t snapshot | grep $DATASET | tail -n 1 | cut -f1)
    [[ $? -eq 0 ]] || fatal "Unable to retrieve latest snapshot"

    # only upload the snapshot if the flag is set
    if [[ UPLOAD_SNAPSHOT -eq 1 ]]
    then
        local snapshot_size=$(zfs list -Hp -o refer -t snapshot $snapshot)
        [[ $? -eq 0 ]] || fatal "Unable to retrieve snapshot size"
        # pad the snapshot_size by 5% since there's some zfs overhead, note the
        # last bit just takes the floor of the floating point value
        local snapshot_size=$(echo "$snapshot_size * 1.05" | bc | cut -d '.' -f1)
        [[ -n "$snapshot_size" ]] || fatal "Unable to calculate snapshot size"
        local manta_dir_prefix=/poseidon/stor/manatee_backups
        local dir=$manta_dir_prefix/$svc_name/$(date -u +%Y/%m/%d/%H)
        $mmkdir -p -u $MANTA_URL -a $MANTA_USER -k $MANTA_KEY_ID $dir
        [[ $? -eq 0 ]] || fatal "unable to create backup dir"
        echo "sending snapshot $snapshot to manta"
        local snapshot_manta_name=$(echo $snapshot | gsed -e 's|\/|\-|g')
        zfs send $snapshot | $mput $dir/$snapshot_manta_name -H "max-content-length: $snapshot_size"
        [[ $? -eq 0 ]] || fatal "unable to send snapshot $snapshot"

        echo "successfully backed up snapshot $snapshot to manta file $dir/$snapshot_manta_name"
    fi
}

function mount_data_set
{
    # destroy the dump dataset if it already exists
    zfs destroy -R $DUMP_DATASET
    # clone the current snapshot
    zfs clone $snapshot $DUMP_DATASET
    [[ $? -eq 0 ]] || fatal "unable to clone snapshot"
    echo "successfully mounted dataset"
    # remove recovery.conf so this pg instance does not become a slave
    rm -f $PG_DIR/recovery.conf
    # remove postmaster.pid
    rm -f $PG_DIR/postmaster.pid

    ctrun -o noorphan sudo -u postgres postgres -D $PG_DIR -p 23456 &
    PG_PID=$!
    [[ $? -eq 0 ]] || fatal "unable to start postgres"

    echo 'sleep some seconds so we wait for pg to start'
    sleep 3
    echo "postgres started"
}

function backup
{
    local date=$(date -u +%Y-%m-%d-%H)

    echo "getting db tables"
    schema=$dump_dir/$date'_schema'
    # trim the first 3 lines of the schema dump
    sudo -u postgres psql -p 23456 moray -c '\dt' | sed -e '1,3d' > $schema
    [[ $? -eq 0 ]] || (rm $schema; fatal "unable to read db schema")
    for i in `sed 'N;$!P;$!D;$d' $schema | tr -d ' '| cut -d '|' -f2`
    do
        local time=$(date -u +%F-%H-%M-%S)
        local dump_file=$dump_dir/$date'_'$i-$time.gz
        sudo -u postgres pg_dump -p 23456 moray -a -t $i | sqlToJson.js | gzip -1 > $dump_file
        [[ $? -eq 0 ]] || (rm $schema; fatal "Unable to dump table $i")
    done
    # dump the entire moray db as well for manatee backups.
    full_dump_file=$dump_dir/$date'_'moray-$time.gz
    sudo -u postgres pg_dump -p 23456 moray | gzip -1 > $full_dump_file
    [[ $? -eq 0 ]] || (rm $schema; fatal "Unable to dump full moray db")
    rm $schema
    [[ $? -eq 0 ]] || fatal "unable to remove schema"
}

function upload
{
    local upload_error=0;
    local manta_dir_prefix=/poseidon/stor/manatee_backups
    for f in $(ls $dump_dir)
    do
        local year=$(echo $f | cut -d _ -f 1 | cut -d - -f 1)
        local month=$(echo $f | cut -d _ -f 1 | cut -d - -f 2)
        local day=$(echo $f | cut -d _ -f 1 | cut -d - -f 3)
        local hour=$(echo $f | cut -d _ -f 1 | cut -d - -f 4)
        local name=$(echo $f | cut -d _ -f 2-)
        local dir=$manta_dir_prefix/$svc_name/$year/$month/$day/$hour
        $mmkdir -p $dir
        if [[ $? -ne 0 ]]
        then
            echo "unable to create backup dir"
            upload_error=1
            continue;
        fi
        echo "uploading dump $f to manta"
        $mput -f $dump_dir/$f $dir/$name
        if [[ $? -ne 0 ]]
        then
            echo "unable to upload dump $dump_dir/$f"
            upload_error=1
        else
            echo "removing dump $dump_dir/$f"
            rm $dump_dir/$f
        fi
    done

    return $upload_error
}

function cleanup
{
    kill -9 $PG_PID
    [[ $? -eq 0 ]] || fatal "unable to kill postgres"
    zfs destroy -R $DUMP_DATASET
    [[ $? -eq 0 ]] || fatal "unable destroy dataset"
}


# s/./\./ to 1.moray.us.... for json
read -r svc_name_delim< <(echo $svc_name | gsed -e 's|\.|\\.|g')

# figure out if we are the peer that should perform backups.
shard_info=$($MANATEE_STAT $zk_ip:2181 -s $svc_name)
[[ -n $shard_info ]] || fatal "Unable to retrieve shardinfo from zookeeper"

async=$(echo $shard_info | json $svc_name_delim.async.ip)
[[ -n $async ]] || echo "warning: unable to parse async peer"
sync=$(echo $shard_info | json $svc_name_delim.sync.ip)
[[ -n $sync ]] || echo "warning: unable to parse sync peer"
primary=$(echo $shard_info | json $svc_name_delim.primary.ip)
[[ -n $primary ]] || fatal "unable to parse primary peer"

continue_backup=0
if [ "$async" = "$my_ip" ]
then
    continue_backup=1
fi

if [ -z "$async" ] && [ "$sync" = "$my_ip" ]
then
    continue_backup=1
fi

if [ -z "$sync" ] && [ -z "$async" ] && [ "$primary" = "$my_ip" ]
then
    continue_backup=1
else
    if [ -z "$sync" ] && [ -z "$async" ]
    then
        fatal "not primary but async/sync dne, exiting 1"
    fi
fi

#XXX remove this
continue_backup = 1
if [ $continue_backup = '1' ]
then
    upload_zfs_snapshot
    mount_data_set
    backup
    for tries in {1..5}
    do
        echo "upload attempt $tries"
        upload
        if [[ $? -eq 0 ]]
        then
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
