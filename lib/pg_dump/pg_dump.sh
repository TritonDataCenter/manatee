#!/bin/bash
set -o xtrace
PATH=/opt/smartdc/manatee/build/node/bin:/opt/smartdc/manatee/lib/tools:/opt/local/bin:/usr/sbin/:/usr/bin:/root/manatee/lib/pg_dump/:$PATH

#XXX need to pickup the url from mdata-get
MANTA_URL="http://manta.coal.joyent.us"
MANTA_USER="poseidon"
MANTA_KEY_PATH="/root/.ssh/"

function fatal
{
  echo "$(basename $0): fatal error: $*"
  rm -rf $snapshot_output
  exit 1
}

my_ip=$(mdata-get sdc:nics.0.ip)
[[ $? -eq 0 ]] || fatal "Unable to retrieve our own IP address"
svc_name=$(mdata-get service_name)
[[ $? -eq 0 ]] || fatal "Unable to retrieve service name"
zk_ip=$(mdata-get nameservers | cut -d ' ' -f1)
[[ $? -eq 0 ]] || fatal "Unable to retrieve nameservers from metadata"

function backup
{
        echo "backing up"
        echo "getting latest snapshot"
        local manta_dir_prefix=/manatee_backups
        snapshot=$(zfs list -t snapshot | tail -1 | cut -d ' ' -f1)
        uuid=`uuid`
        snapshot_output="/tmp/$uuid"
        echo "dumping and bzipping backup to $snapshot_output"
        zfs send $snapshot | bzip2 > $snapshot_output
        [[ $? -eq 0 ]] || fatal "unable dump snapshot"
        echo "making backup dir $manta_dir_prefix$svc_name"
        time=$(date +%F-%H-%M-%S)
        mmkdir.js -u $MANTA_URL -a $MANTA_USER -k $MANTA_KEY_PATH $manta_dir_prefix
        [[ $? -eq 0 ]] || fatal "unable to create backup dir"
        mmkdir.js -u $MANTA_URL -a $MANTA_USER -k $MANTA_KEY_PATH $manta_dir_prefix/$svc_name
        [[ $? -eq 0 ]] || fatal "unable to create backup dir"
        mmkdir.js -u $MANTA_URL -a $MANTA_USER -k $MANTA_KEY_PATH $manta_dir_prefix/$svc_name/$time
        [[ $? -eq 0 ]] || fatal "unable to create backup dir"
        echo "uploading snapshot to manta"
        mput.js -u $MANTA_URL -a $MANTA_USER -k $MANTA_KEY_PATH -f $snapshot_output $manta_dir_prefix/$svc_name/$time/backup.bz2
        [[ $? -eq 0 ]] || fatal "unable to upload backup"
        echo "finished backup, removing backup file $snapshot_output"
        rm -rf $snapshot_output
}

# s/./\./ to 1.moray.us.... for json
read -r svc_name_delim< <(echo $svc_name | gsed -e 's|\.|\\.|g')

# figure out if we are the peer that should perform backups.
shard_info=$(manatee_stat.js -z $zk_ip:2181 -s $svc_name)
[[ $? -eq 0 ]] || fatal "Unable to retrieve shardinfo from zookeeper"

async=$(echo $shard_info | json $svc_name_delim.async.url)
[[ $? -eq 0 ]] || fatal "unable to parse async peer"
sync=$(echo $shard_info | json $svc_name_delim.sync.url)
[[ $? -eq 0 ]] || fatal "unable to parse sync peer"
primary=$(echo $shard_info | json $svc_name_delim.primary.url)
[[ $? -eq 0 ]] || fatal "unable to parse primary peer"

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

if [ $continue_backup = '1' ]
then
        backup
else
        echo "not performing backup, not lowest peer in shard"
        exit 0
fi
