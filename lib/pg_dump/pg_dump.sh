#!/bin/bash
set -o xtrace

function fatal
{
  echo "$(basename $0): fatal error: $*"
  exit 1
}

function backup
{

        echo "backing up"
        echo "getting latest snapshot"
        snapshot=$(zfs list -t snapshot | tail -1 | cut -d ' ' -f1)
        uuid=`uuid`
        snapshot_output="/tmp/$uuid"
        echo "dumping and bzipping backup to $snapshot_output"
        zfs send $snapshot | bzip2 > $snapshot_output
        [[ $? -eq 0 ]] || fatal "unable dump snapshot"
        echo "uploading snapshot to manta"
}

my_ip=$(mdata-get sdc:nics.0.ip)
[[ $? -eq 0 ]] || fatal "Unable to retrieve our own IP address"
svc_name=$(mdata-get service_name)
[[ $? -eq 0 ]] || fatal "Unable to retrieve service name"
zk_ip=$(mdata-get nameservers | cut -d ' ' -f1)
[[ $? -eq 0 ]] || fatal "Unable to retrieve nameservers from metadata"

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

#XXX REMOVE
continue_backup=1
if [ $continue_backup = '1' ]
then
        backup
else
        echo "not performing backup, not lowest peer in shard"
        exit 0
fi
