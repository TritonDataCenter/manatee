#!/bin/bash
echo ""   # blank line in log file helps scroll btwn instances
export PS4='[\D{%FT%TZ}] ${BASH_SOURCE}:${LINENO}: ${FUNCNAME[0]:+${FUNCNAME[0]}(): }'
set -o xtrace
set -o pipefail

PATH=/opt/smartdc/manatee/build/node/bin:/opt/local/bin:/usr/sbin/:/usr/bin:/usr/sbin:/usr/bin:/opt/smartdc/registrar/build/node/bin:/opt/smartdc/registrar/node_modules/.bin:/opt/smartdc/manatee/lib/tools:/opt/smartdc/manatee/lib/pg_dump/

MANTA_URL=$(mdata-get manta_url)
MANTA_USER="poseidon"
MANTA_KEY_ID=$(ssh-keygen -lf ~/.ssh/id_rsa.pub | cut -d ' ' -f2)
MANATEE_STAT=/opt/smartdc/manatee/bin/manatee-stat
ZONE_UUID=$(zoneadm list -p | cut -d ':' -f2)
DATASET=zones/$ZONE_UUID/data/manatee

mmkdir='/opt/smartdc/manatee/node_modules/manta/bin/mmkdir'
mput='/opt/smartdc/manatee/node_modules/manta/bin/mput'

function fatal
{
    echo "$(basename $0): fatal error: $*"
    exit 1
}

function backup
{
    echo "backing up"
    echo "getting latest snapshot"
    local snapshot=$(zfs list -Hp -t snapshot | grep $DATASET | tail -n 1 | cut -f1)
    [[ $? -eq 0 ]] || fatal "Unable to retrieve latest snapshot"
    # column 4 is the refer size
    local snapshot_size=$(zfs list -Hp -t snapshot | grep $DATASET | tail -n 1 | cut -f4)
    [[ $? -eq 0 ]] || fatal "Unable to retrieve snapshot size"
    # pad the snapshot_size by 5% since there's some zfs overhead, note the
    # last bit just takes the floor of the floating point value
    local snapshot_size=$(echo "$snapshot_size * 1.05" | bc | cut -d '.' -f1)
    local manta_dir_prefix=/poseidon/stor/manatee_backups
    local year=$(date -u +%Y)
    local month=$(date -u +%m)
    local day=$(date -u +%d)
    local hour=$(date -u +%H)
    local dir=$manta_dir_prefix/$svc_name/$year/$month/$day/$hour
    $mmkdir -p -u $MANTA_URL -a $MANTA_USER -k $MANTA_KEY_ID $dir
    [[ $? -eq 0 ]] || fatal "unable to create backup dir"

    echo "sending snapshot $snapshot to manta"
    local snapshot_manta_name=$(echo $snapshot | gsed -e 's|\/|\-|g')
    zfs send $snapshot | $mput $dir/$snapshot_manta_name -H "max-content-length: $snapshot_size"
    [[ $? -eq 0 ]] || fatal "unable to send snapshot $snapshot"

    echo "successfully backed up snapshot $snapshot to manta file $dir/$snapshot_manta_name"
    exit 0
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
shard_info=$($MANATEE_STAT $zk_ip:2181 -s $svc_name)
[[ $? -eq 0 ]] || fatal "Unable to retrieve shardinfo from zookeeper"

async=$(echo $shard_info | json $svc_name_delim.async.ip)
[[ $? -eq 0 ]] || fatal "unable to parse async peer"
sync=$(echo $shard_info | json $svc_name_delim.sync.ip)
[[ $? -eq 0 ]] || fatal "unable to parse sync peer"
primary=$(echo $shard_info | json $svc_name_delim.primary.ip)
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
