#!/bin/bash
echo ""   # blank line in log file helps scroll btwn instances
export PS4='[\D{%FT%TZ}] ${BASH_SOURCE}:${LINENO}: ${FUNCNAME[0]:+${FUNCNAME[0]}(): }'
set -o xtrace

PATH=/opt/smartdc/manatee/build/node/bin:/opt/local/bin:/usr/sbin/:/usr/bin:/usr/sbin:/usr/bin:/opt/smartdc/registrar/build/node/bin:/opt/smartdc/registrar/node_modules/.bin:/opt/smartdc/manatee/lib/tools:/opt/smartdc/manatee/lib/pg_dump/
CFG=/opt/smartdc/manatee/etc/backup.json

MANTA_URL=$(cat $CFG | json -a manta_url)
MANTA_USER="poseidon"
MANTA_KEY_ID=`ssh-keygen -lf ~/.ssh/id_rsa.pub | cut -d ' ' -f2`
MANTA_TLS_INSECURE=$(cat $CFG | json -a manta_tls_insecure)
MANATEE_STAT=/opt/smartdc/manatee/bin/manatee-stat

function fatal
{
    echo "$(basename $0): fatal error: $*"
    rm -rf $dump_dir
    exit 1
}

my_ip=$(mdata-get sdc:nics.0.ip)
[[ $? -eq 0 ]] || fatal "Unable to retrieve our own IP address"
svc_name=$(cat $CFG | json -a service_name)
[[ $? -eq 0 ]] || fatal "Unable to retrieve service name"
zk_ip=$(cat $CFG | json -a zkCfg.servers.0.host)
[[ $? -eq 0 ]] || fatal "Unable to retrieve nameservers from metadata"
dump_dir=/var/tmp/`uuid`
mkdir $dump_dir
[[ $? -eq 0 ]] || fatal "Unable to make temp dir"

mmkdir='/opt/smartdc/manatee/node_modules/manta/bin/mmkdir'
mput='/opt/smartdc/manatee/node_modules/manta/bin/mput'

function backup
{
    local manta_dir_prefix=/poseidon/stor/manatee_backups
    local year=$(date -u +%Y)
    local month=$(date -u +%m)
    local day=$(date -u +%d)
    local hour=$(date -u +%H)
    local dir=$manta_dir_prefix/$svc_name/$year/$month/$day/$hour
    $mmkdir -p -u $MANTA_URL -a $MANTA_USER -k $MANTA_KEY_ID $dir
    [[ $? -eq 0 ]] || fatal "unable to create backup dir"

    echo "getting db tables"
    schema=$dump_dir/schema
    # trim the first 3 lines of the schema dump
    sudo -u postgres psql moray -c '\dt' | sed -e '1,3d' > $schema
    [[ $? -eq 0 ]] || fatal "unable to read db schema"
    for i in `sed 'N;$!P;$!D;$d' $schema | tr -d ' '| cut -d '|' -f2`
    do
        local dump_file=$dump_dir/$i
        local time=$(date -u +%F-%H-%M-%S)
        sudo -u postgres pg_dump moray -a -t $i | sqlToJson.js | gzip -1 > $dump_file
        [[ $? -eq 0 ]] || fatal "Unable to dump table $i"
        echo "uploading dump $i to manta"
        $mput -u $MANTA_URL -a $MANTA_USER -k $MANTA_KEY_ID -f $dump_file $dir/$i-$time.gz
        [[ $? -eq 0 ]] || fatal "unable to upload dump $i"
        echo "removing dump $dump_file"
        rm $dump_file
    done
    echo "finished backup, removing backup dir $dump_dir"
    rm -rf $dump_dir
}

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
