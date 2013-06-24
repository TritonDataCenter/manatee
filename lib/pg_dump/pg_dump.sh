#!/bin/bash
echo ""   # blank line in log file helps scroll btwn instances
export PS4='[\D{%FT%TZ}] ${BASH_SOURCE}:${LINENO}: ${FUNCNAME[0]:+${FUNCNAME[0]}(): }'
set -o xtrace

PATH=/opt/smartdc/manatee/build/node/bin:/opt/local/bin:/usr/sbin/:/usr/bin:/usr/sbin:/usr/bin:/opt/smartdc/registrar/build/node/bin:/opt/smartdc/registrar/node_modules/.bin:/opt/smartdc/manatee/lib/tools:/opt/smartdc/manatee/lib/pg_dump/
CFG=/opt/smartdc/manatee/etc/backup.json

# The 'm' commands will pick these up automagically if they are exported.
export MANTA_URL=$(cat $CFG | json -a manta_url)
export MANTA_USER="poseidon"
export MANTA_KEY_ID=`ssh-keygen -lf ~/.ssh/id_rsa.pub | cut -d ' ' -f2`
export MANTA_TLS_INSECURE=$(cat $CFG | json -a manta_tls_insecure)
MANATEE_STAT=/opt/smartdc/manatee/bin/manatee-stat

function fatal
{
    echo "$(basename $0): fatal error: $*"
    exit 1
}

my_ip=$(mdata-get sdc:nics.0.ip)
[[ $? -eq 0 ]] || fatal "Unable to retrieve our own IP address"
svc_name=$(cat $CFG | json -a service_name)
[[ $? -eq 0 ]] || fatal "Unable to retrieve service name"
zk_ip=$(cat $CFG | json -a zkCfg.servers.0.host)
[[ $? -eq 0 ]] || fatal "Unable to retrieve nameservers from metadata"
dump_dir=/var/tmp/upload
mkdir $dump_dir

mmkdir='/opt/smartdc/manatee/node_modules/manta/bin/mmkdir'
mput='/opt/smartdc/manatee/node_modules/manta/bin/mput'

function backup
{
    local year=$(date -u +%Y)
    local month=$(date -u +%m)
    local day=$(date -u +%d)
    local hour=$(date -u +%H)

    echo "getting db tables"
    schema=$dump_dir/$year-$month-$day-$hour'_schema'
    # trim the first 3 lines of the schema dump
    sudo -u postgres psql moray -c '\dt' | sed -e '1,3d' > $schema
    [[ $? -eq 0 ]] || (rm $schema; fatal "unable to read db schema")
    for i in `sed 'N;$!P;$!D;$d' $schema | tr -d ' '| cut -d '|' -f2`
    do
        local time=$(date -u +%F-%H-%M-%S)
        local dump_file=$dump_dir/$year-$month-$day-$hour'_'$i-$time.gz
        sudo -u postgres pg_dump moray -a -t $i | sqlToJson.js | gzip -1 > $dump_file
        [[ $? -eq 0 ]] || (rm $schema; fatal "Unable to dump table $i")
    done
    # dump the entire moray db as well for manatee backups.
    full_dump_file=$dump_dir/$year-$month-$day-$hour'_'moray-$time.gz
    sudo -u postgres pg_dump moray | gzip -1 > $full_dump_file
    [[ $? -eq 0 ]] || (rm $schema; fatal "Unable to dump full moray db")
    rm $schema
}

function upload
{
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
        [[ $? -eq 0 ]] || fatal "unable to create backup dir"
        echo "uploading dump $f to manta"
        $mput -f $dump_dir/$f $dir/$name
        [[ $? -eq 0 ]] || fatal "unable to upload dump $dump_dir/$f"
        echo "removing dump $dump_dir/$f"
        rm $dump_dir/$f
    done
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
    upload
else
    echo "not performing backup, not lowest peer in shard"
    exit 0
fi
