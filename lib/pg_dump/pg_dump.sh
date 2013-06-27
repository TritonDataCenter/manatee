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

# s/./\./ to 1.moray.us.... for json
read -r svc_name_delim< <(echo $svc_name | gsed -e 's|\.|\\.|g')

# figure out if we are the peer that should perform backups.
shard_info=$($MANATEE_STAT $zk_ip:2181 -s $svc_name)
[[ $? -eq 0 ]] || fatal "Unable to retrieve shardinfo from zookeeper"

primary=$(echo $shard_info | json $svc_name_delim.primary.ip)
[[ $? -eq 0 ]] || fatal "unable to parse primary peer"

continue_backup=0

if [ "$primary" = "$my_ip" ]
then
    backup
    for tries in {0..4}
    do
        echo "upload attempt $tries"
        upload
        if [[ $? -eq 0 ]]
        then
            echo "successfully finished uploading attempt $tries, exiting"
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
