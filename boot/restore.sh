#!/usr/bin/bash

PATH=/usr/bin:/usr/sbin
export PATH
if [[ -n "$TRACE" ]]; then
    export PS4='[\D{%FT%TZ}] ${BASH_SOURCE}:${LINENO}: ${FUNCNAME[0]:+${FUNCNAME[0]}(): }'
    set -o xtrace
fi

if [[ $# != 2 ]]; then
  echo "Usage: $0 <zone_name> <target_directory>"
  exit 1
fi

ZONE=$1
TARGET_DIR=$2
ROLE="manatee"

# must shut down manatee services so that the dataset isn't EBUSY
echo "==> shutting down manatee svcs"
zlogin ${ZONE} svcadm disable manatee-sitter
zlogin ${ZONE} svcadm disable manatee-snapshotter
zlogin ${ZONE} svcadm disable manatee-backupserver

# Get existing data dataset.
DATA_DATASET=$(zfs list -H -o name|grep "${ZONE}/data/manatee")

# Destroy previous dataset.
if [[ -z "${DATA_DATASET}" ]]; then
    echo "FATAL: Missing '${DATA_DATASET}' dataset"
    exit 105
fi

echo "==> Destroying dataset '${DATA_DATASET}'"
zfs destroy -r "${DATA_DATASET}"
if [[ $? -gt 0 ]]; then
    echo "FATAL: Unable to zfs destroy '${DATA_DATASET}' dataset"
    exit 106
fi

# ZFS receive the dataset from the backup:
echo "==> Receiving '${TARGET_DIR}/${ROLE}/manatee-data.zfs'"
zfs receive -v "${DATA_DATASET}" < "${TARGET_DIR}/${ROLE}/manatee-data.zfs"
if [[ $? -gt 0 ]]; then
    echo "FATAL: Unable to zfs receive data dataset"
    exit 108
  fi

echo "==> Restoring logs"
gzcat ${TARGET_DIR}/${ROLE}/logs.tar.gz | \
    (cd /zones/$ZONE/root/var; tar xbf 512 -)

# Double check mountpoint for backup dataset:
if [[ "$(zfs get -H -o value mountpoint ${DATA_DATASET})" != \
    "/manatee/pg"  ]]; then

    # We need to boot the zone in order to be able to set mountpoint for the
    # delegated dataset properly:
    echo "==> Booting '${ZONE}' zone"
    /usr/sbin/zoneadm -z ${ZONE} boot


    echo "==> Setting mountpoint for dataset '${DATA_DATASET}'"
    zlogin ${ZONE} /usr/sbin/zfs set mountpoint=/manatee/pg \
       "${DATA_DATASET}"
    if [[ $? -gt 0 ]]; then
        echo "FATAL: Unable to set mountpoint for data dataset into '${ZONE}' zone"
        /usr/sbin/zoneadm -z ${ZONE} halt
        exit 112
    fi

    echo "==> Halting '${ZONE}' zone"
    /usr/sbin/zoneadm -z ${ZONE} halt
fi

echo "==> All done!!!"
exit 0
