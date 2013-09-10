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

# Just in case, create the backup directory:
if [[ ! -e "${TARGET_DIR}" ]]; then
  mkdir -p ${TARGET_DIR}
fi

DATA_DATASET=$(zfs list -H -o name|grep "${ZONE}/data/manatee")

# We cannot backup if cannot find data dataset:
if [[ -z $DATA_DATASET ]]; then
  echo "FATAL: Cannot find '${ROLE}' data dataset"
  exit 103
fi

STAMP=$(date +'%F-%H-%M-%S-%Z')
BACKUP_VERSION="manatee-data-${STAMP}"

# Create data dataset backup
echo "==> Creating snapshot of '${ZONE}/data/manatee' dataset"
zfs snapshot zones/${ZONE}/data/manatee@${BACKUP_VERSION} 2>&1
if [[ $? -gt 0 ]]; then
    echo "FATAL: Unable to snapshot data dataset"
    exit 106
fi

# Create backup directory for the zone stuff:
echo "==> Creating backup directory '${TARGET_DIR}/${ROLE}'"
mkdir -p "${TARGET_DIR}/${ROLE}"

# Send the dataset snapshots:

echo "==> Saving data dataset"
zfs send "zones/${ZONE}/data/manatee@${BACKUP_VERSION}" \
    > "${TARGET_DIR}/${ROLE}/manatee-data.zfs" 2>&1
if [[ $? -gt 0 ]]; then
    echo "Unable to zfs send data dataset"
    exit 108
fi

echo "==> Removing temporary snapshot of '${ZONE}'"
/usr/sbin/zfs destroy "zones/${ZONE}/data/manatee@${BACKUP_VERSION}"

echo "==> Saving logs"
cd /zones/$ZONE/root/var
tar cbfE 512 - svc/log/sds-application-manatee* pg | gzip \
    >${TARGET_DIR}/${ROLE}/logs.tar.gz

exit 0
