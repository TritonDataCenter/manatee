#!/usr/bin/bash
#
# Copyright (c) 2012 Joyent Inc., All rights reserved.
#

export PS4='[\D{%FT%TZ}] ${BASH_SOURCE}:${LINENO}: ${FUNCNAME[0]:+${FUNCNAME[0]}(): }'
set -o xtrace

PATH=/opt/local/bin:/opt/local/sbin:/usr/bin:/usr/sbin

# Install zookeeper package, need to touch this file to disable the license prompt
touch /opt/local/.dli_license_accepted

role=manatee

# Local manifests
CONFIG_AGENT_LOCAL_MANIFESTS_DIRS=/opt/smartdc/$role

# Include common utility functions (then run the boilerplate)
source /opt/smartdc/sdc-boot/scripts/util.sh
sdc_common_setup

# Cookie to identify this as a SmartDC zone and its role
mkdir -p /var/smartdc/$role
mkdir -p /opt/smartdc/$role/ssl

#echo "Installing local node.js"
mkdir -p /opt/smartdc/$role/etc
cd -
/usr/bin/chown -R root:root /opt/smartdc

# Add build/node/bin and node_modules/.bin to PATH
echo "" >>/root/.profile
echo "export PATH=\$PATH:/opt/smartdc/$role/build/node/bin:/opt/smartdc/$role/node_modules/.bin" >>/root/.profile

# All done, run boilerplate end-of-setup
sdc_setup_complete

exit 0
