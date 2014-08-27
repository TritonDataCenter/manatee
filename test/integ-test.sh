#!/bin/bash
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#

#
# Copyright (c) 2014, Joyent, Inc.
#

export PARENT_ZFS_DS=zones/$(zonename)/data/manatee_integ_tests

useradd postgres

mkdir -p /var/pg
chown postgres /var/pg

export PG_UID=$(id -u postgres)

/usr/bin/ctrun -l child -o noorphan node ../node_modules/.bin/nodeunit ./integ.test.js 2>&1 | bunyan
