#!/bin/bash
export PARENT_ZFS_DS=zones/$(zonename)/data/manatee_integ_tests

useradd postgres

mkdir -p /var/pg
chown postgres /var/pg

export PG_UID=$(id -u postgres)

/usr/bin/ctrun -l child -o noorphan node ../node_modules/.bin/nodeunit ./integ.test.js 2>&1 | bunyan
