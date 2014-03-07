PARENT_ZFS_DS=zones/$(zonename)/data/manatee_integ_tests  /usr/bin/ctrun -l child -o noorphan node ../node_modules/.bin/nodeunit ./integ.test.js 2>&1 | bunyan
