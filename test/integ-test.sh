PARENT_ZFS_DS=zones/0f74f025-2345-44b1-8403-faf38695742a/data/manatee_integ_tests  /usr/bin/ctrun -l child -o noorphan ../node_modules/.bin/nodeunit ./integ.test.js 2>&1 | bunyan
