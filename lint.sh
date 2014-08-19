#!/bin/bash
set -o errexit
set -o pipefail

git submodule update --init

cd ./deps/javascriptlint
make install
cd ../..

./deps/javascriptlint/build/install/jsl --conf ./tools/jsl.node.conf \
    ./*.js ./lib/*.js ./test/*.js ./bin/manatee-adm

./deps/jsstyle/jsstyle -f ./tools/jsstyle.conf ./*.js  ./lib/*.js \
    ./test/*.js ./bin/manatee-adm

./deps/json/lib/jsontool.js -f ./package.json 1>/dev/null
for i in $(ls ./etc/*.json)
do
    ./deps/json/lib/jsontool.js -f $i --validate
done
for i in $(ls ./test/etc/*.json)
do
    ./deps/json/lib/jsontool.js -f $i --validate
done
