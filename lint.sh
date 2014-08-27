#!/bin/bash
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#

#
# Copyright (c) 2014, Joyent, Inc.
#

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
