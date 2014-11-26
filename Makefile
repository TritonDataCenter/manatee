#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#

#
# Copyright (c) 2014, Joyent, Inc.
#

JSL		 = ./deps/javascriptlint/build/install/jsl
JSL_CONF_NODE	 = ./tools/jsl.node.conf
JSSTYLE		 = ./deps/jsstyle/jsstyle
JSSTYLE_FLAGS	 = -f ./tools/jsstyle.conf
NPM		 = npm

JS_FILES	:= \
	$(wildcard ./*.js ./lib/*.js ./test/*.js) \
	bin/manatee-adm \
	tools/mksitterconfig
JSL_FILES_NODE	 = $(JS_FILES)
JSSTYLE_FILES	 = $(JS_FILES)

JSON_FILES	 = \
    $(wildcard ./etc/*.json ./test/etc/*.json) \
    package.json

include Makefile.defs

all:
	$(NPM) install

include Makefile.targ
