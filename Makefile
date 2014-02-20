#
# Copyright (c) 2012, Joyent, Inc. All rights reserved.
#
# Makefile: basic Makefile for template API service
#
# This Makefile is a template for new repos. It contains only repo-specific
# logic and uses included makefiles to supply common targets (javascriptlint,
# jsstyle, restdown, etc.), which are used by other repos as well. You may well
# need to rewrite most of this file, but you shouldn't need to touch the
# included makefiles.
#
# If you find yourself adding support for new targets that could be useful for
# other projects too, you should add these to the original versions of the
# included Makefiles (in eng.git) so that other teams can use them too.
#

#
# Tools
#
TAP		:= ./node_modules/.bin/tap
TAR = tar
UNAME := $(shell uname)

ifeq ($(UNAME), SunOS)
	TAR = gtar
endif

#
# Files
#
DOC_FILES	 = index.restdown
JS_FILES	:= $(shell ls *.js) $(shell find lib client test -name '*.js') $(find ./bin/* -type f)
JSL_CONF_NODE	 = tools/jsl.node.conf
JSL_FILES_NODE   = $(JS_FILES)
JSSTYLE_FILES	 = $(JS_FILES)
JSSTYLE_FLAGS    = -C -f ./tools/jsstyle.conf
SMF_MANIFESTS_IN = smf/manifests/backupserver.xml.in \
		smf/manifests/sitter.xml.in \
		smf/manifests/snapshotter.xml.in

#
# Variables
#

NODE_PREBUILT_VERSION   := v0.10.26
NODE_PREBUILT_TAG       := zone
# Allow building on a SmartOS image other than sdc-multiarch/13.3.1.
NODE_PREBUILT_IMAGE=b4bdc598-8939-11e3-bea4-8341f6861379


include ./tools/mk/Makefile.defs
include ./tools/mk/Makefile.node_prebuilt.defs
include ./tools/mk/Makefile.node_deps.defs
include ./tools/mk/Makefile.smf.defs

RELEASE_TARBALL         := manatee-pkg-$(STAMP).tar.bz2
ROOT                    := $(shell pwd)
RELSTAGEDIR                  := /tmp/$(STAMP)

#
# Env variables
#
PATH            := $(NODE_INSTALL)/bin:${PATH}

#
# Repo-specific targets
#
.PHONY: all
all: $(SMF_MANIFESTS) | $(TAP) $(REPO_DEPS) scripts sdc-scripts
	$(NPM) rebuild

$(TAP): | $(NPM_EXEC)
	$(NPM) install

CLEAN_FILES += $(TAP) ./node_modules/tap
DISTCLEAN_FILES = ./node_modules

include ./tools/mk/Makefile.deps
include ./tools/mk/Makefile.node_prebuilt.targ
include ./tools/mk/Makefile.node_deps.targ
include ./tools/mk/Makefile.smf.targ
include ./tools/mk/Makefile.targ

.PHONY: scripts
scripts: deps/manta-scripts/.git
	mkdir -p $(BUILD)/scripts
	cp deps/manta-scripts/*.sh $(BUILD)/scripts

.PHONY: release
release: all deps docs $(SMF_MANIFESTS)
	@echo "Building $(RELEASE_TARBALL)"
	@mkdir -p $(RELSTAGEDIR)/root/opt/smartdc/manatee
	@mkdir -p $(RELSTAGEDIR)/root/opt/smartdc/boot
	@mkdir -p $(RELSTAGEDIR)/site
	@touch $(RELSTAGEDIR)/site/.do-not-delete-me
	@mkdir -p $(RELSTAGEDIR)/root
	cp -r   $(ROOT)/lib \
		$(ROOT)/bin \
		$(ROOT)/deps \
		$(ROOT)/sitter.js \
		$(ROOT)/backupserver.js \
		$(ROOT)/snapshotter.js \
		$(ROOT)/node_modules \
		$(ROOT)/package.json \
		$(ROOT)/sapi_manifests \
		$(ROOT)/smf \
		$(ROOT)/etc \
		$(RELSTAGEDIR)/root/opt/smartdc/manatee/
	mkdir -p $(RELSTAGEDIR)/root/opt/smartdc/manatee/build
	cp -r \
		$(TOP)/build/node \
		$(RELSTAGEDIR)/root/opt/smartdc/manatee/build
	mkdir -p $(RELSTAGEDIR)/root/opt/smartdc/boot/scripts
	cp -R $(RELSTAGEDIR)/root/opt/smartdc/manatee/build/scripts/* \
	    $(RELSTAGEDIR)/root/opt/smartdc/boot/scripts/
	cp -R $(ROOT)/deps/sdc-scripts/* \
	    $(RELSTAGEDIR)/root/opt/smartdc/boot/
	cp -R $(ROOT)/boot/* \
	    $(RELSTAGEDIR)/root/opt/smartdc/boot/
	(cd $(RELSTAGEDIR) && $(TAR) -jcf $(ROOT)/$(RELEASE_TARBALL) root site)
	@rm -rf $(RELSTAGEDIR)

.PHONY: publish
publish: release
	@if [[ -z "$(BITS_DIR)" ]]; then \
		echo "error: 'BITS_DIR' must be set for 'publish' target"; \
		exit 1; \
	fi
	mkdir -p $(BITS_DIR)/manatee
	cp $(ROOT)/$(RELEASE_TARBALL) $(BITS_DIR)/manatee/$(RELEASE_TARBALL)

sdc-scripts: deps/sdc-scripts/.git
