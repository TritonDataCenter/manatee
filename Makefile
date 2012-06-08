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
DOC_FILES	 = index.restdown boilerplateapi.restdown
JS_FILES	:= $(shell ls *.js) $(shell find lib test -name '*.js')
JSL_CONF_NODE	 = tools/jsl.node.conf
JSL_FILES_NODE   = $(JS_FILES)
JSSTYLE_FILES	 = $(JS_FILES)
JSSTYLE_FLAGS    = -o indent=2,doxygen,unparenthesized-return=0
SMF_MANIFESTS_IN = smf/manifests/manatee.xml.in

include ./tools/mk/Makefile.defs
include ./tools/mk/Makefile.node.defs
include ./tools/mk/Makefile.node_deps.defs
include ./tools/mk/Makefile.smf.defs

#
# Repo-specific targets
#
.PHONY: all
all: $(SMF_MANIFESTS) | $(TAP) $(REPO_DEPS)
	$(NPM) rebuild

$(TAP): | $(NPM_EXEC)
	$(NPM) install

CLEAN_FILES += $(TAP) ./node_modules/tap

.PHONY: test
test: $(TAP)
	TAP=1 $(TAP) test/*.test.js

include ./tools/mk/Makefile.deps
include ./tools/mk/Makefile.node.targ
include ./tools/mk/Makefile.node_deps.targ
include ./tools/mk/Makefile.smf.targ
include ./tools/mk/Makefile.targ

ROOT                    := $(shell pwd)
RELEASE_TARBALL         := manatee-pkg-$(STAMP).tar.bz2
TMPDIR                  := /tmp/$(STAMP)

.PHONY: setup
setup: | $(NPM_EXEC)
	$(NPM) install

.PHONY: release
release: setup deps docs $(SMF_MANIFESTS)
	@echo "Building $(RELEASE_TARBALL)"
	@mkdir -p $(TMPDIR)/root/opt/smartdc/manatee
	@mkdir -p $(TMPDIR)/site
	@touch $(TMPDIR)/site/.do-not-delete-me
	@mkdir -p $(TMPDIR)/root
	@mkdir -p $(TMPDIR)/root/opt/smartdc/manatee/ssl
	cp -r   $(ROOT)/build \
		$(ROOT)/lib \
		$(ROOT)/manatee.js \
		$(ROOT)/node_modules \
		$(ROOT)/package.json \
		$(ROOT)/smf \
		$(ROOT)/cfg \
		$(TMPDIR)/root/opt/smartdc/manatee/
	(cd $(TMPDIR) && $(TAR) -jcf $(ROOT)/$(RELEASE_TARBALL) root site)
	@rm -rf $(TMPDIR)

.PHONY: publish
publish: release
	@if [[ -z "$(BITS_DIR)" ]]; then \
		echo "error: 'BITS_DIR' must be set for 'publish' target"; \
		exit 1; \
	fi
	mkdir -p $(BITS_DIR)/manatee
	cp $(ROOT)/$(RELEASE_TARBALL) $(BITS_DIR)/manatee/$(RELEASE_TARBALL)
