#!/bin/bash
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#

#
# Copyright (c) 2015, Joyent, Inc.
#

#
# manatee-specific catest customizations
#
function catest_init
{
	[[ -z "$ZK_IPS" ]] && fail "ZK_IPS must be set in the environment"
	[[ -z "$SHARD" ]]  && fail "SHARD must be set in the environment"
	return 0
}
