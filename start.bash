#!/bin/bash

# start.bash
#
# This source file is part of the FoundationDB open source project
#
# Copyright 2013-2022 Apple Inc. and the FoundationDB project authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http:#www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

set -eu;

FDB_CLUSTER_FILE="${FDB_CLUSTER_FILE:-/etc/foundationdb/fdb.cluster}"
coordinator_ip=$(dig +short "$FDB_COORDINATOR")
coordinator_port=${FDB_COORDINATOR_PORT:-4500}
echo "docker:docker@$coordinator_ip:$coordinator_port" > "$FDB_CLUSTER_FILE"

# Attempt to connect. Configure the database if necessary.
if ! /usr/bin/fdbcli -C $FDB_CLUSTER_FILE --exec status --timeout 3 ; then
    echo "creating the database"
    if ! fdbcli -C $FDB_CLUSTER_FILE --exec "configure new single memory ; status" --timeout 10 ; then
        echo "Unable to configure new FDB cluster."
        exit 1
    fi
fi

tail -f /dev/null
