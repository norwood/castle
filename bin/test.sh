#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

SCRIPT_DIR="$(dirname "$0")"
CASTLE_BIN="${SCRIPT_DIR}/castle.sh"
WAIT_BIN="${SCRIPT_DIR}/wait.sh"

die() {
    echo $@
    exit 1
}

[[ -f "${CASTLE_CLUSTER_INPUT_PATH}" ]] || \
    die "You must set CASTLE_CLUSTER_INPUT_PATH to the location of a spec file."
[[ -z "${CASTLE_WORKING_DIRECTORY}" ]] && \
    die "You must set CASTLE_WORKING_DIRECTORY to where you want the working directory to be."
export CASTLE_WORKING_DIRECTORY
export CASTLE_CLUSTER_INPUT_PATH

# Bring up the cluster
"${CASTLE_BIN}" ${@} -v up || \
    die "Failed to bring up the cluster."
unset CASTLE_CLUSTER_INPUT_PATH

# Wait for the test to finish.
WAIT_LOG="${CASTLE_WORKING_DIRECTORY}/wait.log"
"${WAIT_BIN}" ${@} taskStatus &> "${WAIT_LOG}" &
wait_bin_pid=$!
tail -f "${WAIT_LOG}" &
tail_pid=$!
wait ${wait_bin_pid}
wait_status=$?
kill ${tail_pid}

# Bring down the cluster.
"${CASTLE_BIN}" ${@} -v down || \
    die "Failed to bring down the cluster."
exit $wait_status
