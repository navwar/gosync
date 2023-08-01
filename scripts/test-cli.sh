#!/bin/bash

# ==================================================================================
#
# Work of the U.S. Department of the Navy, Naval Information Warfare Center Pacific.
# Released as open source under the MIT License.  See LICENSE file.
#
# ==================================================================================

set -euo pipefail

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

export testdata_local="${DIR}/../testdata"

export temp="${DIR}/../temp"

testCompletionBash() {
  "${DIR}/../bin/gosync" completion bash > /dev/null
}

testCompletionPowershell() {
  "${DIR}/../bin/gosync" completion powershell > /dev/null
}

testSchemes() {
  "${DIR}/../bin/gosync" schemes
}

testSync() {
  "${DIR}/../bin/gosync" sync --parents "${testdata_local}" "${temp}"
}

testDelete() {
  "${DIR}/../bin/gosync" sync --delete "${testdata_local}" "${temp}"
}

testVersion() {
  local expected='0.0.1'
  local output=$("${DIR}/../bin/gosync" version)
  assertEquals "unexpected output" "${expected}" "${output}"
}

oneTimeSetUp() {
  echo "Using temporary directory at ${SHUNIT_TMPDIR}"
  echo "Reading testdata from ${testdata_local}"
}

oneTimeTearDown() {
  echo "Tearing Down"
}

# Load shUnit2.
. "${DIR}/shunit2"
