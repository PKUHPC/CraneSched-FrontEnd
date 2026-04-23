#!/bin/bash
# =============================================================================
# Test Script: account/user-partition resource limit (PR #287)
# =============================================================================
# This script tests the partition-specific resource limit feature added in
# PR #287 (feat: Add [account/user-partition] resource limit).
#
# Features tested:
#   1. Set partition resource limits for accounts
#   2. Set partition resource limits for users
#   3. Show partition resource limits (--partition-limit / -P flag)
#   4. Error cases (missing partition in where clause)
#   5. Submit jobs and verify partition limits are enforced
#
# Prerequisites:
#   - CraneSched cluster is running
#   - cacctmgr, cbatch, cqueue, ccancel are in PATH
#   - Test account and user exist (or will be created by this script)
#   - At least one partition is available
#
# Usage:
#   bash test_partition_limit.sh [OPTIONS]
#
# Options:
#   -a <account>    Test account name (default: test_acc_limit)
#   -u <user>       Test user name (default: current user)
#   -p <partition>  Partition to test (default: auto-detect)
#   -c              Cleanup test account/user after test
#   -h              Show this help
# =============================================================================

set -euo pipefail

# ---- Default configuration ----
TEST_ACCOUNT="test_acc_limit"
TEST_USER="${USER:-$(whoami)}"
TEST_PARTITION=""
CLEANUP=false
PASS=0
FAIL=0
SKIP=0

# ---- Color output ----
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

log_info()  { echo -e "${BLUE}[INFO]${NC}  $*"; }
log_pass()  { echo -e "${GREEN}[PASS]${NC}  $*"; PASS=$((PASS+1)); }
log_fail()  { echo -e "${RED}[FAIL]${NC}  $*"; FAIL=$((FAIL+1)); }
log_skip()  { echo -e "${YELLOW}[SKIP]${NC}  $*"; SKIP=$((SKIP+1)); }
log_title() { echo -e "\n${BLUE}========== $* ==========${NC}"; }

# ---- Parse arguments ----
while getopts "a:u:p:ch" opt; do
    case $opt in
        a) TEST_ACCOUNT="$OPTARG" ;;
        u) TEST_USER="$OPTARG" ;;
        p) TEST_PARTITION="$OPTARG" ;;
        c) CLEANUP=true ;;
        h)
            sed -n '2,30p' "$0" | grep '^#' | sed 's/^# \?//'
            exit 0
            ;;
        *) echo "Unknown option: -$OPTARG"; exit 1 ;;
    esac
done

# ---- Check prerequisites ----
log_title "Checking Prerequisites"

for cmd in cacctmgr cbatch cqueue ccancel; do
    if ! command -v "$cmd" &>/dev/null; then
        log_fail "Command '$cmd' not found in PATH"
        exit 1
    fi
done
log_pass "All required commands found"

# Auto-detect partition if not specified
if [[ -z "$TEST_PARTITION" ]]; then
    TEST_PARTITION=$(cqueue --format="%P" --noheader 2>/dev/null | head -1 | tr -d ' ')
    if [[ -z "$TEST_PARTITION" ]]; then
        log_fail "Cannot auto-detect partition. Please specify with -p <partition>"
        exit 1
    fi
    log_info "Auto-detected partition: $TEST_PARTITION"
fi

log_info "Test account:   $TEST_ACCOUNT"
log_info "Test user:      $TEST_USER"
log_info "Test partition: $TEST_PARTITION"

# ---- Setup: Create test account and add user ----
log_title "Setup: Creating Test Account and User"

# Create test account (ignore error if already exists)
if cacctmgr add account "$TEST_ACCOUNT" partition="$TEST_PARTITION" 2>/dev/null; then
    log_info "Created test account: $TEST_ACCOUNT"
else
    log_info "Test account '$TEST_ACCOUNT' already exists (or creation failed, continuing)"
fi

# Add test user to account (ignore error if already exists)
if cacctmgr add user "$TEST_USER" account="$TEST_ACCOUNT" partition="$TEST_PARTITION" 2>/dev/null; then
    log_info "Added user '$TEST_USER' to account '$TEST_ACCOUNT'"
else
    log_info "User '$TEST_USER' already in account '$TEST_ACCOUNT' (or add failed, continuing)"
fi

# ---- Test Group 1: Modify account partition limits ----
log_title "Test Group 1: Set Account Partition Resource Limits"

# Test 1.1: Set maxJobs for account in partition
log_info "Test 1.1: Set maxJobs=5 for account '$TEST_ACCOUNT' in partition '$TEST_PARTITION'"
if cacctmgr modify account where name="$TEST_ACCOUNT" partition="$TEST_PARTITION" set maxJobs=5 2>&1; then
    log_pass "Set account maxJobs=5 succeeded"
else
    log_fail "Set account maxJobs=5 failed"
fi

# Test 1.2: Set maxSubmitJobs for account in partition
log_info "Test 1.2: Set maxSubmitJobs=10 for account '$TEST_ACCOUNT' in partition '$TEST_PARTITION'"
if cacctmgr modify account where name="$TEST_ACCOUNT" partition="$TEST_PARTITION" set maxSubmitJobs=10 2>&1; then
    log_pass "Set account maxSubmitJobs=10 succeeded"
else
    log_fail "Set account maxSubmitJobs=10 failed"
fi

# Test 1.3: Set maxTres for account in partition
log_info "Test 1.3: Set maxTres=cpu:8 for account '$TEST_ACCOUNT' in partition '$TEST_PARTITION'"
if cacctmgr modify account where name="$TEST_ACCOUNT" partition="$TEST_PARTITION" set maxTres=cpu:8 2>&1; then
    log_pass "Set account maxTres=cpu:8 succeeded"
else
    log_fail "Set account maxTres=cpu:8 failed"
fi

# Test 1.4: Set maxTresPerJob for account in partition
log_info "Test 1.4: Set maxTresPerJob=cpu:4 for account '$TEST_ACCOUNT' in partition '$TEST_PARTITION'"
if cacctmgr modify account where name="$TEST_ACCOUNT" partition="$TEST_PARTITION" set maxTresPerJob=cpu:4 2>&1; then
    log_pass "Set account maxTresPerJob=cpu:4 succeeded"
else
    log_fail "Set account maxTresPerJob=cpu:4 failed"
fi

# Test 1.5: Set maxWall for account in partition
log_info "Test 1.5: Set maxWall=3600 for account '$TEST_ACCOUNT' in partition '$TEST_PARTITION'"
if cacctmgr modify account where name="$TEST_ACCOUNT" partition="$TEST_PARTITION" set maxWall=3600 2>&1; then
    log_pass "Set account maxWall=3600 succeeded"
else
    log_fail "Set account maxWall=3600 failed"
fi

# Test 1.6: Set maxWallPerJob for account in partition
log_info "Test 1.6: Set maxWallPerJob=1800 for account '$TEST_ACCOUNT' in partition '$TEST_PARTITION'"
if cacctmgr modify account where name="$TEST_ACCOUNT" partition="$TEST_PARTITION" set maxWallPerJob=1800 2>&1; then
    log_pass "Set account maxWallPerJob=1800 succeeded"
else
    log_fail "Set account maxWallPerJob=1800 failed"
fi

# ---- Test Group 2: Modify user partition limits ----
log_title "Test Group 2: Set User Partition Resource Limits"

# Test 2.1: Set maxJobs for user in partition
log_info "Test 2.1: Set maxJobs=3 for user '$TEST_USER' in partition '$TEST_PARTITION'"
if cacctmgr modify user where name="$TEST_USER" account="$TEST_ACCOUNT" partition="$TEST_PARTITION" set maxJobs=3 2>&1; then
    log_pass "Set user maxJobs=3 succeeded"
else
    log_fail "Set user maxJobs=3 failed"
fi

# Test 2.2: Set maxSubmitJobs for user in partition
log_info "Test 2.2: Set maxSubmitJobs=6 for user '$TEST_USER' in partition '$TEST_PARTITION'"
if cacctmgr modify user where name="$TEST_USER" account="$TEST_ACCOUNT" partition="$TEST_PARTITION" set maxSubmitJobs=6 2>&1; then
    log_pass "Set user maxSubmitJobs=6 succeeded"
else
    log_fail "Set user maxSubmitJobs=6 failed"
fi

# Test 2.3: Set maxTres for user in partition
log_info "Test 2.3: Set maxTres=cpu:4 for user '$TEST_USER' in partition '$TEST_PARTITION'"
if cacctmgr modify user where name="$TEST_USER" account="$TEST_ACCOUNT" partition="$TEST_PARTITION" set maxTres=cpu:4 2>&1; then
    log_pass "Set user maxTres=cpu:4 succeeded"
else
    log_fail "Set user maxTres=cpu:4 failed"
fi

# Test 2.4: Set maxTresPerJob for user in partition
log_info "Test 2.4: Set maxTresPerJob=cpu:2 for user '$TEST_USER' in partition '$TEST_PARTITION'"
if cacctmgr modify user where name="$TEST_USER" account="$TEST_ACCOUNT" partition="$TEST_PARTITION" set maxTresPerJob=cpu:2 2>&1; then
    log_pass "Set user maxTresPerJob=cpu:2 succeeded"
else
    log_fail "Set user maxTresPerJob=cpu:2 failed"
fi

# Test 2.5: Set maxWall for user in partition
log_info "Test 2.5: Set maxWall=1800 for user '$TEST_USER' in partition '$TEST_PARTITION'"
if cacctmgr modify user where name="$TEST_USER" account="$TEST_ACCOUNT" partition="$TEST_PARTITION" set maxWall=1800 2>&1; then
    log_pass "Set user maxWall=1800 succeeded"
else
    log_fail "Set user maxWall=1800 failed"
fi

# Test 2.6: Set maxWallPerJob for user in partition
log_info "Test 2.6: Set maxWallPerJob=900 for user '$TEST_USER' in partition '$TEST_PARTITION'"
if cacctmgr modify user where name="$TEST_USER" account="$TEST_ACCOUNT" partition="$TEST_PARTITION" set maxWallPerJob=900 2>&1; then
    log_pass "Set user maxWallPerJob=900 succeeded"
else
    log_fail "Set user maxWallPerJob=900 failed"
fi

# ---- Test Group 3: Show partition limits ----
log_title "Test Group 3: Show Partition Resource Limits"

# Test 3.1: Show account partition limits with --partition-limit
log_info "Test 3.1: Show account partition limits (--partition-limit)"
OUTPUT=$(cacctmgr show account "$TEST_ACCOUNT" --partition-limit 2>&1)
echo "$OUTPUT"
if echo "$OUTPUT" | grep -q "$TEST_PARTITION"; then
    log_pass "Account partition limit table shown with --partition-limit"
else
    log_fail "Account partition limit table not shown or partition not found in output"
fi

# Test 3.2: Show account partition limits with -P (short form)
log_info "Test 3.2: Show account partition limits (-P short form)"
OUTPUT=$(cacctmgr show account "$TEST_ACCOUNT" -P 2>&1)
echo "$OUTPUT"
if echo "$OUTPUT" | grep -q "$TEST_PARTITION"; then
    log_pass "Account partition limit table shown with -P"
else
    log_fail "Account partition limit table not shown with -P"
fi

# Test 3.3: Show user partition limits with --partition-limit
log_info "Test 3.3: Show user partition limits (--partition-limit)"
OUTPUT=$(cacctmgr show user "$TEST_USER" --partition-limit 2>&1)
echo "$OUTPUT"
if echo "$OUTPUT" | grep -q "$TEST_PARTITION"; then
    log_pass "User partition limit table shown with --partition-limit"
else
    log_fail "User partition limit table not shown or partition not found in output"
fi

# Test 3.4: Show all accounts with partition limits
log_info "Test 3.4: Show all accounts with partition limits"
OUTPUT=$(cacctmgr show account --partition-limit 2>&1)
echo "$OUTPUT"
if echo "$OUTPUT" | grep -q "$TEST_ACCOUNT"; then
    log_pass "All accounts partition limit table shown"
else
    log_fail "Test account not found in all accounts partition limit output"
fi

# Test 3.5: Show all users with partition limits
log_info "Test 3.5: Show all users with partition limits"
OUTPUT=$(cacctmgr show user --partition-limit 2>&1)
echo "$OUTPUT"
if echo "$OUTPUT" | grep -q "$TEST_USER"; then
    log_pass "All users partition limit table shown"
else
    log_fail "Test user not found in all users partition limit output"
fi

# ---- Test Group 4: Error cases ----
log_title "Test Group 4: Error Cases"

# Test 4.1: Set maxJobs without specifying partition (should fail)
log_info "Test 4.1: Set maxJobs without partition in where clause (should fail)"
if cacctmgr modify account where name="$TEST_ACCOUNT" set maxJobs=5 2>&1 | grep -qi "partition"; then
    log_pass "Correctly rejected: maxJobs requires partition in where clause"
else
    log_fail "Should have rejected maxJobs without partition specification"
fi

# Test 4.2: Set maxSubmitJobs for user without partition (should fail)
log_info "Test 4.2: Set maxSubmitJobs for user without partition (should fail)"
if cacctmgr modify user where name="$TEST_USER" set maxSubmitJobs=5 2>&1 | grep -qi "partition"; then
    log_pass "Correctly rejected: maxSubmitJobs requires partition in where clause"
else
    log_fail "Should have rejected maxSubmitJobs without partition specification"
fi

# Test 4.3: Set maxTresPerJob without partition (should fail)
log_info "Test 4.3: Set maxTresPerJob without partition (should fail)"
if cacctmgr modify account where name="$TEST_ACCOUNT" set maxTresPerJob=cpu:2 2>&1 | grep -qi "partition"; then
    log_pass "Correctly rejected: maxTresPerJob requires partition in where clause"
else
    log_fail "Should have rejected maxTresPerJob without partition specification"
fi

# Test 4.4: Set maxWallPerJob without partition (should fail)
log_info "Test 4.4: Set maxWallPerJob without partition (should fail)"
if cacctmgr modify user where name="$TEST_USER" set maxWallPerJob=900 2>&1 | grep -qi "partition"; then
    log_pass "Correctly rejected: maxWallPerJob requires partition in where clause"
else
    log_fail "Should have rejected maxWallPerJob without partition specification"
fi

# ---- Test Group 5: Submit jobs and verify limits ----
log_title "Test Group 5: Submit Jobs and Verify Partition Limits"

# Set a tight limit: maxSubmitJobs=2 for user in partition
log_info "Setting maxSubmitJobs=2 for user '$TEST_USER' in partition '$TEST_PARTITION'"
cacctmgr modify user where name="$TEST_USER" account="$TEST_ACCOUNT" partition="$TEST_PARTITION" set maxSubmitJobs=2 2>&1 || true

# Create a simple test job script
TMPJOB=$(mktemp /tmp/test_partition_limit_XXXXXX.sh)
cat > "$TMPJOB" <<'JOBEOF'
#!/bin/bash
#SBATCH --time=00:01:00
sleep 60
JOBEOF

log_info "Test 5.1: Submit jobs up to the limit"
JOB_IDS=()

# Submit first job (should succeed)
JOB1_OUTPUT=$(cbatch --partition="$TEST_PARTITION" --account="$TEST_ACCOUNT" "$TMPJOB" 2>&1)
if echo "$JOB1_OUTPUT" | grep -qi "job\|submitted\|id"; then
    JOB1_ID=$(echo "$JOB1_OUTPUT" | grep -oP '\d+' | head -1)
    JOB_IDS+=("$JOB1_ID")
    log_pass "First job submitted successfully (ID: $JOB1_ID)"
else
    log_fail "First job submission failed: $JOB1_OUTPUT"
fi

# Submit second job (should succeed)
JOB2_OUTPUT=$(cbatch --partition="$TEST_PARTITION" --account="$TEST_ACCOUNT" "$TMPJOB" 2>&1)
if echo "$JOB2_OUTPUT" | grep -qi "job\|submitted\|id"; then
    JOB2_ID=$(echo "$JOB2_OUTPUT" | grep -oP '\d+' | head -1)
    JOB_IDS+=("$JOB2_ID")
    log_pass "Second job submitted successfully (ID: $JOB2_ID)"
else
    log_fail "Second job submission failed: $JOB2_OUTPUT"
fi

# Submit third job (should fail due to maxSubmitJobs=2 limit)
log_info "Test 5.2: Submit job exceeding maxSubmitJobs limit (should be rejected)"
JOB3_OUTPUT=$(cbatch --partition="$TEST_PARTITION" --account="$TEST_ACCOUNT" "$TMPJOB" 2>&1)
if echo "$JOB3_OUTPUT" | grep -qi "limit\|exceed\|denied\|error\|fail"; then
    log_pass "Third job correctly rejected due to maxSubmitJobs limit"
else
    # If it succeeded, add to cleanup list
    JOB3_ID=$(echo "$JOB3_OUTPUT" | grep -oP '\d+' | head -1)
    if [[ -n "$JOB3_ID" ]]; then
        JOB_IDS+=("$JOB3_ID")
    fi
    log_fail "Third job should have been rejected (maxSubmitJobs=2 limit), but got: $JOB3_OUTPUT"
fi

# Cleanup submitted jobs
log_info "Cleaning up submitted test jobs..."
for jid in "${JOB_IDS[@]}"; do
    ccancel "$jid" 2>/dev/null || true
done
rm -f "$TMPJOB"

# ---- Test Group 6: Reset limits ----
log_title "Test Group 6: Reset Partition Limits"

# Reset maxSubmitJobs to unlimited (large value)
log_info "Test 6.1: Reset user maxSubmitJobs to unlimited"
if cacctmgr modify user where name="$TEST_USER" account="$TEST_ACCOUNT" partition="$TEST_PARTITION" set maxSubmitJobs=4294967295 2>&1; then
    log_pass "Reset user maxSubmitJobs to unlimited"
else
    log_fail "Failed to reset user maxSubmitJobs"
fi

# ---- Cleanup ----
if $CLEANUP; then
    log_title "Cleanup"
    log_info "Removing test user from account..."
    cacctmgr delete user "$TEST_USER" account="$TEST_ACCOUNT" 2>/dev/null || true
    log_info "Removing test account..."
    cacctmgr delete account "$TEST_ACCOUNT" 2>/dev/null || true
    log_pass "Cleanup completed"
fi

# ---- Summary ----
log_title "Test Summary"
TOTAL=$((PASS + FAIL + SKIP))
echo -e "Total:  $TOTAL"
echo -e "${GREEN}Passed: $PASS${NC}"
echo -e "${RED}Failed: $FAIL${NC}"
echo -e "${YELLOW}Skipped: $SKIP${NC}"

if [[ $FAIL -eq 0 ]]; then
    echo -e "\n${GREEN}All tests passed!${NC}"
    exit 0
else
    echo -e "\n${RED}$FAIL test(s) failed!${NC}"
    exit 1
fi
