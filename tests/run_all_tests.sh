#!/bin/bash

# Test Runner Script
# Runs all test suites in order with specified wait times between them
# Collects results in a structured format

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
RESULTS_DIR="$SCRIPT_DIR/test_results"
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
SUMMARY_FILE="$RESULTS_DIR/summary_$TIMESTAMP.txt"
RAW_OUTPUT="$RESULTS_DIR/raw_output_$TIMESTAMP.log"

# Colors for terminal output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Totals
TOTAL_PASSED=0
TOTAL_FAILED=0
TOTAL_ERRORS=0
TOTAL_SKIPPED=0

# Create results directory
mkdir -p "$RESULTS_DIR"

# Temp file to collect suite results
SUITE_RESULTS="$RESULTS_DIR/.suite_results_$TIMESTAMP"
> "$SUITE_RESULTS"

# Initialize summary file
{
    echo "========================================"
    echo "TEST RUN SUMMARY"
    echo "Started: $(date)"
    echo "========================================"
    echo ""
} > "$SUMMARY_FILE"

# Initialize raw output file
{
    echo "Raw test output - $(date)"
    echo "========================================"
} > "$RAW_OUTPUT"

parse_junit_xml() {
    local xml_file="$1"
    local suite_name="$2"

    local passed=0 failed=0 errors=0 skipped=0 tests=0

    if [[ -f "$xml_file" ]]; then
        local testsuite_line
        testsuite_line=$(grep -m1 '<testsuite' "$xml_file" 2>/dev/null || echo "")

        if [[ -n "$testsuite_line" ]]; then
            tests=$(echo "$testsuite_line" | sed -n 's/.*tests="\([0-9]*\)".*/\1/p')
            errors=$(echo "$testsuite_line" | sed -n 's/.*errors="\([0-9]*\)".*/\1/p')
            failed=$(echo "$testsuite_line" | sed -n 's/.*failures="\([0-9]*\)".*/\1/p')
            skipped=$(echo "$testsuite_line" | sed -n 's/.*skipped="\([0-9]*\)".*/\1/p')

            tests=${tests:-0}
            errors=${errors:-0}
            failed=${failed:-0}
            skipped=${skipped:-0}

            passed=$((tests - errors - failed - skipped))
        fi
    fi

    echo "$passed $failed $errors $skipped"
}

extract_failures() {
    local xml_file="$1"
    local suite_name="$2"

    if [[ ! -f "$xml_file" ]]; then
        return
    fi

    # Extract failing test names and messages
    local current_test=""
    while IFS= read -r line; do
        # Match testcase name
        if echo "$line" | grep -q '<testcase'; then
            current_test=$(echo "$line" | sed -n 's/.*name="\([^"]*\)".*/\1/p')
        fi

        # Match failure or error with message
        if echo "$line" | grep -q '<failure\|<error'; then
            local msg
            msg=$(echo "$line" | sed -n 's/.*message="\([^"]*\)".*/\1/p')
            echo "  - $current_test" >> "$SUMMARY_FILE"
            if [[ -n "$msg" ]]; then
                # Truncate long messages
                msg=$(echo "$msg" | head -c 200)
                echo "    Message: $msg" >> "$SUMMARY_FILE"
            fi
            echo "" >> "$SUMMARY_FILE"
        fi
    done < "$xml_file"
}

run_test_suite() {
    local suite_name="$1"
    local test_path="$2"
    local wait_after="$3"
    local xml_file="$RESULTS_DIR/${suite_name}_$TIMESTAMP.xml"

    echo -e "${YELLOW}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${YELLOW}Running: $suite_name${NC}"
    echo -e "${YELLOW}Path: $test_path${NC}"
    echo -e "${YELLOW}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"

    # Log to raw output
    {
        echo ""
        echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
        echo "Suite: $suite_name"
        echo "Started: $(date)"
        echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    } >> "$RAW_OUTPUT"

    cd "$PROJECT_ROOT"

    # Run pytest with JUnit XML output
    local exit_code=0
    uv run pytest "$test_path" \
        --tb=short \
        --junitxml="$xml_file" \
        -q 2>&1 | tee -a "$RAW_OUTPUT" || exit_code=$?

    # Parse results
    local results
    results=$(parse_junit_xml "$xml_file" "$suite_name")
    local passed failed errors skipped
    read -r passed failed errors skipped <<< "$results"

    # Update totals
    TOTAL_PASSED=$((TOTAL_PASSED + passed))
    TOTAL_FAILED=$((TOTAL_FAILED + failed))
    TOTAL_ERRORS=$((TOTAL_ERRORS + errors))
    TOTAL_SKIPPED=$((TOTAL_SKIPPED + skipped))

    # Determine status and store result
    local status
    if [[ $exit_code -eq 0 ]]; then
        status="PASSED"
        echo -e "${GREEN}✓ $suite_name: $passed passed, $skipped skipped${NC}"
    else
        status="FAILED"
        echo -e "${RED}✗ $suite_name: $passed passed, $failed failed, $errors errors, $skipped skipped${NC}"
    fi

    # Store suite result for final summary
    echo "$suite_name|$status|$passed|$failed|$errors|$skipped" >> "$SUITE_RESULTS"

    # Wait before next suite (if not last)
    if [[ $wait_after -gt 0 ]]; then
        echo -e "${YELLOW}Waiting ${wait_after}s before next suite...${NC}"
        sleep "$wait_after"
    fi

    return $exit_code
}

# Kill orphaned worker processes left over from previous/aborted test runs.
# Workers started with start_new_session=True survive parent death, and can
# claim tasks or hold DB connections that break subsequent layers.
kill_orphaned_workers() {
    local count
    count=$(pgrep -f "horsies worker" 2>/dev/null | wc -l | tr -d ' ')
    if [[ "$count" -gt 0 ]]; then
        echo -e "${YELLOW}Stopping $count orphaned horsies worker process(es)...${NC}"
        # Graceful SIGTERM first — lets workers close DB connections
        pkill -TERM -f "horsies worker" 2>/dev/null || true
        for i in 1 2 3; do
            count=$(pgrep -f "horsies worker" 2>/dev/null | wc -l | tr -d ' ')
            if [[ "$count" -eq 0 ]]; then
                return
            fi
            sleep 1
        done
        # Force SIGKILL if SIGTERM didn't work
        echo -e "${YELLOW}Force-killing $count remaining worker(s)...${NC}"
        pkill -9 -f "horsies worker" 2>/dev/null || true
        for i in 1 2; do
            count=$(pgrep -f "horsies worker" 2>/dev/null | wc -l | tr -d ' ')
            if [[ "$count" -eq 0 ]]; then
                return
            fi
            sleep 1
        done
        echo -e "${RED}Warning: $count worker(s) still alive after kill${NC}"
    fi
}

# Main execution
echo -e "${GREEN}Starting test run at $(date)${NC}"
echo ""

OVERALL_EXIT_CODE=0

# Clean slate: kill any workers left from previous runs
kill_orphaned_workers

# Run test suites in order
run_test_suite "unit" "tests/unit" 4 || OVERALL_EXIT_CODE=1
run_test_suite "integration" "tests/integration" 10 || OVERALL_EXIT_CODE=1
kill_orphaned_workers
run_test_suite "e2e_layer1" "tests/e2e/test_layer1_tasks.py" 10 || OVERALL_EXIT_CODE=1
kill_orphaned_workers
run_test_suite "e2e_layer2" "tests/e2e/test_layer2_cluster.py" 10 || OVERALL_EXIT_CODE=1
kill_orphaned_workers
run_test_suite "e2e_layer3" "tests/e2e/test_layer3_scheduler.py" 10 || OVERALL_EXIT_CODE=1
kill_orphaned_workers
run_test_suite "e2e_layer4" "tests/e2e/test_layer4_workflow_structure.py" 10 || OVERALL_EXIT_CODE=1
kill_orphaned_workers
run_test_suite "e2e_layer5" "tests/e2e/test_layer5_workflow_dataflow.py" 10 || OVERALL_EXIT_CODE=1
kill_orphaned_workers
run_test_suite "e2e_layer6" "tests/e2e/test_layer6_workflow_advanced.py" 10 || OVERALL_EXIT_CODE=1
kill_orphaned_workers
run_test_suite "e2e_fanout" "tests/e2e/test_fanout_saturation.py" 0 || OVERALL_EXIT_CODE=1

# Generate final summary
{
    echo ""
    echo "========================================"
    echo "RESULTS BY SUITE"
    echo "========================================"
} >> "$SUMMARY_FILE"

while IFS='|' read -r name status passed failed errors skipped; do
    printf "%-20s %s (passed: %d, failed: %d, errors: %d, skipped: %d)\n" \
        "$name:" "$status" "$passed" "$failed" "$errors" "$skipped" >> "$SUMMARY_FILE"
done < "$SUITE_RESULTS"

{
    echo ""
    echo "========================================"
    echo "TOTALS"
    echo "========================================"
    echo "Passed:  $TOTAL_PASSED"
    echo "Failed:  $TOTAL_FAILED"
    echo "Errors:  $TOTAL_ERRORS"
    echo "Skipped: $TOTAL_SKIPPED"
    echo ""
} >> "$SUMMARY_FILE"

# Extract and list failures if any
if [[ $TOTAL_FAILED -gt 0 || $TOTAL_ERRORS -gt 0 ]]; then
    {
        echo "========================================"
        echo "FAILING TESTS"
        echo "========================================"
    } >> "$SUMMARY_FILE"

    while IFS='|' read -r name status passed failed errors skipped; do
        if [[ $failed -gt 0 || $errors -gt 0 ]]; then
            echo "" >> "$SUMMARY_FILE"
            echo "[$name]" >> "$SUMMARY_FILE"
            extract_failures "$RESULTS_DIR/${name}_$TIMESTAMP.xml" "$name"
        fi
    done < "$SUITE_RESULTS"
fi

echo "Completed: $(date)" >> "$SUMMARY_FILE"

# Cleanup temp file
rm -f "$SUITE_RESULTS"

# Print final summary to terminal
echo ""
echo -e "${YELLOW}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${YELLOW}FINAL SUMMARY${NC}"
echo -e "${YELLOW}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
cat "$SUMMARY_FILE"
echo ""
echo -e "Detailed summary: ${GREEN}$SUMMARY_FILE${NC}"
echo -e "Raw output: ${GREEN}$RAW_OUTPUT${NC}"
echo -e "XML reports: ${GREEN}$RESULTS_DIR/*.xml${NC}"

exit $OVERALL_EXIT_CODE
