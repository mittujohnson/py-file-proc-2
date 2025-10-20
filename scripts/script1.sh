#!/bin/bash

# --- Configuration ---
LOG_FILE="/tmp/process_data_$(date +%Y%m%d_%H%M%S).log"

# Input Parameters (Passed from Python)
SIMULATE_STATUS="$1"  # e.g., 'success', 'fail'
FILENAME="$2"         # e.g., 'orders_q3_2025.csv'
THRESHOLD_VALUE="$3"  # e.g., '1000'

# Function for logging output with a timestamp
log() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

# Function to handle errors and exit
error_exit() {
    log "CRITICAL ERROR: $1"
    log "Process failed. Check log: $LOG_FILE"
    exit 1
}

# --- Main Workflow ---
log "START: Data Processing Initiated."
log "Parameters Received: STATUS=$SIMULATE_STATUS, FILE=$FILENAME, THRESHOLD=$THRESHOLD_VALUE"
log "Log output will be saved to: $LOG_FILE"

# Exit immediately if any command fails.
set -e

# 1. Input Validation and Argument Check
log "STEP 1: Validating all required inputs..."
if [ -z "$SIMULATE_STATUS" ] || [ -z "$FILENAME" ] || [ -z "$THRESHOLD_VALUE" ]; then
    error_exit "One or more required arguments (status, filename, or threshold) is missing."
fi

# 2. Simulate File Existence Check
log "STEP 2: Checking file existence ($FILENAME)..."
if [ ! -f "/data/$FILENAME" ]; then # Assuming files are expected in a /data directory
    error_exit "Input file not found at expected path: /data/$FILENAME."
fi

# 3. Simulate Core Processing Logic with Threshold Check
log "STEP 3: Processing data in $FILENAME..."
if [ "$SIMULATE_STATUS" == "success" ]; then
    # Success Path: Simulate a check that passes
    log "INFO: Data processed. Applying threshold check..."
    
    # Example Logic: Checking if threshold is met (Simulated)
    if [ "$THRESHOLD_VALUE" -lt 500 ]; then
        log "WARNING: Threshold is too low ($THRESHOLD_VALUE). Processing halted gracefully."
        echo "WARNING: Check threshold value before rerunning." >&2 # Send warning to STDERR
        exit 3 # Use a specific exit code for application logic failure (different from system failure)
    fi

    log "STATUS: Processing complete. Final result generated."
    echo "SUCCESS: $FILENAME processed. Threshold of $THRESHOLD_VALUE was valid." # STDOUT for Python
    
else
    # Failure Path: Simulate a critical system failure
    log "FATAL: System error simulated during processing phase."
    echo "FAILURE DETAILS: Connection to database lost while processing $FILENAME." >&2
    exit 1 # Use exit 1 for system/critical error
fi

# 4. Final Cleanup and Exit
log "END: Process finished successfully (or exited with error code)."
exit 0 # Should only be reached on a 'success' path without simulation errors