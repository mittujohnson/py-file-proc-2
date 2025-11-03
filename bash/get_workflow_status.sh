#!/bin/bash

# --- Configuration Constants ---
CHECK_INTERVAL_SECONDS=60 # Check status every 60 seconds (1 minute)
TIMEOUT_SECONDS=3600      # 1 hour timeout (60 minutes * 60 seconds)

# --- List of Final/Terminal Statuses ---
# Add any status that means the workflow is no longer 'Running'
TERMINAL_STATUSES=("Succeeded" "Failed" "Stopped" "Terminated" "Aborted") 

# ==============================================================================
# 🕵️ Function: get_workflow_status (Re-used from previous response)
# Executes 'pmcmd getworkflowdetails' and returns the workflow run status.
# NOTE: Replace '/path/to/pmcmd' with your actual path.
# ==============================================================================
get_workflow_status() {
    # Arguments: $1: Int_Service, $2: Domain, $3: Repository, $4: Folder, $5: Workflow
    local PMCMD_OUTPUT
    PMCMD_OUTPUT=$(/path/to/pmcmd getworkflowdetails \
        -sv "$1" \
        -d "$2" \
        -r "$3" \
        -f "$4" \
        "$5" 2>&1)
        
    local exit_status=$?

    if [ "$exit_status" -ne 0 ]; then
        echo "Error: PMCMD failed to connect or run getworkflowdetails." >&2
        return 2 # Non-zero return for command failure
    fi

    # Parse the Workflow Status
    local status_string
    status_string=$(echo "$PMCMD_OUTPUT" | grep "Workflow run status:" | awk '{print $NF}')

    if [ -z "$status_string" ]; then
        echo "Unknown: Status line not found." >&2
        return 3 # Non-zero return for parsing failure
    fi

    echo "$status_string"
    return 0
}

# ==============================================================================
# ⏱️ Function: wait_for_workflow_completion
# Loops to check status until completion or timeout.
#
# Arguments:
# $1: Integration Service Name
# $2: Domain Name
# $3: Repository Name
# $4: Folder Name
# $5: Workflow Name
# ==============================================================================
wait_for_workflow_completion() {
    local int_svc="$1"
    local domain_name="$2"
    local repo_name="$3"
    local folder_name="$4"
    local workflow_name="$5"
    
    local start_time=$(date +%s)
    local current_time
    local elapsed_time=0
    local current_status

    echo "--- Initiating wait loop for ${workflow_name} ---"
    echo "Timeout set to ${TIMEOUT_SECONDS} seconds."
    echo "Check interval: ${CHECK_INTERVAL_SECONDS} seconds."

    while true; do
        # Calculate elapsed time
        current_time=$(date +%s)
        elapsed_time=$((current_time - start_time))

        # Check for timeout
        if [ "$elapsed_time" -ge "$TIMEOUT_SECONDS" ]; then
            echo "❌ ERROR: Timeout reached (${TIMEOUT_SECONDS}s). Workflow is still RUNNING." >&2
            return 1 # Indicate timeout failure
        fi

        # Get the current status
        current_status=$(get_workflow_status "$int_svc" "$domain_name" "$repo_name" "$folder_name" "$workflow_name")
        local status_check_code=$?
        
        # Check if the get_workflow_status function itself failed
        if [ "$status_check_code" -ne 0 ]; then
            echo "🚨 Critical error occurred while checking status. Aborting loop." >&2
            return 2 # Indicate check failure
        fi

        echo "Status at T+${elapsed_time}s: ${current_status}..."

        # Check if status is a terminal state
        for term_status in "${TERMINAL_STATUSES[@]}"; do
            if [ "$current_status" == "$term_status" ]; then
                echo "✅ Workflow has reached terminal status: ${current_status}."
                
                if [ "$current_status" == "Succeeded" ]; then
                    return 0 # Success
                else
                    return 3 # Non-successful terminal status (Failed, Stopped, etc.)
                fi
            fi
        done
        
        # If still running, wait for the interval before checking again
        sleep $CHECK_INTERVAL_SECONDS
    done
}


# ==============================================================================
# 🏁 Main Script Execution Example (Integration)
# ==============================================================================

# 💡 Setup your workflow details and run the workflow first (using the startWorkflow 
#    function from the previous answer, or assume it's running).

INTEGRATION_SVC="INT_SVC_PROD"
DOMAIN="Domain_Dev"
REPOSITORY="REPO_DEV"
FOLDER="MyDataProject"
WORKFLOW_TO_CHECK="wf_daily_etl"

# Assuming the workflow is already RUNNING or just started...

wait_for_workflow_completion \
    "${INTEGRATION_SVC}" \
    "${DOMAIN}" \
    "${REPOSITORY}" \
    "${FOLDER}" \
    "${WORKFLOW_TO_CHECK}"

FINAL_WAIT_STATUS=$?

echo "========================================================"
if [ "$FINAL_WAIT_STATUS" -eq 0 ]; then
    echo "🟢 Workflow completed successfully within the timeout!"
elif [ "$FINAL_WAIT_STATUS" -eq 1 ]; then
    echo "🔴 ERROR: Workflow TIMED OUT after 1 hour."
elif [ "$FINAL_WAIT_STATUS" -eq 3 ]; then
    echo "🔴 ERROR: Workflow completed with a failure status (Failed/Stopped/Aborted)."
else
    echo "🚨 Critical Script Error (Exit Code: $FINAL_WAIT_STATUS)."
fi
echo "========================================================"