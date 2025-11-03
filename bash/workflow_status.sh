#!/bin/bash

# ==============================================================================
# 🕵️ Function: get_workflow_status
# Executes 'pmcmd getworkflowdetails' and returns the workflow run status.
#
# Arguments:
# $1: Integration Service Name
# $2: Domain Name
# $3: Repository Name
# $4: Folder Name
# $5: Workflow Name
#
# Returns: The workflow status string (e.g., 'Running', 'Succeeded', 'Failed'), 
#          or 'Error' if the command fails.
# ==============================================================================
get_workflow_status() {
    local int_svc="$1"
    local domain_name="$2"
    local repo_name="$3"
    local folder_name="$4"
    local workflow_name="$5"
    
    # --- 1. Basic Argument Check ---
    if [ "$#" -ne 5 ]; then
        echo "❌ ERROR: get_workflow_status requires 5 arguments." >&2
        echo "Usage: get_workflow_status <Int_Service> <Domain> <Repository> <Folder> <Workflow>" >&2
        return 1
    fi
    
    echo "Checking status for workflow: ${workflow_name}..."

    # --- 2. Execute pmcmd getworkflowdetails and capture output ---
    # The output typically includes a line like: "Workflow run status: Succeeded"
    local PMCMD_OUTPUT
    PMCMD_OUTPUT=$(/path/to/pmcmd **getworkflowdetails** \
        -sv **"${int_svc}"** \
        -d **"${domain_name}"** \
        -r **"${repo_name}"** \
        -f **"${folder_name}"** \
        "${workflow_name}" 2>&1) # Redirect stderr to stdout for full capture
        
    local exit_status=$?

    # --- 3. Handle Command Failure (e.g., connection issue, bad parameters) ---
    if [ "$exit_status" -ne 0 ]; then
        echo "🚨 PMCMD failed with exit code $exit_status. Cannot retrieve status." >&2
        # Optionally, log the full output: echo "$PMCMD_OUTPUT"
        echo "Error"
        return 2
    fi

    # --- 4. Parse the Workflow Status ---
    # Find the line containing "Workflow run status:", and use awk to print the last field.
    local status_string
    status_string=$(echo "$PMCMD_OUTPUT" | grep "Workflow run status:" | awk '{print $NF}')

    # Check if status was found (in case output format changed)
    if [ -z "$status_string" ]; then
        echo "⚠️ WARNING: Status line not found in pmcmd output." >&2
        echo "Unknown"
        return 3
    fi

    echo "$status_string"
    return 0
}

# ==============================================================================
# 🏁 Main Script Usage Example
# ==============================================================================

# 💡 Replace these placeholder values with your actual Informatica details
INTEGRATION_SVC="INT_SVC_PROD"
DOMAIN="Domain_Dev"
REPOSITORY="REPO_DEV"
FOLDER="MyDataProject"
WORKFLOW_TO_CHECK="wf_daily_etl"

# --- Call the function and capture the status ---
WORKFLOW_STATUS=$(get_workflow_status \
    "${INTEGRATION_SVC}" \
    "${DOMAIN}" \
    "${REPOSITORY}" \
    "${FOLDER}" \
    "${WORKFLOW_TO_CHECK}")

# --- Decision Logic based on the captured status ---
echo "----------------------------------------"
echo "Retrieved Status: ${WORKFLOW_STATUS}"
echo "----------------------------------------"

case "${WORKFLOW_STATUS}" in
    "Running")
        echo "🔵 The workflow is currently ${WORKFLOW_STATUS}. Will check again later."
        # Add logic to wait or exit
        ;;
    "Succeeded")
        echo "🟢 The workflow ${WORKFLOW_STATUS} successfully. Proceeding to next step."
        # Add success logic
        ;;
    "Failed"|"Stopped"|"Terminated")
        echo "🔴 The workflow ${WORKFLOW_STATUS}. INVESTIGATION REQUIRED!" >&2
        # Add error handling (e.g., sending an alert)
        exit 1
        ;;
    "Unknown"|"Error")
        echo "❓ Cannot determine status or PMCMD failed. Check connection details." >&2
        exit 2
        ;;
    *)
        echo "🟡 Received an unexpected status: ${WORKFLOW_STATUS}"
        ;;
esac