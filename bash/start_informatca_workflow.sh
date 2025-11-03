#!/bin/bash

# ==============================================================================
# 🚀 Function: run_informatica_workflow
# Executes an Informatica PowerCenter workflow using the 'pmcmd' utility.
#
# Arguments:
# $1: Integration Service Name (e.g., 'INT_SVC_PROD')
# $2: Domain Name (e.g., 'Domain_1')
# $3: Repository Name (e.g., 'REPO_PROD')
# $4: Folder Name (e.g., 'MyProject')
# $5: Workflow Name (e.g., 'wf_load_data')
# ==============================================================================
run_informatica_workflow() {
    local int_svc="$1"
    local domain_name="$2"
    local repo_name="$3"
    local folder_name="$4"
    local workflow_name="$5"
    
    # --- 1. Basic Argument Check ---
    if [ "$#" -ne 5 ]; then
        echo "❌ ERROR: run_informatica_workflow requires 5 arguments." >&2
        echo "Usage: run_informatica_workflow <Int_Service> <Domain> <Repository> <Folder> <Workflow>" >&2
        return 1
    fi

    echo "========================================================"
    echo "Starting Informatica Workflow: ${workflow_name}..."
    echo "Service: ${int_svc} | Domain: ${domain_name} | Folder: ${folder_name}"
    echo "========================================================"

    # NOTE: pmcmd command details:
    # 1. Provide the correct path to pmcmd if it's not in your PATH.
    # 2. Add -u (user) and -p (password) arguments here if authentication is needed,
    #    or ensure the script is run by a user with environment variables or trusted authentication setup.

    # --- 2. Execute pmcmd startWorkflow Command ---
    /path/to/pmcmd **startWorkflow** \
        -sv **"${int_svc}"** \
        -d **"${domain_name}"** \
        -r **"${repo_name}"** \
        -f **"${folder_name}"** \
        "${workflow_name}"
        
    local exit_status=$?

    # --- 3. Handle Status ---
    if [ "$exit_status" -eq 0 ]; then
        echo "✅ SUCCESS: Workflow ${workflow_name} started successfully (pmcmd exited with 0)."
    else
        echo "❌ FAILURE: Workflow ${workflow_name} failed to start or encountered an immediate error." >&2
        echo "   pmcmd Exit Code: ${exit_status}" >&2
        # Return the failure status for the main script to handle
        return $exit_status
    fi
    
    return 0
}

# ==============================================================================
# 🏁 Main Script Execution Example
# ==============================================================================

# 💡 Replace these placeholder values with your actual Informatica details
INTEGRATION_SVC="INT_SVC_PROD"
DOMAIN="Domain_Dev"
REPOSITORY="REPO_DEV"
FOLDER="MyDataProject"
WORKFLOW_TO_RUN="wf_daily_etl"

# --- Call the function ---
run_informatica_workflow \
    "${INTEGRATION_SVC}" \
    "${DOMAIN}" \
    "${REPOSITORY}" \
    "${FOLDER}" \
    "${WORKFLOW_TO_RUN}"

# --- Handle the overall status of the function call ---
SCRIPT_EXIT_STATUS=$?

if [ "$SCRIPT_EXIT_STATUS" -eq 0 ]; then
    echo ""
    echo "--- Main Script Status: Workflow call completed successfully. ---"
else
    echo ""
    echo "--- Main Script Status: Workflow call failed with exit code ${SCRIPT_EXIT_STATUS}. Aborting. ---"
    exit $SCRIPT_EXIT_STATUS
fi