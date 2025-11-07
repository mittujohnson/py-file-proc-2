import json
import subprocess
import sys
import time

# --- Configuration Constants ---
POLLING_INTERVAL_SECONDS = 30 # Time delay between status checks
MAX_RETRIES = 60              # Max checks (30 min total monitoring time if interval is 30s)
CONFIG_FILE = 'config.json'

# --- Informatica Workflow Status Codes (pmcmd return codes) ---
STATUS_RUNNING = 1
STATUS_SUCCEEDED = 2
STATUS_STOPPED = 3
STATUS_FAILED = 4
STATUS_ABORTED = 5
STATUS_UNSCHEDULED = 6

STATUS_MAP = {
    STATUS_RUNNING: "Running (1)",
    STATUS_SUCCEEDED: "Succeeded (2)",
    STATUS_STOPPED: "Stopped (3)",
    STATUS_FAILED: "Failed (4)",
    STATUS_ABORTED: "Aborted (5)",
    STATUS_UNSCHEDULED: "Unscheduled (6)"
}

def load_config():
    """Load Informatica connection details from config.json."""
    try:
        with open(CONFIG_FILE, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"❌ Error: Configuration file '{CONFIG_FILE}' not found.")
        sys.exit(1)
    except json.JSONDecodeError:
        print(f"❌ Error: Could not decode JSON from '{CONFIG_FILE}'.")
        sys.exit(1)
    except KeyError as e:
        print(f"❌ Error: Missing key {e} in '{CONFIG_FILE}'.")
        sys.exit(1)

# ----------------------------------------------------------------------
## 🚀 Workflow Start Function
# ----------------------------------------------------------------------

def start_workflow(config, user, password, folder, workflow):
    """Executes pmcmd startworkflow and checks if the start command succeeded."""
    pmcmd_exec = config['pmcmd_path']
    int_svc = config['integration_service']
    domain = config['domain_name']

    # pmcmd startworkflow command (using -nowait so we can monitor separately)
    pmcmd_command = [
        pmcmd_exec,
        'startworkflow',
        '-sv', int_svc,
        '-d', domain,
        '-u', user,
        '-p', password,
        '-f', folder,
        '-nowait', # Important: Start the workflow but don't wait for completion
        workflow
    ]
    
    print(f"✅ Attempting to **START** workflow: {workflow} in folder {folder}...")

    try:
        # Check=True ensures a subprocess.CalledProcessError is raised if pmcmd fails to start the workflow (return code != 0)
        subprocess.run(
            pmcmd_command,
            check=True,
            capture_output=True,
            text=True
        )
        print("➡️ Workflow started successfully. Entering monitor mode...")
        return True

    except subprocess.CalledProcessError as e:
        print(f"\n🚨 **FAILURE** starting workflow. PMCMD returned code: {e.returncode}")
        print(e.stderr)
        return False
    except FileNotFoundError:
        print(f"\n❌ Error: pmcmd executable not found at '{pmcmd_exec}'.")
        sys.exit(1)

# ----------------------------------------------------------------------
## 🔍 Workflow Monitor Function
# ----------------------------------------------------------------------

def get_workflow_status(config, user, password, folder, workflow):
    """Executes pmcmd getworkflowdetails and returns the workflow status code."""
    pmcmd_exec = config['pmcmd_path']
    int_svc = config['integration_service']
    domain = config['domain_name']

    pmcmd_command = [
        pmcmd_exec,
        'getworkflowdetails',
        '-sv', int_svc,
        '-d', domain,
        '-u', user,
        '-p', password,
        '-f', folder,
        workflow
    ]

    try:
        # The workflow status is returned as the exit code of the pmcmd process
        result = subprocess.run(
            pmcmd_command,
            check=False, # We handle the return code ourselves since non-zero codes are expected
            capture_output=True,
            text=True
        )
        # If the workflow is found, its status is the return code
        return result.returncode
        
    except FileNotFoundError:
        print(f"\n❌ Error: pmcmd executable not found at '{pmcmd_exec}'.")
        sys.exit(1)

# ----------------------------------------------------------------------
## 🔗 Main Orchestration Function
# ----------------------------------------------------------------------

def orchestrate_workflow(config, user, password, folder, workflow):
    """Starts the workflow and then monitors it until completion."""

    # 1. Start the workflow
    if not start_workflow(config, user, password, folder, workflow):
        return 1 # Exit with error code if starting failed

    print(f"\n**Monitoring Workflow:** {workflow}...")
    print(f"Polling interval: {POLLING_INTERVAL_SECONDS}s | Max checks: {MAX_RETRIES}")

    # 2. Monitor the workflow
    for attempt in range(1, MAX_RETRIES + 1):
        status_code = get_workflow_status(config, user, password, folder, workflow)
        status_name = STATUS_MAP.get(status_code, f"Unknown ({status_code})")
        
        current_time = time.strftime('%Y-%m-%d %H:%M:%S')
        
        if status_code == STATUS_SUCCEEDED:
            print(f"\n✅ **SUCCESS** - Workflow **{workflow}** completed successfully at {current_time}.")
            return 0 # Success exit code
            
        elif status_code in (STATUS_FAILED, STATUS_ABORTED, STATUS_STOPPED):
            print(f"\n❌ **FAILURE** - Workflow **{workflow}** ended with status: {status_name} at {current_time}.")
            return 1 # Failure exit code
            
        elif status_code == STATUS_RUNNING:
            print(f"[{current_time}] Check {attempt}/{MAX_RETRIES}: Status is {status_name}. Sleeping...")
            time.sleep(POLLING_INTERVAL_SECONDS)
            
        else:
            print(f"\n⚠️ **WARNING** - Workflow **{workflow}** ended with unexpected status: {status_name} at {current_time}.")
            return 1

    # If the loop completes due to max retries
    print(f"\n🚨 **TIMEOUT** - Workflow **{workflow}** did not complete within the maximum monitoring time.")
    return 1 # Failure exit code


if __name__ == '__main__':
    # Argument validation
    if len(sys.argv) != 5:
        print("Usage: python orchestrator.py <userid> <password> <folder> <workflow_name>")
        sys.exit(1)
        
    # Extract arguments
    userid = sys.argv[1]
    password = sys.argv[2]
    folder_name = sys.argv[3]
    workflow_name = sys.argv[4]

    # Load configuration and start orchestration
    config_data = load_config()
    final_exit_code = orchestrate_workflow(config_data, userid, password, folder_name, workflow_name)
    sys.exit(final_exit_code)