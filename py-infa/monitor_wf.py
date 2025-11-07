import json
import subprocess
import sys
import time

# --- Configuration ---
POLLING_INTERVAL_SECONDS = 30 
MAX_RETRIES = 60 # Maximum number of checks (30 min if interval is 30s)

# --- Informatica Workflow Status Codes ---
# These are the codes returned by pmcmd getworkflowdetails 
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

def load_config(config_file='config.json'):
    """Load Informatica connection details from config.json."""
    try:
        with open(config_file, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"❌ Error: Configuration file '{config_file}' not found.")
        sys.exit(1)
    except json.JSONDecodeError:
        print(f"❌ Error: Could not decode JSON from '{config_file}'. Check file format.")
        sys.exit(1)
    except KeyError as e:
        print(f"❌ Error: Missing key {e} in '{config_file}'.")
        sys.exit(1)

def get_workflow_status(config, user, password, folder, workflow):
    """Executes pmcmd getworkflowdetails and returns the status code."""
    pmcmd_exec = config['pmcmd_path']
    int_svc = config['integration_service']
    domain = config['domain_name']

    # pmcmd getworkflowdetails command
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
        result = subprocess.run(
            pmcmd_command,
            check=True,
            capture_output=True,
            text=True
        )
        
        # The 'getworkflowdetails' output is large, we only need the exit code.
        # The exit code from pmcmd *itself* is the workflow status code.
        return result.returncode
        
    except subprocess.CalledProcessError as e:
        # If the pmcmd command returns a non-zero exit code, it means the workflow
        # has a status code (e.g., 2 for Success, 4 for Failed).
        # We check the return code of the failed process.
        if e.returncode in STATUS_MAP:
             return e.returncode
        else:
             print(f"🚨 PMCMD Command Failed with unexpected code: {e.returncode}")
             print(e.stderr)
             return STATUS_FAILED # Treat unexpected error as failure
             
    except FileNotFoundError:
        print(f"❌ Error: pmcmd executable not found at '{pmcmd_exec}'. Check 'pmcmd_path' in config.json.")
        sys.exit(1)


def monitor_workflow(config, user, password, folder, workflow):
    """Monitors a workflow until it succeeds or fails."""
    
    print(f"⏳ Starting monitor for workflow: **{workflow}** in folder **{folder}**.")
    print(f"Polling interval: {POLLING_INTERVAL_SECONDS}s | Max checks: {MAX_RETRIES}")

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
            print(f"[{current_time}] Check {attempt}/{MAX_RETRIES}: Workflow status is {status_name}. Waiting {POLLING_INTERVAL_SECONDS} seconds...")
            time.sleep(POLLING_INTERVAL_SECONDS)
            
        else:
            print(f"\n⚠️ **WARNING** - Workflow **{workflow}** ended with unexpected status: {status_name} at {current_time}.")
            return 1 # Treat unexpected status as failure

    # If the loop completes without success/failure
    print(f"\n🚨 **TIMEOUT** - Workflow **{workflow}** did not complete within {MAX_RETRIES * POLLING_INTERVAL_SECONDS} seconds.")
    return 1 # Failure exit code


if __name__ == '__main__':
    # Check for required command-line arguments
    if len(sys.argv) != 5:
        print("Usage: python monitor_workflow.py <userid> <password> <folder> <workflow_name>")
        sys.exit(1)
        
    # Extract arguments
    userid = sys.argv[1]
    password = sys.argv[2]
    folder_name = sys.argv[3]
    workflow_name = sys.argv[4]

    # Load configuration and start monitoring
    config_data = load_config()
    exit_code = monitor_workflow(config_data, userid, password, folder_name, workflow_name)
    sys.exit(exit_code)