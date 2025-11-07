import json
import subprocess
import sys

def run_informatica_workflow(user, password, folder, workflow):
    """
    Reads config, constructs the pmcmd command, and executes the workflow.
    """
    config_file = 'config.json'
    
    try:
        # Load server connection details from config.json
        with open(config_file, 'r') as f:
            config = json.load(f)
            
        int_svc = config['integration_service']
        domain = config['domain_name']
        pmcmd_exec = config['pmcmd_path']
        
    except FileNotFoundError:
        print(f"Error: Configuration file '{config_file}' not found.")
        sys.exit(1)
    except json.JSONDecodeError:
        print(f"Error: Could not decode JSON from '{config_file}'. Check file format.")
        sys.exit(1)
    except KeyError as e:
        print(f"Error: Missing key {e} in '{config_file}'.")
        sys.exit(1)

    # pmcmd startworkflow command and arguments
    # Note: It's generally safer to pass arguments as a list to subprocess.run
    pmcmd_command = [
        pmcmd_exec,
        'startworkflow',
        '-sv', int_svc,
        '-d', domain,
        '-u', user,
        '-p', password,
        '-f', folder,
        '-wait', # Optional: Wait for the workflow to complete
        workflow
    ]
    
    print(f"Attempting to run workflow: {workflow} in folder {folder}...")

    try:
        # Execute the command
        # capture_output=True captures stdout/stderr
        # text=True decodes the output as text
        # check=True raises a CalledProcessError if the command returns a non-zero exit code
        result = subprocess.run(
            pmcmd_command,
            check=True,
            capture_output=True,
            text=True
        )
        
        print("\n--- Command Output (STDOUT) ---")
        print(result.stdout)
        print("-------------------------------\n")
        print(f"Successfully started and completed workflow: {workflow}")

    except subprocess.CalledProcessError as e:
        print(f"\n--- Command Failed (STDERR) ---")
        print(e.stderr)
        print("-------------------------------\n")
        print(f"Error executing pmcmd command. Return Code: {e.returncode}")
        sys.exit(e.returncode)
    except FileNotFoundError:
        print(f"\nError: pmcmd executable not found at '{pmcmd_exec}'. Check 'pmcmd_path' in config.json.")
        sys.exit(1)


if __name__ == '__main__':
    # Check for required command-line arguments
    if len(sys.argv) != 5:
        print("Usage: python run_workflow.py <userid> <password> <folder> <workflow_name>")
        sys.exit(1)
        
    # Extract arguments
    userid = sys.argv[1]
    password = sys.argv[2]
    folder_name = sys.argv[3]
    workflow_name = sys.argv[4]

    run_informatica_workflow(userid, password, folder_name, workflow_name)