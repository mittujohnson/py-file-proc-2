import subprocess
import os

# --- Python Function 1 (Initial Setup) ---
def start_processing():
    """First function to run: Initializes the workflow."""
    print("--- 1. Initial Python Setup Started ---")
    print("Preparing input files and configurations...")
    # Add any setup logic here (e.g., logging, environment checks)
    print("--- 1. Initial Python Setup Complete ---\n")

# --- Python Function 2 (Success Path) ---
def handle_success(message):
    """Function to run if the shell script succeeds."""
    print("--- 3a. Success Handler Running ---")
    print(f"Workflow finished cleanly. Message from shell: {message}")
    # Add finalization logic here (e.g., sending success notification)
    print("--- 3a. Success Handler Complete ---\n")

# --- Python Function 3 (Failure Path) ---
def handle_failure(message):
    """Function to run if the shell script fails."""
    print("--- 3b. Failure Handler Running ---")
    print(f"ALERT: Shell script failed. Error details: {message}")
    # Add error handling logic here (e.g., sending PagerDuty alert, cleanup)
    print("--- 3b. Failure Handler Complete ---\n")

# --- Orchestration Function ---
def run_full_workflow(simulation_mode):
    """
    Runs the entire pipeline, calling the shell script and branching based on its exit code.
    """
    
    # 1. Start the initial Python function
    start_processing()
    
    # Define the command to run the shell script, passing the simulation mode as argument
    script_path = os.path.join(os.getcwd(), 'process_data.sh')
    command = ['bash', script_path, simulation_mode]
    
    print(f"--- 2. Executing Shell Script with mode: {simulation_mode} ---")
    
    try:
        # 2. Execute the shell script and wait for it to finish
        # We use run() with capture_output=True to capture stdout/stderr
        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            check=False  # Do NOT raise exception on non-zero exit code yet
        )
        
        # 3. Check the exit code for branching
        if result.returncode == 0:
            # Command SUCCESS
            handle_success(result.stdout.strip())
        else:
            # Command FAILURE (returncode is non-zero)
            # Use stderr for error details, falling back to stdout
            error_output = result.stderr.strip() or result.stdout.strip()
            handle_failure(error_output)
            
    except FileNotFoundError:
        print(f"CRITICAL ERROR: Shell script '{script_path}' not found.")
    except Exception as e:
        print(f"An unexpected error occurred during subprocess execution: {e}")

if __name__ == "__main__":
    # Ensure the script file has execute permissions (necessary in many environments)
    try:
        os.chmod('process_data.sh', 0o755)
    except Exception:
        pass # Ignore if permission change fails
        
    print("\n====================================")
    print("A) --- SIMULATING SUCCESS SCENARIO ---")
    print("====================================")
    run_full_workflow("success")
    
    print("\n====================================")
    print("B) --- SIMULATING FAILURE SCENARIO ---")
    print("====================================")
    run_full_workflow("fail")