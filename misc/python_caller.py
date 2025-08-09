import subprocess
import time

def call_program_sequentially(program_path, parameter, interval_seconds, exit_value):
    """
    Calls a Python program with a parameter at a specified interval.

    Args:
        program_path (str): The path to the program to be called.
        parameter (str): The parameter to pass to the program.
        interval_seconds (int): The time interval in seconds.
        exit_value (str): The value in the output that will trigger an exit.
    """
    run_count = 0
    max_runs = 10
    
    try:
        while run_count < max_runs:
            run_count += 1
            print(f"--- Run {run_count}/{max_runs} ---")
            print(f"Calling {program_path} with parameter: {parameter}")
            
            # Use subprocess.run to execute the program and capture its output
            result = subprocess.run(
                ['python', program_path, parameter], 
                check=True, 
                text=True, 
                capture_output=True
            )
            
            program_output = result.stdout.strip()
            print("Program output:")
            print(program_output)
            
            # Check for the exit value in the output
            if exit_value in program_output:
                print(f"Exiting loop because output contained '{exit_value}'.")
                break
            
            if run_count < max_runs:
                print(f"Waiting for {interval_seconds} seconds before the next call...")
                time.sleep(interval_seconds)
            
    except FileNotFoundError:
        print(f"Error: The program '{program_path}' was not found.")
    except subprocess.CalledProcessError as e:
        print(f"Error: The program exited with a non-zero status code: {e.returncode}")
        print(f"Stderr: {e.stderr}")
    except KeyboardInterrupt:
        print("Program stopped by user.")
        
# --- Configuration ---
# You can adjust these variables
target_script = 'target_program.py'
my_parameter = 'some_data'
call_interval = 5 # in seconds
exit_on_output = 'SUCCESS' # The value to look for in the output

if __name__ == "__main__":
    call_program_sequentially(target_script, my_parameter, call_interval, exit_on_output)