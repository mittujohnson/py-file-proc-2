#!/bin/bash

# ==============================================================================
# 🗂️ Core Utility Function: Count files by extension in a given directory
# Arguments: $1 = directory_path, $2 = file_extension
# ==============================================================================
count_files() {
    local target_dir="$1"
    local extension="$2"
    local count=0

    # 1. Input Validation (Checking if directory exists)
    if [ ! -d "$target_dir" ]; then
        log_error "Directory not found: $target_dir"
        return 1 # Return a non-zero status to indicate failure
    fi

    # 2. Core Logic: Use 'find' to count files matching the extension
    # We use -type f for files and -iname for case-insensitive matching
    count=$(find "$target_dir" -type f -iname "*.$extension" | wc -l)

    # 3. Output the result
    echo "$count"
    return 0 # Return 0 for success
}

# ==============================================================================
# 📝 Logging/Helper Function: Print a custom error message to stderr
# Arguments: $1 = error_message
# ==============================================================================
log_error() {
    local message="$1"
    # Print to stderr
    echo -e "❌ ERROR: $message" >&2
}

# ==============================================================================
# 🚀 Main Execution Logic Function
# This function orchestrates the script flow and handles command-line arguments.
# ==============================================================================
main() {
    # Check for required command-line arguments: Directory and Extension
    if [ "$#" -ne 2 ]; then
        log_error "Invalid number of arguments."
        echo "Usage: $0 <DIRECTORY_PATH> <FILE_EXTENSION>"
        exit 1
    fi

    local directory_path="$1"
    local file_extension="$2"
    
    echo "🔍 Analyzing directory: $directory_path"
    echo "🔎 Looking for files with extension: .$file_extension"
    echo "--------------------------------------------------------"

    # Call the core function and capture its output (the count) and exit status
    local file_count
    file_count=$(count_files "$directory_path" "$file_extension")
    local status=$? # Capture the exit status of the *last* command (count_files)

    # Check the status returned by the function
    if [ "$status" -eq 0 ]; then
        echo "✅ SUCCESS: Found $file_count files with extension '.$file_extension'."
    else
        # Error was already logged by log_error inside count_files
        echo "🛑 Analysis failed."
        exit 2
    fi
}

# ==============================================================================
# 🏁 Script Entry Point
# The script always starts here by calling the 'main' function with
# all command-line arguments passed to the script ($@).
# ==============================================================================
main "$@"