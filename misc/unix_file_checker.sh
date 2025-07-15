#!/bin/bash

# Function to display usage information
usage() {
  echo "Usage: $0 <filename>"
  echo "  <filename>: The path to the file containing a list of scripts, one per line."
  exit 1
}

# Check if a filename is provided as an argument
if [ -z "$1" ]; then
  usage
fi

input_file="$1"

# Check if the input file exists and is readable
if [ ! -f "$input_file" ]; then
  echo "Error: File '$input_file' not found."
  exit 1
elif [ ! -r "$input_file" ]; then
  echo "Error: File '$input_file' is not readable."
  exit 1
fi

echo "--- Script Executability Report ---"
echo "Script Name | Executable"
echo "--------------------------"

# Read the file line by line
while IFS= read -r script_name || [[ -n "$script_name" ]]; do
  # Skip empty lines
  if [ -z "$script_name" ]; then
    continue
  fi

  # Check if the script is executable
  if [ -x "$script_name" ]; then
    echo "$script_name | TRUE"
  else
    echo "$script_name | FALSE"
  fi
done < "$input_file"

echo "--------------------------"
echo "Report generated successfully."
