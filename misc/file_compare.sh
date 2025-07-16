#!/bin/bash

# Check if two arguments (folder paths) are provided
if [ "$#" -ne 2 ]; then
    echo "Usage: $0 <folder1_path> <folder2_path>"
    exit 1
fi

folder1="$1"
folder2="$2"

# Check if folders exist
if [ ! -d "$folder1" ]; then
    echo "Error: Folder '$folder1' not found."
    exit 1
fi

if [ ! -d "$folder2" ]; then
    echo "Error: Folder '$folder2' not found."
    exit 1
fi

echo "File Name,Folder 1 Availability,Folder 2 Availability,Content Match"
echo "-----------------------------------------------------------------"

# Get files from folder 1
files_in_folder1=($(ls -1 "$folder1"))

# Get files from folder 2
files_in_folder2=($(ls -1 "$folder2"))

# Combine all unique file names from both folders
all_files=($(printf "%s\n%s\n" "${files_in_folder1[@]}" "${files_in_folder2[@]}" | sort -u))

for file in "${all_files[@]}"; do
    folder1_available="N"
    folder2_available="N"
    content_match="N/A" # Default for files not in both folders

    # Check availability in folder 1
    if [[ " ${files_in_folder1[@]} " =~ " ${file} " ]]; then
        folder1_available="Y"
    fi

    # Check availability in folder 2
    if [[ " ${files_in_folder2[@]} " =~ " ${file} " ]]; then
        folder2_available="Y"
    fi

    # Compare content if file is present in both folders
    if [ "$folder1_available" == "Y" ] && [ "$folder2_available" == "Y" ]; then
        # Use diff -wB:
        # -w: Ignores all whitespace
        # -B: Ignores blank lines
        if diff -wB "${folder1}/${file}" "${folder2}/${file}" > /dev/null 2>&1; then
            content_match="Y"
        else
            content_match="N"
        fi
    fi

    echo "$file,$folder1_available,$folder2_available,$content_match"
done