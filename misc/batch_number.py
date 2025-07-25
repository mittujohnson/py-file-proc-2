import math

def get_file_suffix_range(total_suffix_number, batch_number, current_batch):
    """
    Calculates the range of file suffix numbers for a given batch.

    Args:
        total_suffix_number (int): The total number representing the file suffix.
        batch_number (int): The total number of batches.
        current_batch (int): The current batch number (1-indexed).

    Returns:
        list: A list containing two integers [start_suffix, end_suffix] for the given batch.
              Returns an empty list if inputs are invalid (e.g., current_batch > batch_number).
    """

    if not all(isinstance(arg, int) and arg > 0 for arg in [total_suffix_number, batch_number, current_batch]):
        raise ValueError("All inputs (total_suffix_number, batch_number, current_batch) must be positive integers.")

    if current_batch > batch_number:
        return []

    base_batch_size = total_suffix_number // batch_number
    remainder = total_suffix_number % batch_number

    start_suffix = (current_batch - 1) * base_batch_size + 1
    end_suffix = current_batch * base_batch_size

    # Distribute the remainder evenly among the first 'remainder' batches
    if current_batch <= remainder:
        start_suffix += (current_batch - 1)
        end_suffix += current_batch
    else:
        start_suffix += remainder
        end_suffix += remainder

    return [start_suffix, end_suffix]

# Examples:
# Example 1: total_suffix_number = 100, batch_number = 2
print("Example 1 (total=100, batches=2):")
print(f"Batch 1: {get_file_suffix_range(100, 2, 1)}") # Expected: [1, 50]
print(f"Batch 2: {get_file_suffix_range(100, 2, 2)}") # Expected: [51, 100]

# Example 2: total_suffix_number = 100, batch_number = 3
print("\nExample 2 (total=100, batches=3):")
print(f"Batch 1: {get_file_suffix_range(100, 3, 1)}") # Expected: [1, 34] (remainder adds to first batch)
print(f"Batch 2: {get_file_suffix_range(100, 3, 2)}") # Expected: [35, 67]
print(f"Batch 3: {get_file_suffix_range(100, 3, 3)}") # Expected: [68, 100]

# Example 3: total_suffix_number = 10, batch_number = 1
print("\nExample 3 (total=10, batches=1):")
print(f"Batch 1: {get_file_suffix_range(10, 1, 1)}") # Expected: [1, 10]

# Example 4: total_suffix_number = 7, batch_number = 4
print("\nExample 4 (total=7, batches=4):")
print(f"Batch 1: {get_file_suffix_range(7, 4, 1)}") # Expected: [1, 2]
print(f"Batch 2: {get_file_suffix_range(7, 4, 2)}") # Expected: [3, 4]
print(f"Batch 3: {get_file_suffix_range(7, 4, 3)}") # Expected: [5, 6]
print(f"Batch 4: {get_file_suffix_range(7, 4, 4)}") # Expected: [7, 7]

# Example with invalid current_batch
print("\nExample with invalid current_batch:")
print(f"Batch 5 (out of 4): {get_file_suffix_range(7, 4, 5)}") # Expected: []

# Example with invalid input types
try:
    get_file_suffix_range(100, "2", 1)
except ValueError as e:
    print(f"\nError handling test: {e}")