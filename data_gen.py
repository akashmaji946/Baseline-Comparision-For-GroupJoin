import sys
import random
import threading

# --- File Generation Functions (for threading) ---

def generate_file_a(num_rows, keys):
    """Generates the A.txt file with k,v format."""
    print("Thread A: Starting generation of A.txt...")
    try:
        with open("A.txt", "w") as f_a:
            for i in range(num_rows):
                k = keys[i]
                v = random.randint(1, 100)
                f_a.write(f"{k},{v}\n")
    except IOError as e:
        print(f"Thread A: Error writing to file A.txt: {e}")
        return
    print(">> Thread A: A.txt created successfully.")

def generate_file_b(num_rows, keys):
    """Generates the B.txt file with k format."""
    print("Thread B: Starting generation of B.txt...")
    try:
        with open("B.txt", "w") as f_b:
            for i in range(num_rows):
                k = keys[i]
                f_b.write(f"{k}\n")
    except IOError as e:
        print(f"Thread B: Error writing to file B.txt: {e}")
        return
    print(">> Thread B: B.txt created successfully.")


def generate_data():
    """
    Generates two CSV files, A.txt and B.txt, with a specified level of key uniqueness.
    Uses separate threads for generating each file.
    """

    # --- Configuration from Command-Line Arguments ---
    try:
        num_rows_a = int(sys.argv[1])
        num_rows_b = int(sys.argv[2])
        uniqueness_percent = float(sys.argv[3])
        if not 0.0 <= uniqueness_percent <= 1.0:
            raise ValueError("Uniqueness percentage must be between 0.0 and 1.0")

        print(f"Table A rows: {num_rows_a}")
        print(f"Table B rows: {num_rows_b}")
        print(f"Key Uniqueness: {uniqueness_percent * 100:.2f}%")

    except (IndexError, ValueError) as e:
        print(f"Error: Invalid arguments. {e}")
        print(f"Usage: python {sys.argv[0]} [num_rows_a] [num_rows_b] [uniqueness_percent]")
        print("Example: python3 data_gen.py 1000 10000 0.9")
        return

    # --- Key Generation based on Uniqueness ---
    print("\nGenerating key pool...")
    total_keys_required = num_rows_a
    num_unique_keys = int(total_keys_required * uniqueness_percent)
    num_duplicate_keys = total_keys_required - num_unique_keys
    
    # Define a reasonable range for key values
    key_range_max = total_keys_required * 2

    # 1. Create the set of unique keys
    # Using a set ensures uniqueness initially, then converting to a list
    unique_key_set = set()
    while len(unique_key_set) < num_unique_keys:
        unique_key_set.add(random.randint(0, key_range_max))
    
    unique_key_list = list(unique_key_set)

    if not unique_key_list:
         # Handle edge case where uniqueness is 0%
        if total_keys_required > 0:
            unique_key_list.append(random.randint(0, key_range_max))
        else:
            print("No rows to generate.")
            return

    # 2. Create the duplicates by sampling from the unique keys
    duplicates = [random.choice(unique_key_list) for _ in range(num_duplicate_keys)]

    # 3. Combine unique keys and duplicates to form the final pool
    final_key_pool = unique_key_list + duplicates
    random.shuffle(final_key_pool)
    print("Key pool generated successfully for A.")


    keys_for_a = final_key_pool[:num_rows_a]


    total_keys_required = num_rows_b
    num_unique_keys = int(total_keys_required * uniqueness_percent)
    num_duplicate_keys = total_keys_required - num_unique_keys
    
    # Define a reasonable range for key values
    key_range_max = total_keys_required * 2

    # 1. Create the set of unique keys
    # Using a set ensures uniqueness initially, then converting to a list
    unique_key_set = set()
    while len(unique_key_set) < num_unique_keys:
        unique_key_set.add(random.randint(0, key_range_max))
    
    unique_key_list = list(unique_key_set)

    if not unique_key_list:
         # Handle edge case where uniqueness is 0%
        if total_keys_required > 0:
            unique_key_list.append(random.randint(0, key_range_max))
        else:
            print("No rows to generate.")
            return

    # 2. Create the duplicates by sampling from the unique keys
    duplicates = [random.choice(unique_key_list) for _ in range(num_duplicate_keys)]

    # 3. Combine unique keys and duplicates to form the final pool
    final_key_pool = unique_key_list + duplicates
    random.shuffle(final_key_pool)
    print("Key pool generated successfully for B.")

    keys_for_b = final_key_pool[:num_rows_b]

    # Create thread objects
    thread_a = threading.Thread(target=generate_file_a, args=(num_rows_a, keys_for_a))
    thread_b = threading.Thread(target=generate_file_b, args=(num_rows_b, keys_for_b))

    print("\nStarting file generation threads...")
    # Start threads
    thread_a.start()
    thread_b.start()

    # Wait for both threads to complete
    thread_a.join()
    thread_b.join()
    
    print("\nAll tasks completed.")

if __name__ == "__main__":
    generate_data()
