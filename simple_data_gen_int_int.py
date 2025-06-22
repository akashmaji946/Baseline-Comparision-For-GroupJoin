import sys
import random

def generate_data():
    """
    Generates two simplified CSV files, A.txt and B.txt, with random data.
    - A.txt format: k,v
    - B.txt format: k
    The number of rows for each file is taken from command-line arguments.
    """

    # --- Configuration ---
    # Default number of rows if no command-line arguments are provided
    try:
        num_rows_a = int(sys.argv[1])
        print(f"Number of rows for Table A set to {num_rows_a} from command line.")
    except (IndexError, ValueError):
        num_rows_a = 10000
        print(f"Usage: python {sys.argv[0]} [num_rows_a] [num_rows_b]")
        print(f"Using default number of rows for Table A: {num_rows_a}")

    try:
        num_rows_b = int(sys.argv[2])
        print(f"Number of rows for Table B set to {num_rows_b} from command line.")
    except (IndexError, ValueError):
        num_rows_b = 10000
        print(f"Using default number of rows for Table B: {num_rows_b}")
    
    key_pool_size = max(1, (num_rows_a + num_rows_b) // 20)
    key_range_max = key_pool_size * 5
    

    key_pool = [random.randint(0, key_range_max) for _ in range(key_pool_size)]
    if not key_pool:
        key_pool.append(0)

    # --- Generate A.txt (k,v) ---
    print("\nGenerating A.txt...")
    try:
        with open("A.txt", "w") as f_a:
            for _ in range(num_rows_a):
                k = random.choice(key_pool) 
                v = random.randint(1, 100)
                f_a.write(f"{k},{v}\n")
    except IOError as e:
        print(f"Error writing to file A.txt: {e}")
        return

    print(">> A.txt created successfully.")

    # --- Generate B.txt (k) ---
    print("\nGenerating B.txt...")
    try:
        with open("B.txt", "w") as f_b:
            for _ in range(num_rows_b):
                k = random.choice(key_pool)
                f_b.write(f"{k}\n")
    except IOError as e:
        print(f"Error writing to file B.txt: {e}")
        return
        
    print(">> B.txt created successfully.")

if __name__ == "__main__":
    generate_data()
