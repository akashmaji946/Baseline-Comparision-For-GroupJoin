import sys
import random

def generate_data():
    """
    Generates two simplified CSV files, A.txt and B.txt, with random data.
    - A.txt format: k,v
    - B.txt format: k
    This version generates completely random integer keys without using shared pools.
    """

    # --- Configuration ---
    # The range for a standard 32-bit signed integer.
    INT_MIN = -2147483648
    INT_MAX = 2147483647

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
    

    # --- Generate A.txt (k,v) ---
    print("\nGenerating A.txt...")
    try:
        with open("A.txt", "w") as f_a:
            for _ in range(num_rows_a):
                # Generate a completely random key within the 32-bit int range
                k = random.randint(INT_MIN, INT_MAX) 
                # Generate a random value
                v = random.randint(INT_MIN, INT_MAX)
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
                # Generate a completely random key within the 32-bit int range
                k = random.randint(INT_MIN, INT_MAX)
                f_b.write(f"{k}\n")
    except IOError as e:
        print(f"Error writing to file B.txt: {e}")
        return
        
    print(">> B.txt created successfully.")

if __name__ == "__main__":
    generate_data()
