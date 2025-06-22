#!/bin/bash

# This script automates the performance testing of the join algorithms.
# It iterates through different table sizes and key uniqueness percentages,
# generates test data, runs the C++ benchmark program, and logs the output.

# --- Configuration ---
CPP_SOURCE_FILE="combined_int_long.cpp"
CPP_EXECUTABLE="a.out"
PYTHON_GENERATOR="data_gen.py"
OUTPUT_FILE="run_times_and_speedups.txt"

# Arrays for test parameters
# SIZES=(1000 10000 100000 1000000 10000000 100000000)
SIZES=(100000000 10000000 1000000 100000 10000 1000)
UNIQUENESS_VALUES=$(seq 0.1 0.1 1.0) # Generates 0.1, 0.2, ..., 1.0

# --- Script Start ---
# Clean up previous results file
if [ -f "$OUTPUT_FILE" ]; then
    rm "$OUTPUT_FILE"
fi
echo "Benchmark log started on $(date)" > "$OUTPUT_FILE"

# Compile the C++ code once before starting the tests
echo "Compiling C++ source file: $CPP_SOURCE_FILE..."
g++ -std=c++17 -O2 "$CPP_SOURCE_FILE" -o "$CPP_EXECUTABLE"
if [ $? -ne 0 ]; then
    echo "Compilation failed. Exiting."
    exit 1
fi
echo "Compilation successful."

# --- Main Test Loop ---
for size in "${SIZES[@]}"; do
    for uniqueness in $UNIQUENESS_VALUES; do
        
        echo "------------------------------------------------------------" | tee -a "$OUTPUT_FILE"
        echo "Starting test with Size: $size, Uniqueness: $uniqueness"
        
        # Log current parameters to the output file
        echo "TableA size: $size" >> "$OUTPUT_FILE"
        echo "TableB size: $size" >> "$OUTPUT_FILE"
        echo "Uniqueness: $uniqueness" >> "$OUTPUT_FILE"
        
        # Generate the test data files (A.txt and B.txt)
        echo "Running data generator..."
        python3 "$PYTHON_GENERATOR" "$size" "$size" "$uniqueness"
        
        # Check if data generation was successful
        if [ ! -f "A.txt" ] || [ ! -f "B.txt" ]; then
            echo "Data generation failed. Skipping this test."
            continue
        fi
        
        # Run the compiled C++ program and append its output to the log file
        echo "Running C++ benchmark..."
        ./"$CPP_EXECUTABLE" >> "$OUTPUT_FILE"
        
        echo "Test completed."

    done
done

echo "------------------------------------------------------------" | tee -a "$OUTPUT_FILE"
echo "All benchmarks finished. Results are in $OUTPUT_FILE"

# Clean up generated files
# rm -f A.txt B.txt As.txt Bs.txt

