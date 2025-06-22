import re
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

def parse_benchmark_data(filename="times.txt"):
    """
    Parses the benchmark output file and converts it into a pandas DataFrame.

    Args:
        filename (str): The name of the input log file.

    Returns:
        pandas.DataFrame: A DataFrame containing the parsed benchmark results,
                          or None if the file cannot be read.
    """
    print(f"Reading and parsing '{filename}'...")
    try:
        with open(filename, 'r') as f:
            content = f.read()
    except FileNotFoundError:
        print(f"Error: The file '{filename}' was not found.")
        print("Please run the benchmark script first to generate the output file.")
        return None

    # Regex to find each block of results
    block_regex = re.compile(
        r"TableA size: \s*(\d+)\s*\n"
        r"TableB size: \s*(\d+)\s*\n"
        r"Uniqueness: \s*([\d.]+)\s*\n"
        r".*?Execution Time \(HashJoin-Then-Aggregation\): \s*([\d.e\-+]+) s\s*\n"
        r"Execution Time \(GroupJoin\): \s*([\d.e\-+]+) s\s*\n"
        r"Speed Up: \s*([\d.e\-+]+)",
        re.DOTALL
    )

    records = []
    for match in block_regex.finditer(content):
        size_a, size_b, uniqueness, time_hash, time_group, speedup = match.groups()
        records.append({
            'size': int(size_a),
            'uniqueness': float(uniqueness),
            'time_hashjoin_s': float(time_hash),
            'time_groupjoin_s': float(time_group),
            'speedup': float(speedup)
        })
    
    if not records:
        print("Warning: No valid data blocks were found in the file.")
        return None
        
    print(f"Successfully parsed {len(records)} data points.")
    return pd.DataFrame(records)

def create_visualizations(df):
    """
    Generates and saves plots from the benchmark data DataFrame.

    Args:
        df (pandas.DataFrame): The DataFrame with benchmark data.
    """
    if df is None or df.empty:
        print("Cannot create visualizations because no data was provided.")
        return

    print("Generating plots...")
    
    # Set plot style
    sns.set_theme(style="whitegrid")

    # Get the different table sizes that were tested
    table_sizes = sorted(df['size'].unique())

    for size in table_sizes:
        # Filter data for the current table size
        subset = df[df['size'] == size]
        if subset.empty:
            continue

        # --- Plot 1: Execution Time vs. Uniqueness ---
        fig, ax1 = plt.subplots(figsize=(12, 7))
        
        ax1.set_title(f'Execution Time vs. Uniqueness (Table Size: {size:,})', fontsize=16, weight='bold')
        ax1.set_xlabel('Key Uniqueness Percentage (%)', fontsize=12)
        ax1.set_ylabel('Execution Time (seconds)', fontsize=12)
        
        # Plotting the two algorithms
        sns.lineplot(data=subset, x='uniqueness', y='time_hashjoin_s', 
                     ax=ax1, marker='o', label='Hash-Join then Aggregate', color='r')
        sns.lineplot(data=subset, x='uniqueness', y='time_groupjoin_s', 
                     ax=ax1, marker='o', label='Group-Join (Pre-Aggregate)', color='b')
        
        ax1.set_yscale('log') # Log scale is best for wide time ranges
        ax1.legend()
        ax1.grid(True, which="both", ls="--")
        
        # Format x-axis as percentage
        ax1.set_xticklabels([f'{x*100:.0f}%' for x in ax1.get_xticks()])
        
        plt.tight_layout()
        plot_filename_1 = f"plot_times_vs_uniqueness_size_{size}.png"
        plt.savefig(plot_filename_1)
        print(f"Saved plot: {plot_filename_1}")
        plt.close(fig)

        # --- Plot 2: Speed Up vs. Uniqueness ---
        fig, ax2 = plt.subplots(figsize=(12, 7))

        ax2.set_title(f'Speed Up vs. Uniqueness (Table Size: {size:,})', fontsize=16, weight='bold')
        ax2.set_xlabel('Key Uniqueness Percentage (%)', fontsize=12)
        ax2.set_ylabel('Speed Up Factor (X times faster)', fontsize=12)
        
        sns.lineplot(data=subset, x='uniqueness', y='speedup', 
                     ax=ax2, marker='o', label='Speed Up (Group-Join / Hash-Join)', color='g')
        ax2.axhline(1, color='gray', linestyle='--', label='No Speed Up')
        ax2.legend()
        
        # Format x-axis as percentage
        ax2.set_xticklabels([f'{x*100:.0f}%' for x in ax2.get_xticks()])

        plt.tight_layout()
        plot_filename_2 = f"plot_speedup_vs_uniqueness_size_{size}.png"
        plt.savefig(plot_filename_2)
        print(f"Saved plot: {plot_filename_2}")
        plt.close(fig)

if __name__ == "__main__":
    # This script assumes 'times.txt' is in the same directory.
    # To run this script, you need pandas, matplotlib, and seaborn.
    # You can install them with pip:
    # pip install pandas matplotlib seaborn
    
    benchmark_df = parse_benchmark_data()
    create_visualizations(benchmark_df)
    print("\nVisualization script finished.")
