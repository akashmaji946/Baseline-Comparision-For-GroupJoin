# Baseline Benchmark: Group-Join vs. Hash-Join

This experiment benchmarks two common strategies for `JOIN` followed by `GROUP BY ... SUM()` operations in databases using C++:

* Hash-Join then Aggregate: The standard approach — perform a full join, then aggregate.
* Group-Join (Pre-Aggregation): Optimized strategy — aggregate before joining, reducing intermediate data.

---

## Project Goals

* Compare performance of Group-Join vs. Hash-Join Aggregation.
* Demonstrate the effectiveness of pre-aggregation in reducing runtime and memory usage.
* Provide visualizations and LaTeX tables for clear performance analysis.

---

## Prerequisites

Ensure you have the following installed:

* C++ compiler supporting C++14 or later (e.g., `g++`, `clang++`)
* Python 3.9+
* `pip` for Python package management

---

## Setup Instructions

### 1. Clone the Repository

```bash
git clone https://github.com/your-username/join-aggregation-benchmark.git
cd join-aggregation-benchmark
```

### 2. Set Up Python Virtual Environment (Optional but Recommended)

```bash
python3 -m venv venv
source venv/bin/activate  # For Windows: .\venv\Scripts\activate
```

### 3. Install Python Dependencies

```bash
pip install pandas matplotlib seaborn
```

### 4. Compile the C++ Benchmark

```bash
g++ -std=c++17 -O2 combined_compare.cpp
```

---

## Running the Benchmark

### Option A: Automated (Recommended)

```bash
chmod +x run_benchmark.sh
./run_benchmark.sh
```

This script will:

* Generate test datasets (`A.txt`, `B.txt`)
* Run the benchmark for varying configs
* Save results to `times.txt`

---

### Option B: Manual Execution

#### Step 1: Generate Data

```bash
python3 data_gen.py [rows_A] [rows_B] [uniqueness_percentage]
# Example:
python3 data_gen.py 1000000 1000000 0.5
```

#### Step 2: Run Benchmark
Run the C++ file as:
```bash
./a.out >> results.txt
```

---

## Results and Visualization

After running the benchmark, generate plots:

```bash
python3 plot_all.py
```

This will produce:
* different plots
* `combined_speedups.png`: Overview plot
* Individual plots for each test

---

## File Structure

| File/Folder           | Description                                      |
| ------------------    | ------------------------------------------------ |
| `combined_compare.cpp`| C++ implementation of both join strategies       |
| `data_gen.py`         | Generates test data (`A.txt`, `B.txt`)           |
| `run_benchmark.sh`    | Automates test execution and data cleanup        |
| `plot_all.py`         | Parses results, generates plots                  |
| `times.txt`           | Stores benchmark results                         |
| `results.txt`         | Manual run results                               |
---

## Sample Benchmark Output

```text
[INFO] Dataset: 1M rows | Uniqueness: 50%
Hash-Join Time: 412 ms
Group-Join Time: 273 ms
Speedup: 1.51x
```

---

## Author
- Akash Maji
- akashmaji@iisc.ac.in
