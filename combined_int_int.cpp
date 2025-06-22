#include <iostream>
#include <fstream>
#include <sstream>
#include <vector>
#include <string>
#include <unordered_map>
#include <chrono>
#include <algorithm> // Required for std::sort

// -- Data Structures to represent table rows --

// Represents a single row from table A
struct RowA {
    int k;
    int v;
};

// Represents a single row from table B
struct RowB {
    int k;
};

// Represents a row after the join operation
// Materializing A.k, A.v, B.k
struct JoinedRow {
    int a_k;
    int a_v;
    int b_k;
};

// Represents a final aggregated result row
struct AggregatedResult {
    int k;
    long long sum_v; // Use long long to handle potentially large sums
};

// -- Helper Functions --

/**
 * @brief Parses a CSV string and returns a vector of strings.
 * @param line The string line to parse.
 * @param delimiter The character to split the string by.
 * @return A vector of string tokens.
 */
std::vector<std::string> parse_csv_line(const std::string& line, char delimiter = ',') {
    std::vector<std::string> tokens;
    std::string token;
    std::istringstream tokenStream(line);
    while (std::getline(tokenStream, token, delimiter)) {
        // Trim leading/trailing whitespace
        size_t first = token.find_first_not_of(" \t\n\r");
        if (std::string::npos == first) {
            tokens.push_back("");
            continue;
        }
        size_t last = token.find_last_not_of(" \t\n\r");
        tokens.push_back(token.substr(first, (last - first + 1)));
    }
    return tokens;
}


// -- Core Logic Functions --

// --- METHOD 1: Post-Aggregation (Hash Join then Aggregate) ---

/**
 * @brief Reads data from a CSV file into a vector of RowA structs.
 * @param filename The name of the file to read.
 * @return A vector of RowA structs.
 */
std::vector<RowA> read_table_a(const std::string& filename) {
    std::vector<RowA> table;
    std::ifstream file(filename);
    std::string line;

    if (!file.is_open()) {
        std::cerr << "Error: Could not open file " << filename << std::endl;
        return table;
    }

    while (std::getline(file, line)) {
        std::vector<std::string> tokens = parse_csv_line(line);
        if (tokens.size() == 2) {
            try {
                RowA row;
                row.k = std::stoi(tokens[0]);
                row.v = std::stoi(tokens[1]);
                table.push_back(row);
            } catch (const std::invalid_argument& ia) {
                std::cerr << "Invalid argument in file " << filename << ": " << ia.what() << " on line: " << line << '\n';
            }
        }
    }
    file.close();
    return table;
}

/**
 * @brief Reads data from a CSV file into a vector of RowB structs.
 * @param filename The name of the file to read.
 * @return A vector of RowB structs.
 */
std::vector<RowB> read_table_b(const std::string& filename) {
    std::vector<RowB> table;
    std::ifstream file(filename);
    std::string line;

    if (!file.is_open()) {
        std::cerr << "Error: Could not open file " << filename << std::endl;
        return table;
    }

    while (std::getline(file, line)) {
        std::vector<std::string> tokens = parse_csv_line(line);
        if (tokens.size() == 1) {
             try {
                RowB row;
                row.k = std::stoi(tokens[0]);
                table.push_back(row);
            } catch (const std::invalid_argument& ia) {
                std::cerr << "Invalid argument in file " << filename << ": " << ia.what() << " on line: " << line << '\n';
            }
        }
    }
    file.close();
    return table;
}

/**
 * @brief Performs a hash join on two tables.
 * @param table_a The left table (build side).
 * @param table_b The right table (probe side).
 * @return A vector of JoinedRow structs representing the result of the join.
 */
std::vector<JoinedRow> hash_join(const std::vector<RowA>& table_a, const std::vector<RowB>& table_b) {
    std::unordered_map<int, std::vector<const RowA*>> hash_table;
    for (const auto& row_a : table_a) {
        hash_table[row_a.k].push_back(&row_a);
    }
    
    std::vector<JoinedRow> joined_result;
    for (const auto& row_b : table_b) {
        auto it = hash_table.find(row_b.k);
        if (it != hash_table.end()) {
            for (const auto* matching_row_a_ptr : it->second) {
                joined_result.push_back({matching_row_a_ptr->k, matching_row_a_ptr->v, row_b.k});
            }
        }
    }
    return joined_result;
}

/**
 * @brief Performs aggregation (GROUP BY k, SUM v) on the joined data.
 * @param joined_data The vector of JoinedRow structs.
 * @return A vector of AggregatedResult structs.
 */
std::vector<AggregatedResult> perform_aggregation(const std::vector<JoinedRow>& joined_data) {
    std::unordered_map<int, long long> aggregation_map;
    for (const auto& row : joined_data) {
        aggregation_map[row.a_k] += row.a_v;
    }

    std::vector<AggregatedResult> final_result;
    for (const auto& pair : aggregation_map) {
        final_result.push_back({pair.first, pair.second});
    }
    return final_result;
}

// --- METHOD 2: Pre-Aggregation (GroupJoin) ---

/**
 * @brief Performs a join and aggregation using a pre-aggregation strategy on in-memory vectors.
 * @param table_a The vector for the left table (A).
 * @param table_b The vector for the right table (B).
 * @return A vector of AggregatedResult structs.
 */
std::vector<AggregatedResult> pre_aggregation_join(const std::vector<RowA>& table_a, const std::vector<RowB>& table_b) {
    // 1. Read table A and pre-aggregate sums of 'v' for each key 'k'.
    std::unordered_map<int, long long> pre_agg_a;
    for (const auto& row : table_a) {
        pre_agg_a[row.k] += row.v;
    }

    // 2. Read table B and count occurrences of each key 'k'.
    std::unordered_map<int, int> key_counts_b;
    for (const auto& row : table_b) {
        key_counts_b[row.k]++;
    }

    // 3. Join the aggregated results.
    std::vector<AggregatedResult> final_result;
    for(const auto& b_pair : key_counts_b) {
        int k = b_pair.first;
        int count_in_b = b_pair.second;

        auto a_it = pre_agg_a.find(k);
        if (a_it != pre_agg_a.end()) {
            long long sum_in_a = a_it->second;
            long long final_sum = sum_in_a * count_in_b;
            final_result.push_back({k, final_sum});
        }
    }

    return final_result;
}

/**
 * @brief Sorts and saves the aggregated results to a CSV file.
 * @param filename The name of the output file.
 * @param results The vector of AggregatedResult structs to save.
 */
void save_results(const std::string& filename, const std::vector<AggregatedResult>& results) {
    std::vector<AggregatedResult> sorted_results = results;

    std::sort(sorted_results.begin(), sorted_results.end(), [](const AggregatedResult& a, const AggregatedResult& b){
        return a.k < b.k;
    });

    std::ofstream output_file(filename);
    if (!output_file.is_open()) {
        std::cerr << "Error: Could not open file for writing: " << filename << std::endl;
        return;
    }

    output_file << "k,summ\n";
    for (const auto& row : sorted_results) {
        output_file << row.k << "," << row.sum_v << "\n";
    }
    output_file.close();
}


int main() {
    const std::string file_a_name = "A.txt";
    const std::string file_b_name = "B.txt";

    // Load data into memory once
    std::vector<RowA> table_a = read_table_a(file_a_name);
    std::vector<RowB> table_b = read_table_b(file_b_name);

    if (table_a.empty()) {
        std::cerr << "Table 1 issue!" << std::endl;
        return 1;
    }
    if (table_b.empty()) {
        std::cerr << "Table 2 issue!" << std::endl;
        return 1;
    }

    // --- Method 1: HashJoin-Then-Aggregation ---
    auto start1 = std::chrono::high_resolution_clock::now();
    
    std::vector<JoinedRow> joined_table = hash_join(table_a, table_b);
    std::vector<AggregatedResult> final_results_1 = perform_aggregation(joined_table);
    
    auto end1 = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double, std::milli> duration1 = end1 - start1;


    // --- Method 2: GroupJoin (Pre-Aggregation) ---
    auto start2 = std::chrono::high_resolution_clock::now();
    
    std::vector<AggregatedResult> final_results_2 = pre_aggregation_join(table_a, table_b);

    auto end2 = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double, std::milli> duration2 = end2 - start2;
    

    // --- Save and Display Results ---
    // save_results("As.txt", final_results_1);
    std::cout << "Execution Time (HashJoin-Then-Aggregation): " << duration1.count() << " ms" << std::endl;

    // save_results("Bs.txt", final_results_2);
    std::cout << "Execution Time (GroupJoin): " << duration2.count() << " ms" << std::endl;

    if (duration2.count() > 0) {
         std::cout << "Speed Up: " << duration1.count() / duration2.count() << std::endl;
    }else{
        std::cout << "Fatal Error:" << std::endl;
    }

    save_results("As.txt", final_results_1);
    save_results("Bs.txt", final_results_2);

    return 0;
}
