#include <iostream>
#include <fstream>
#include <sstream>
#include <vector>
#include <string>
#include <unordered_map>
#include <variant>
#include <algorithm>
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
    int sum_v;
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

    // Skip header if it exists
    // std::getline(file, line);

    while (std::getline(file, line)) {
        std::vector<std::string> tokens = parse_csv_line(line);
        if (tokens.size() == 4) {
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

    // Skip header if it exists
    // std::getline(file, line);

    while (std::getline(file, line)) {
        std::vector<std::string> tokens = parse_csv_line(line);
        if (tokens.size() == 5) {
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
    // 1. Build Phase: Create a hash table on the key 'k' from the left table (A)
    std::unordered_map<int, std::vector<RowA>> hash_table;
    for (const auto& row_a : table_a) {
        hash_table[row_a.k].push_back(row_a);
    }
    
    std::vector<JoinedRow> joined_result;

    // 2. Probe Phase: Iterate through the right table (B) and probe the hash table
    for (const auto& row_b : table_b) {
        // Check if the key from table B exists in our hash table
        auto it = hash_table.find(row_b.k);
        if (it != hash_table.end()) {
            // If a match is found, materialize the joined rows
            for (const auto& matching_row_a : it->second) {
                joined_result.push_back({matching_row_a.k, matching_row_a.v, row_b.k});
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
    // Use a map to store the sum for each key 'k'
    std::unordered_map<int, int> aggregation_map;

    for (const auto& row : joined_data) {
        aggregation_map[row.a_k] += row.a_v;
    }

    // Convert the map to the final result vector
    std::vector<AggregatedResult> final_result;
    for (const auto& pair : aggregation_map) {
        final_result.push_back({pair.first, pair.second});
    }

    return final_result;
}


/**
 * @brief Displays the final aggregated results to the console.
 * @param results The vector of AggregatedResult structs to display.
 */
void display_results(const std::string& title, const std::vector<AggregatedResult>& results) {
    std::cout << "\n--- " << title << " ---" << std::endl;
    std::cout << "k\t|\tsumm" << std::endl;
    std::cout << "--------------------------------" << std::endl;
    // Note: Iterating over an unordered_map gives no guarantee of order.
    // For consistent output for comparison, we can sort the results.
    // This is optional and adds a small overhead.
    std::vector<AggregatedResult> sorted_results = results;
    std::sort(sorted_results.begin(), sorted_results.end(), [](const AggregatedResult& a, const AggregatedResult& b){
        return a.k < b.k;
    });

    for (const auto& row : sorted_results) {
        std::cout << row.k << "\t|\t" << row.sum_v << std::endl;
    }
}


int main() {
    // // Create dummy CSV files for demonstration
    // std::ofstream file_a("A.txt");
    // file_a << "300, 25, 'A', 1.5\n";
    // file_a << "300, 25, 'A', 1.5\n";
    // file_a << "300, 25, 'A', 1.5\n";
    // file_a << "300, 25, 'A', 1.5\n";
    // file_a << "400, 20, 'A', 1.5\n";
    // file_a << "400, 20, 'A', 1.5\n";
    // file_a << "400, 20, 'A', 1.5\n";
    // file_a << "600, 50, 'A', 1.5\n";
    // file_a << "600, 10, 'A', 1.5\n";
    // file_a << "600, 30, 'A', 1.5\n";
    // file_a << "600, 10, 'A', 1.5\n";
    // file_a.close();

    // std::ofstream file_b("B.txt");
    // file_b << "1, 300, 1, 'A', 1.5\n";
    // file_b << "1, 300, 2, 'A', 1.5\n";
    // file_b << "1, 400, 3, 'A', 1.5\n";
    // file_b << "1, 400, 4, 'A', 1.5\n";
    // file_b << "1, 400, 5, 'A', 1.5\n";
    // file_b << "1, 600, 6, 'A', 1.5\n";
    // file_b << "1, 600, 7, 'A', 1.5\n";
    // file_b.close();


    // 1. Read the tables from the CSV files
    std::vector<RowA> table_a = read_table_a("A.txt");
    std::vector<RowB> table_b = read_table_b("B.txt");

    if (table_a.empty() || table_b.empty()) {
        std::cerr << "Error reading one or both tables. Exiting." << std::endl;
        return 1;
    }

    // 2. Perform the hash join
    std::vector<JoinedRow> joined_table = hash_join(table_a, table_b);

    // 3. Perform the aggregation
    std::vector<AggregatedResult> final_results = perform_aggregation(joined_table);
    
    // 4. Display the final results
    display_results("Final Results (Join-Aggregation)",final_results);

    return 0;
}
