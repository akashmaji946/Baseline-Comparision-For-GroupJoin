#include <iostream>
#include <fstream>
#include <sstream>
#include <vector>
#include <string>
#include <unordered_map>
#include <chrono>
#include <algorithm> // Required for std::sort

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

// --- Pre-Aggregation Method (Optimized) ---

/**
 * @brief Performs a join and aggregation using a pre-aggregation strategy.
 * This is more memory-efficient as it avoids materializing the full join result.
 * @param file_a The filename for the left table (A).
 * @param file_b The filename for the right table (B).
 * @return A vector of AggregatedResult structs.
 */
std::vector<AggregatedResult> pre_aggregation_join(const std::string& file_a, const std::string& file_b) {
    std::ifstream file;
    std::string line;

    // 1. Read table A and pre-aggregate sums of 'v' for each key 'k'.
    std::unordered_map<int, long long> pre_agg_a; // Use long long for sum to prevent overflow
    file.open(file_a);
    if (!file.is_open()) {
        std::cerr << "Error: Could not open file " << file_a << std::endl;
        return {};
    }
    while (std::getline(file, line)) {
        std::vector<std::string> tokens = parse_csv_line(line);
        if (tokens.size() == 4) {
            try {
                int k = std::stoi(tokens[0]);
                int v = std::stoi(tokens[1]);
                pre_agg_a[k] += v;
            } catch (const std::invalid_argument& ia) { /* ignore parse errors on this line */ }
        }
    }
    file.close();

    // 2. Read table B and count occurrences of each key 'k'.
    std::unordered_map<int, int> key_counts_b;
    file.open(file_b);
     if (!file.is_open()) {
        std::cerr << "Error: Could not open file " << file_b << std::endl;
        return {};
    }
    while (std::getline(file, line)) {
        std::vector<std::string> tokens = parse_csv_line(line);
        if (tokens.size() == 5) {
            try {
                int k = std::stoi(tokens[1]);
                key_counts_b[k]++;
            } catch (const std::invalid_argument& ia) { /* ignore parse errors on this line */ }
        }
    }
    file.close();

    // 3. Join the aggregated results.
    std::vector<AggregatedResult> final_result;
    for(const auto& b_pair : key_counts_b) {
        int k = b_pair.first;
        int count_in_b = b_pair.second;

        // Find the matching key in the pre-aggregated map from table A
        auto a_it = pre_agg_a.find(k);
        if (a_it != pre_agg_a.end()) {
            // If match found, multiply the pre-calculated sum from A by the count from B
            long long sum_in_a = a_it->second;
            long long final_sum = sum_in_a * count_in_b;
            final_result.push_back({k, final_sum});
        }
    }

    return final_result;
}


/**
 * @brief Displays the final aggregated results to the console.
 * @param title A title for the output.
 * @param results The vector of AggregatedResult structs to display.
 */
void display_results(const std::string& title, const std::vector<AggregatedResult>& results) {
    std::cout << "\n--- " << title << " ---" << std::endl;
    std::cout << "k\t|\tsumm" << std::endl;
    std::cout << "--------------------------------" << std::endl;
    // Create a copy to sort for display without modifying the original vector
    std::vector<AggregatedResult> sorted_results = results;
    std::sort(sorted_results.begin(), sorted_results.end(), [](const AggregatedResult& a, const AggregatedResult& b){
        return a.k < b.k;
    });

    for (const auto& row : sorted_results) {
        std::cout << row.k << "\t|\t" << row.sum_v << std::endl;
    }
}

/**
 * @brief Sorts and saves the aggregated results to a CSV file.
 * @param filename The name of the output file.
 * @param results The vector of AggregatedResult structs to save.
 */
void save_results(const std::string& filename, const std::vector<AggregatedResult>& results) {
    // Create a copy to sort for saving without modifying the original vector
    std::vector<AggregatedResult> sorted_results = results;

    // Sort the results by key 'k' for consistent output
    std::sort(sorted_results.begin(), sorted_results.end(), [](const AggregatedResult& a, const AggregatedResult& b){
        return a.k < b.k;
    });

    // Open the output file
    std::ofstream output_file(filename);
    if (!output_file.is_open()) {
        std::cerr << "Error: Could not open file for writing: " << filename << std::endl;
        return;
    }

    // Write header
    output_file << "k,summ\n";

    // Write the sorted data
    for (const auto& row : sorted_results) {
        output_file << row.k << "," << row.sum_v << "\n";
    }

    output_file.close();
    std::cout << "\nResults successfully saved to " << filename << std::endl;
}


int main() {
    const std::string file_a_name = "A.txt";
    const std::string file_b_name = "B.txt";

    // Start the timer
    auto start = std::chrono::high_resolution_clock::now();
    
    // Perform the optimized pre-aggregation join
    std::vector<AggregatedResult> final_results = pre_aggregation_join(file_a_name, file_b_name);

    // Stop the timer
    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double, std::milli> duration = end - start;
    
    // Check if the operation was successful before proceeding
    if (final_results.empty()) {
        std::cerr << "Operation failed or produced no results. Check if input files exist and are not empty." << std::endl;
        return 1;
    }

    // Display the final results to the console
    // display_results("Final Results (Pre-Aggregation)", final_results);
    // Save the final results to a file
    save_results("results.txt", final_results);

    std::cout << "\nTotal Execution Time: (Pre-Aggregation) => " << duration.count() << " ms" << std::endl;

    return 0;
}
