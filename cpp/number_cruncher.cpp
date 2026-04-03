#include <iostream>
#include <cstdlib>
#include <chrono>
#include <string>
#include <sstream>

// --- THE MAP PHASE ---
void run_map_task(int chunk_index) {
    std::cout << "[C++ MAP Worker " << chunk_index << "] Executing heavy math...\n";
    double sum = 0;
    for(long long i = 0; i < 500000000; ++i) { sum += (i * 0.001); }
    std::cout << "🚨 [C++ MAP Worker " << chunk_index << "] Result: " << sum << "\n";
}

// --- THE GATHER PHASE ---
void run_gather_task(const std::string& input_uris) {
    std::cout << "🌐 [C++ GATHER Worker] Booted. Received Inputs: " << input_uris << "\n";
    
    std::stringstream ss(input_uris);
    std::string item;
    double total_sum = 0;
    
    while (std::getline(ss, item, ',')) {
        if (item.empty()) continue; // 🚨 Bulletproof: Skip empty strings
        try {
            total_sum += std::stod(item);
        } catch (...) {
            std::cout << "❌ Error parsing chunk: " << item << "\n";
        }
    }
    
    std::cout << "✅ [C++ GATHER Worker] DAG Complete. Final Sum: " << total_sum << "\n";
}

int main() {
    // Look for the explicit JOB_TYPE injected by Go
    const char* job_type = std::getenv("JOB_TYPE");
    
    if (job_type != nullptr && std::string(job_type) == "GATHER") {
        const char* inputs_env = std::getenv("INPUT_URIS");
        std::string inputs = inputs_env ? std::string(inputs_env) : "";
        run_gather_task(inputs);
    } else {
        const char* chunk_env = std::getenv("CHUNK_INDEX");
        int chunk_index = chunk_env ? std::atoi(chunk_env) : 0;
        run_map_task(chunk_index);
    }

    return 0;
}