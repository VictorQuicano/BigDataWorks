#include <iostream>
#include <fstream>
#include <unordered_map>
#include <string>
#include <vector>
#include <thread>
#include <mutex>
#include <chrono>
#include <algorithm>
#include <atomic>
#include <filesystem>
#include <queue>
#include <condition_variable>
#include <sstream>
#include <iomanip>

#include <unordered_set>

using namespace std;

class ThreadSafeQueue {
private:
    queue<pair<string, size_t>> queue_t;
    std::mutex mutex;
    condition_variable cv;
    bool finished = false;

public:
    void push(const string& data, size_t chunk_id) {
        unique_lock<std::mutex> lock(mutex);
        queue_t.push(make_pair(data, chunk_id));
        cv.notify_one();
    }

    bool pop(pair<string, size_t>& item) {
        unique_lock<std::mutex> lock(mutex);
        cv.wait(lock, [this] { return !queue_t.empty() || finished; });
        
        if (queue_t.empty() && finished) {
            return false;
        }
        
        item = queue_t.front();
        queue_t.pop();
        return true;
    }

    void finish() {
        unique_lock<std::mutex> lock(mutex);
        finished = true;
        cv.notify_all();
    }

    bool is_empty() {
        unique_lock<std::mutex> lock(mutex);
        return queue_t.empty();
    }
};

// Thread-safe map merging
class GlobalWordCount {
private:
    unordered_map<string, uint64_t> counts;
    std::mutex mutex;
    atomic<uint64_t> total_words{0};

public:
    void merge(const unordered_map<string, uint64_t>& local_counts, uint64_t local_total) {
        unique_lock<std::mutex> lock(mutex);
        for (const auto& [word, count] : local_counts) {
            counts[word] += count;
        }
        total_words += local_total;
    }

    void merge_from_file(const string& filename) {
        ifstream file(filename);
        if (!file.is_open()) {
            cerr << "Failed to open intermediate file: " << filename << endl;
            return;
        }

        string line;
        while (getline(file, line)) {
            size_t space_pos = line.rfind(' ');
            if (space_pos != string::npos) {
                string word = line.substr(0, space_pos);
                uint64_t count = stoull(line.substr(space_pos + 1));
                counts[word] += count;
                total_words += count;
            }
        }
        file.close();
    }

    void write_to_file(const string& filename) {
        ofstream file(filename);
        if (!file.is_open()) {
            cerr << "Failed to open output file: " << filename << endl;
            return;
        }

        // Convert to vector for sorting
        vector<pair<string, uint64_t>> sorted_counts(counts.begin(), counts.end());
        sort(sorted_counts.begin(), sorted_counts.end(), 
            [](const auto& a, const auto& b) { return a.second > b.second; });

        for (const auto& [word, count] : sorted_counts) {
            file << word << " " << count << "\n";
        }
        file.close();
    }

    void write_intermediate(const string& filename, size_t memory_limit) {
        unique_lock<std::mutex> lock(mutex);
        
        if (counts.size() <= memory_limit) {
            return;
        }

        ofstream file(filename, ios::app);
        if (!file.is_open()) {
            cerr << "Failed to open intermediate file: " << filename << endl;
            return;
        }

        vector<pair<string, uint64_t>> sorted_counts(counts.begin(), counts.end());
        sort(sorted_counts.begin(), sorted_counts.end(), 
            [](const auto& a, const auto& b) { return a.second > b.second; });

        for (const auto& [word, count] : sorted_counts) {
            file << word << " " << count << "\n";
        }
        file.close();

        if (memory_limit > 0 && sorted_counts.size() > memory_limit) {
            unordered_map<string, uint64_t> new_counts;
            for (size_t i = 0; i < memory_limit / 2; ++i) {
                if (i >= sorted_counts.size()) break;
                new_counts[sorted_counts[i].first] = sorted_counts[i].second;
            }
            counts = move(new_counts);
        }
    }

    uint64_t get_total_words() const {
        return total_words;
    }

    size_t get_unique_words() const {
        return counts.size();
    }
};

void process_chunk(ThreadSafeQueue& queue, GlobalWordCount& global_counts, 
                  const string& temp_file, size_t memory_limit, atomic<bool>& stop_flag) {
    pair<string, size_t> item;
    
    while (!stop_flag && queue.pop(item)) {
        const string& chunk = item.first;
        size_t chunk_id = item.second;
        
        unordered_map<string, uint64_t> local_counts;
        uint64_t local_total = 0;
        
        istringstream stream(chunk);
        string word;
        
        string temp_word;
        while (stream >> word) {
            temp_word = word;
            size_t start = 0;
            size_t end = word.size();
            std::unordered_set<string> valid_words = {"dios", "hasta", "senor", "entonces"};

            while (start < end && ispunct(static_cast<unsigned char>(word[start]))) {
                start++;
            }
            while (end > start && ispunct(static_cast<unsigned char>(word[end - 1]))) {
                end--;
            }
            if (!valid_words.count(word)) {
                cout<<"Palabra no válida: \"" << temp_word << "\"" << endl;
            }
        
            if (start < end) {
                string clean_word = word.substr(start, end - start);
            
                // Convertir a minúsculas (opcional, pero recomendable)
                transform(clean_word.begin(), clean_word.end(), clean_word.begin(), ::tolower);
            
                // Agregar al contador
                local_counts[clean_word]++;
                local_total++;
            }
        }
        
        global_counts.merge(local_counts, local_total);
        
        global_counts.write_intermediate(temp_file, memory_limit);
    }
}

string format_bytes(uint64_t bytes) {
    const char* suffixes[] = {"B", "KB", "MB", "GB", "TB"};
    int suffix_index = 0;
    double size = static_cast<double>(bytes);
    
    while (size >= 1024 && suffix_index < 4) {
        size /= 1024.0;
        suffix_index++;
    }
    
    ostringstream oss;
    oss << fixed << setprecision(2) << size << " " << suffixes[suffix_index];
    return oss.str();
}

string format_number(uint64_t num) {
    ostringstream oss;
    oss << fixed << setprecision(2);
    
    if (num < 1000) {
        oss << num;
    } else if (num < 1000000) {
        oss << (num / 1000.0) << "K";
    } else if (num < 1000000000) {
        oss << (num / 1000000.0) << "M";
    } else {
        oss << (num / 1000000000.0) << "B";
    }
    
    return oss.str();
}

int main(int argc, char* argv[]) {
    if (argc < 3) {
        cerr << "Usage: " << argv[0] << " <input_file> <output_file> [chunk_size_MB] [num_threads] [memory_limit]" << endl;
        return 1;
    }
    
    string input_file = argv[1];
    string output_file = argv[2];
    
    // Default values
    size_t chunk_size_mb = (argc > 3) ? stoul(argv[3]) : 100; // Default 100MB
    size_t chunk_size = chunk_size_mb * 1024 * 1024;
    
    size_t num_threads = (argc > 4) ? stoul(argv[4]) : thread::hardware_concurrency();
    if (num_threads == 0) num_threads = 4; // Fallback if hardware_concurrency returns 0
    
    size_t memory_limit = (argc > 5) ? stoul(argv[5]) : 1000000; // Default 1M unique words
    
    string temp_file = output_file + ".temp";
    
    // Remove temp file if it exists
    if (std::filesystem::exists(temp_file)) {
        std::filesystem::remove(temp_file);
    }
    
    // Check if input file exists and get its size
    if (!std::filesystem::exists(input_file)) {
        cerr << "Input file does not exist: " << input_file << endl;
        return 1;
    }
    
    auto file_size = std::filesystem::file_size(input_file);
    
    cout << "Processing file: " << input_file << endl;
    cout << "File size: " << format_bytes(file_size) << endl;
    cout << "Chunk size: " << format_bytes(chunk_size) << endl;
    cout << "Using " << num_threads << " threads" << endl;
    cout << "Memory limit: " << format_number(memory_limit) << " unique words" << endl;
    
    auto start_time = chrono::high_resolution_clock::now();
    
    // Setup thread-safe structures
    ThreadSafeQueue chunk_queue;
    GlobalWordCount global_counts;
    atomic<bool> stop_flag(false);
    
    // Start worker threads
    vector<thread> threads;
    for (size_t i = 0; i < num_threads; ++i) {
        threads.emplace_back(process_chunk, ref(chunk_queue), ref(global_counts), 
                             ref(temp_file), memory_limit, ref(stop_flag));
    }
    
    // Read file in chunks and queue them for processing
    ifstream file(input_file, ios::binary);
    if (!file.is_open()) {
        cerr << "Failed to open input file: " << input_file << endl;
        stop_flag = true;
        chunk_queue.finish();
        for (auto& thread : threads) {
            if (thread.joinable()) thread.join();
        }
        return 1;
    }
    
    vector<char> buffer;
    buffer.reserve(chunk_size + 1024);

    size_t bytes_processed = 0;
    size_t chunk_id = 0;
    string leftover;
    
    // Progress reporting thread
    atomic<size_t> progress_bytes(0);
    thread progress_thread([&]() {
        while (!stop_flag) {
            auto elapsed = chrono::duration_cast<chrono::seconds>(
                chrono::high_resolution_clock::now() - start_time).count();
            
            if (elapsed > 0) {
                double percentage = static_cast<double>(progress_bytes) / file_size * 100.0;
                double speed_mbps = static_cast<double>(progress_bytes) / (1024.0 * 1024.0) / elapsed;
                
                cout << "\rProgress: " << fixed << setprecision(2) << percentage << "% "
                          << "(" << format_bytes(progress_bytes) << " / " << format_bytes(file_size) << ") - "
                          << speed_mbps << " MB/s - "
                          << "Words: " << format_number(global_counts.get_total_words()) << " - "
                          << "Time: " << elapsed << "s" << flush;
            }
            
            this_thread::sleep_for(chrono::seconds(1));
        }
    });
    
    try {
        while (file) {
            file.read(buffer.data(), chunk_size + 1); // leer chunk_size + 1
            streamsize bytes_read = file.gcount();
                
            if (bytes_read <= 0) break;
                
            bytes_processed += bytes_read;
            progress_bytes = bytes_processed;
                
            // Convertir a string
            string chunk(buffer.data(), bytes_read);
                
            // Añadir leftover anterior si hay
            if (!leftover.empty()) {
                chunk = leftover + chunk;
                leftover.clear();
            }
        
            // Si leímos exactamente chunk_size + 1, verificamos el último carácter
            if (bytes_read == static_cast<streamsize>(chunk_size + 1)) {
                if (!isspace(chunk.back())) {
                    size_t last_space = chunk.find_last_of(" \t\n\r");
                    if (last_space != string::npos) {
                        leftover = chunk.substr(last_space + 1);
                        chunk.resize(last_space + 1);
                    }
                }
            }
        
            chunk_queue.push(chunk, chunk_id++);
        }
        
        // Handle any remaining leftover
        if (!leftover.empty()) {
            chunk_queue.push(leftover, chunk_id++);
        }
        
        // Signal that we're done reading
        chunk_queue.finish();
        
        // Wait for all workers to finish
        for (auto& thread : threads) {
            thread.join();
        }
        
        // Stop the progress thread
        stop_flag = true;
        if (progress_thread.joinable()) {
            progress_thread.join();
        }
        
        // Process any intermediate results
        if (filesystem::exists(temp_file)) {
            cout << "\nMerging intermediate results..." << endl;
            global_counts.merge_from_file(temp_file);
            filesystem::remove(temp_file);
        }
        
        // Write final results
        cout << "\nWriting final results to " << output_file << "..." << endl;
        global_counts.write_to_file(output_file);
        
        auto end_time = chrono::high_resolution_clock::now();
        auto duration = chrono::duration_cast<chrono::seconds>(end_time - start_time).count();
        
        cout << "\nProcessing complete!" << endl;
        cout << "Total words: " << global_counts.get_total_words() << endl;
        cout << "Unique words: " << global_counts.get_unique_words() << endl;
        cout << "Total time: " << duration << " seconds" << endl;
        
    } catch (const exception& e) {
        cerr << "\nError: " << e.what() << endl;
        stop_flag = true;
        if (progress_thread.joinable()) {
            progress_thread.join();
        }
        
        // Wait for all workers to finish
        chunk_queue.finish();
        for (auto& thread : threads) {
            if (thread.joinable()) thread.join();
        }
        
        return 1;
    }
    
    return 0;
}