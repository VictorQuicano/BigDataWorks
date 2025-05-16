#include <unordered_set>
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
namespace fs = std::filesystem;

// Estructura para representar un trabajo a procesar
struct WorkItem {
    string file_path;  // Ruta del archivo
    size_t chunk_id;   // ID del chunk dentro del archivo
    string content;    // Contenido del chunk

    WorkItem() {}
    
    WorkItem(const string& path, size_t id, const string& data) 
        : file_path(path), chunk_id(id), content(data) {}
};

class ThreadSafeQueue {
private:
    queue<WorkItem> queue_t;
    std::mutex mutex;
    condition_variable cv;
    bool finished = false;
    size_t current_size = 0;

public:
    void push(const WorkItem& item) {
        unique_lock<std::mutex> lock(mutex);
        queue_t.push(item);
        current_size++;
        cv.notify_one();
    }

    bool pop(WorkItem& item) {
        unique_lock<std::mutex> lock(mutex);
        cv.wait(lock, [this] { return !queue_t.empty() || finished; });
        
        if (queue_t.empty() && finished) {
            return false;
        }
        
        item = queue_t.front();
        queue_t.pop();
        current_size--;
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
    
    size_t size() {
        unique_lock<std::mutex> lock(mutex);
        return current_size;
    }
};

class GlobalInvertedIndex {
private:
    unordered_map<string, unordered_set<string>> index;
    std::mutex mutex;
    size_t max_memory_words;
    string temp_dir;
    size_t temp_file_counter = 0;
    vector<string> temp_files;

public:
    GlobalInvertedIndex(size_t max_words = 5000000, const string& tmp_dir = "") 
        : max_memory_words(max_words), temp_dir(tmp_dir) {
        // Si no se especifica un directorio temporal, usar el directorio actual
        if (temp_dir.empty()) {
            temp_dir = fs::temp_directory_path().string();
        }
        
        // Asegurarse de que el directorio temporal existe
        if (!fs::exists(temp_dir)) {
            try {
                fs::create_directories(temp_dir);
            } catch (const exception& e) {
                cerr << "Failed to create temp directory: " << e.what() << endl;
                temp_dir = ".";
            }
        }
    }

    ~GlobalInvertedIndex() {
        // Limpiar archivos temporales al destruir el objeto
        for (const auto& file : temp_files) {
            try {
                if (fs::exists(file)) {
                    fs::remove(file);
                }
            } catch (const exception& e) {
                cerr << "Failed to remove temp file: " << e.what() << endl;
                temp_dir = ".";
            }
        }
    }

    void merge(const unordered_map<string, unordered_set<string>>& local_index) {
        unique_lock<std::mutex> lock(mutex);
        
        // Añadir el índice local al global
        for (const auto& [word, doc_ids] : local_index) {
            index[word].insert(doc_ids.begin(), doc_ids.end());
        }
        
        // Si el índice global es demasiado grande, guardarlo en un archivo temporal
        if (index.size() > max_memory_words) {
            flush_to_temp_file();
        }
    }
    
    void flush_to_temp_file() {
        if (index.empty()) return;
        
        string temp_filename = temp_dir + "/index_temp_" + to_string(temp_file_counter++) + ".tmp";
        ofstream temp_file(temp_filename);
        
        if (!temp_file.is_open()) {
            cerr << "Failed to open temp file: " << temp_filename << endl;
            return;
        }
        
        // Escribir el índice actual al archivo temporal
        for (const auto& [word, docs] : index) {
            temp_file << word;
            for (const auto& doc : docs) {
                temp_file << " " << doc;
            }
            temp_file << "\n";
        }
        
        temp_file.close();
        temp_files.push_back(temp_filename);
        
        // Limpiar el índice en memoria
        index.clear();
        
        cout << "\nFlushed index to temporary file: " << temp_filename << endl;
        cout << "Current memory usage reduced." << endl;
    }

    void write_to_file(const string& filename) {
        // Si hay archivos temporales, primero combinar todo
        if (!temp_files.empty()) {
            merge_temp_files();pop
        }
        
        ofstream file(filename);
        if (!file.is_open()) {
            cerr << "Failed to open output file: " << filename << endl;
            return;
        }

        for (const auto& [word, docs] : index) {
            file << word;
            for (const auto& doc : docs) {
                file << " " << doc;
            }
            file << "\n";
        }

        file.close();
    }
    
    void merge_temp_files() {
        cout << "\nMerging " << temp_files.size() << " temporary files..." << endl;
        
        // Si tenemos demasiados archivos, los combinamos iterativamente
        while (temp_files.size() > 10) { // Establecemos un límite arbitrario
            vector<string> new_temp_files;
            
            for (size_t i = 0; i < temp_files.size(); i += 10) {
                size_t end = min(i + 10, temp_files.size());
                string new_temp = temp_dir + "/index_merged_" + to_string(temp_file_counter++) + ".tmp";
                
                merge_files(vector<string>(temp_files.begin() + i, temp_files.begin() + end), new_temp);
                new_temp_files.push_back(new_temp);
                
                // Eliminar archivos temporales que ya se han combinado
                for (size_t j = i; j < end; ++j) {
                    fs::remove(temp_files[j]);
                }
            }
            
            temp_files = new_temp_files;
        }
        
        // Combinar los archivos temporales restantes con el índice en memoria
        for (const auto& temp_file : temp_files) {
            merge_file_to_memory(temp_file);
            fs::remove(temp_file);
        }
        
        temp_files.clear();
    }
    
    void merge_files(const vector<string>& files, const string& output) {
        // Mapa temporal para combinar índices
        unordered_map<string, unordered_set<string>> merged_index;
        
        for (const auto& file : files) {
            ifstream in(file);
            if (!in.is_open()) continue;
            
            string line;
            while (getline(in, line)) {
                istringstream iss(line);
                string word;
                iss >> word;
                
                string doc_id;
                while (iss >> doc_id) {
                    merged_index[word].insert(doc_id);
                }
            }
            
            in.close();
        }
        
        // Escribir el índice combinado
        ofstream out(output);
        if (!out.is_open()) {
            cerr << "Failed to open merged temp file: " << output << endl;
            return;
        }
        
        for (const auto& [word, docs] : merged_index) {
            out << word;
            for (const auto& doc : docs) {
                out << " " << doc;
            }
            out << "\n";
        }
        
        out.close();
    }
    
    void merge_file_to_memory(const string& file_path) {
        ifstream in(file_path);
        if (!in.is_open()) return;
        
        string line;
        while (getline(in, line)) {
            istringstream iss(line);
            string word;
            iss >> word;
            
            string doc_id;
            while (iss >> doc_id) {
                index[word].insert(doc_id);
            }
        }
        
        in.close();
    }

    size_t get_total_words() const {
        return index.size() + (temp_files.size() * max_memory_words / 2); // Estimación
    }
};

void process_chunk(ThreadSafeQueue& queue, GlobalInvertedIndex& global_index, 
    atomic<bool>& stop_flag) {
    WorkItem item;

    while (!stop_flag && queue.pop(item)) {
        const string& chunk = item.content;
        
        // Crear un identificador único para el documento basado en la ruta del archivo y el chunk_id
        fs::path path(item.file_path);
        string doc_id = path.filename().string() + "_chunk_" + to_string(item.chunk_id);
        
        unordered_map<string, unordered_set<string>> local_index;
        istringstream stream(chunk);
        string word;
    
        while (stream >> word) {
            size_t start = 0, end = word.size();
            while (start < end && ispunct(static_cast<unsigned char>(word[start]))) ++start;
            
            while (end > start && ispunct(static_cast<unsigned char>(word[end - 1]))) --end;
            if (start < end) {
                string clean_word = word.substr(start, end - start);
                transform(clean_word.begin(), clean_word.end(), clean_word.begin(), ::tolower);
                local_index[clean_word].insert(doc_id);
            }
        }
        
        global_index.merge(local_index);
    }
}

// HELPERS OF OUTPUT
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
        cerr << "Usage: " << argv[0] << " <input_directory> <output_file> [chunk_size_MB] [num_threads] [max_memory_words]" << endl;
        return 1;
    }
    
    string input_directory = argv[1];
    string output_file = argv[2];
    
    // Default values
    size_t chunk_size_mb = (argc > 3) ? stoul(argv[3]) : 100; // Default 100MB
    size_t chunk_size = chunk_size_mb * 1024 * 1024;
    
    size_t num_threads = (argc > 4) ? stoul(argv[4]) : thread::hardware_concurrency();
    if (num_threads == 0) num_threads = 4; // Fallback if hardware_concurrency returns 0
    
    size_t max_memory_words = (argc > 5) ? stoul(argv[5]) : 5000;
    
    // Verificar que el directorio existe
    if (!fs::exists(input_directory) || !fs::is_directory(input_directory)) {
        cerr << "Input directory does not exist or is not a directory: " << input_directory << endl;
        return 1;
    }
    
    // Crear directorio temporal para archivos intermedios
    string temp_dir = fs::temp_directory_path().string() + "/index_temp_" + to_string(chrono::system_clock::now().time_since_epoch().count());
    try {
        fs::create_directories(temp_dir);
    } catch (const exception& e) {
        cerr << "Failed to create temp directory: " << e.what() << endl;
        temp_dir = fs::path(output_file).parent_path().string() + "/temp_files";
        try {
            fs::create_directories(temp_dir);
        } catch (...) {
            temp_dir = "."; // Usar directorio actual como último recurso
        }
    }
    
    // Calcular el tamaño total de todos los archivos en el directorio
    uint64_t total_size = 0;
    size_t total_files = 0;
    
    try {
        for (const auto& entry : fs::recursive_directory_iterator(input_directory)) {
            if (fs::is_regular_file(entry)) {
                total_size += fs::file_size(entry);
                total_files++;
            }
        }
    } catch (const exception& e) {
        cerr << "Error scanning directory: " << e.what() << endl;
        return 1;
    }
    
    cout << "Processing directory: " << input_directory << endl;
    cout << "Total files found: " << total_files << endl;
    cout << "Total size: " << format_bytes(total_size) << endl;
    cout << "Chunk size: " << format_bytes(chunk_size) << endl;
    cout << "Using " << num_threads << " threads" << endl;
    cout << "Max words in memory: " << format_number(max_memory_words) << endl;
    cout << "Temporary directory: " << temp_dir << endl;
    
    auto start_time = chrono::high_resolution_clock::now();
    
    ThreadSafeQueue chunk_queue;
    GlobalInvertedIndex global_index(max_memory_words, temp_dir);
    atomic<bool> stop_flag(false);
    atomic<size_t> progress_bytes(0);
    atomic<size_t> total_files_processed(0);
    
    // Crear hilos para procesar chunks
    vector<thread> processing_threads;
    for (size_t i = 0; i < num_threads; ++i) {
        processing_threads.emplace_back(process_chunk, ref(chunk_queue), ref(global_index), ref(stop_flag));
    }
    
    // Limitar la cola para evitar uso excesivo de memoria
    const size_t max_queue_size = 50;  // Ajustar según necesidades
    
    // Hilo para mostrar progreso
    thread progress_thread([&]() {
        while (!stop_flag) {
            auto elapsed = chrono::duration_cast<chrono::seconds>(
                chrono::high_resolution_clock::now() - start_time).count();
            
            if (elapsed > 0) {
                double percentage = total_size > 0 ? static_cast<double>(progress_bytes) / total_size * 100.0 : 0;
                double speed_mbps = static_cast<double>(progress_bytes) / (1024.0 * 1024.0) / elapsed;
                
                cout << "\rProgress: " << fixed << setprecision(2) << percentage << "% "
                     << "(" << format_bytes(progress_bytes) << " / " << format_bytes(total_size) << ") - "
                     << speed_mbps << " MB/s - "
                     << "Files: " << total_files_processed << "/" << total_files << " - "
                     << "Words: " << format_number(global_index.get_total_words()) << " - "
                     << "Time: " << elapsed << "s" << flush;
            }
            
            this_thread::sleep_for(chrono::seconds(1));
        }
    });
    
    try {
        // Leer archivos secuencialmente para evitar sobrecarga de memoria
        vector<fs::path> file_list;
        
        // Recopilar todos los archivos regulares
        for (const auto& entry : fs::recursive_directory_iterator(input_directory)) {
            if (fs::is_regular_file(entry)) {
                file_list.push_back(entry.path());
            }
        }
        
        cout << "\nStarting file processing..." << endl;
        
        for (const auto& file_path : file_list) {
            if (stop_flag) break;
            
            try {
                // Procesar archivo en lotes de chunks
                ifstream file(file_path, ios::binary);
                if (!file.is_open()) {
                    cerr << "\nFailed to open input file: " << file_path << endl;
                    continue;
                }
                
                auto file_size = fs::file_size(file_path);
                vector<char> buffer;
                buffer.reserve(chunk_size + 1024);
                
                size_t chunk_id = 0;
                string leftover;
                
                while (file && !stop_flag) {
                    // Control de flujo básico para evitar sobrecargar la cola
                    while (chunk_queue.size() > max_queue_size && !stop_flag) {
                        this_thread::sleep_for(chrono::milliseconds(100));
                    }
                    
                    buffer.resize(chunk_size + 1);
                    file.read(buffer.data(), chunk_size + 1);
                    streamsize bytes_read = file.gcount();
                    
                    if (bytes_read <= 0) break;
                    
                    // Actualizar bytes procesados
                    progress_bytes.fetch_add(bytes_read);
                    
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
                    
                    chunk_queue.push(WorkItem(file_path.string(), chunk_id++, chunk));
                }
                
                // Handle any remaining leftover
                if (!leftover.empty() && !stop_flag) {
                    chunk_queue.push(WorkItem(file_path.string(), chunk_id++, leftover));
                }
                
                total_files_processed.fetch_add(1);
                
            } catch (const exception& e) {
                cerr << "\nError processing file " << file_path << ": " << e.what() << endl;
            }
        }
        
        // Señalar que hemos terminado de leer todos los archivos
        chunk_queue.finish();
        
        // Esperar a que terminen todos los workers
        for (auto& thread : processing_threads) {
            thread.join();
        }
        
        // Detener el hilo de progreso
        stop_flag = true;
        if (progress_thread.joinable()) {
            progress_thread.join();
        }
        
        // Escribir resultados finales
        cout << "\nWriting final results to " << output_file << "..." << endl;
        global_index.write_to_file(output_file);
        
        fs::remove_all(temp_dir);
        
        auto end_time = chrono::high_resolution_clock::now();
        auto duration = chrono::duration_cast<chrono::seconds>(end_time - start_time).count();
        
        cout << "\nProcessing complete!" << endl;
        cout << "Total unique words indexed: " << global_index.get_total_words() << endl;
        cout << "Total time: " << duration << " seconds" << endl;
        
    } catch (const exception& e) {
        cerr << "\nError: " << e.what() << endl;
        stop_flag = true;
        
        if (progress_thread.joinable()) {
            progress_thread.join();
        }
        
        // Esperar a que terminen todos los workers
        chunk_queue.finish();
        for (auto& thread : processing_threads) {
            if (thread.joinable()) thread.join();
        }
        
        fs::remove_all(temp_dir);
        
        return 1;
    }
    
    return 0;
}