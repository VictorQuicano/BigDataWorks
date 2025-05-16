#include <iostream>
#include <fstream>
#include <vector>
#include <random>
#include <string>
#include <thread>
#include <mutex>
#include <filesystem>


using namespace std;

const size_t BLOCK_SIZE = 1024 * 1024; // 1 MB
size_t FILE_SIZE_GB = 21;
const size_t FILE_SIZE_BYTES = FILE_SIZE_GB * size_t(1024) * 1024 * 1024;

string input_file = "../00_Inputs/most-common-spanish-words-v5.txt";
//string output_file = "archivo_20GB.txt";
string output_dir = "outputs_test";    

string generarTextoAleatorio(vector<string>& palabras, size_t tamanoBytes, mt19937& gen) {
    string resultado;
    std::uniform_int_distribution<> dist(0, palabras.size() - 1);
    while (resultado.size() < tamanoBytes) {
        resultado += palabras[dist(gen)] + " ";
    }
    return resultado;
}

void getDictionary(vector<string>& palabras) {
    ifstream archivo(input_file);
    if (!archivo) {
        cerr << "No se pudo abrir el archivo de palabras.\n";
        return;
    }
    for(string palabra; getline(archivo, palabra); ) {
        palabras.push_back(palabra);
    }
}

void escribirParte(int thread_id, size_t bytes_por_thread, vector<string>& palabras) {
    string nombre_archivo = output_dir + "/parts/parte_" + to_string(thread_id) + ".txt";
    ofstream archivo(nombre_archivo, std::ios::out | std::ios::binary);

    if (!archivo) {
        cerr << "Error creando archivo " << nombre_archivo << "\n";
        return;
    }

    mt19937 gen(thread_id);

    size_t bloques = bytes_por_thread / BLOCK_SIZE;

    for (size_t i = 0; i < bloques; ++i) {
        string bloque = generarTextoAleatorio(palabras,BLOCK_SIZE, gen);
        archivo.write(bloque.c_str(), bloque.size());
        if (i % 100 == 0) {
            std::cout << "Thread " << thread_id << ": " << i << " MB escritos...\n";
        }
    }

    archivo.close();
}


int main(int argc, char* argv[]) {
    
    vector<string> palabras;
    getDictionary(palabras);

    if(!std::filesystem::exists(output_dir)) {
        std::filesystem::create_directories(output_dir + "/parts");
    }
    int num_threads = thread::hardware_concurrency();
    cout<<"Numero de hilos disponibles: " << num_threads << endl;
    if (num_threads == 0) num_threads = 4;

    std::cout << "Usando " << num_threads << " hilos\n";

    size_t bytes_por_thread = FILE_SIZE_BYTES / num_threads;
    vector<std::thread> hilos;

    for (int i = 0; i < num_threads; ++i) {
        hilos.emplace_back(escribirParte, i, bytes_por_thread, std::ref(palabras));
    }

    for (auto& hilo : hilos) {
        hilo.join();
    }

    std::cout << "¡Archivos generados!\n";
    return 0;
    
}