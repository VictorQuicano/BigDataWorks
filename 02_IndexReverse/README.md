# Desafío: Implementar un Word Count en C++

Este proyecto consiste en contar palabras en un archivo de texto **de gran tamaño (20GB)** sin utilizar herramientas como MapReduce o Apache Spark, aprovechando procesamiento paralelo en C++ con hilos (`std::thread`).

> [!CAUTION]  
> En caso de leer archivos con saltos de línea o caracteres especiales...(TODO)

---

## 🛠 Generación del archivo de prueba (20GB)

Antes de ejecutar el word count, debes generar el archivo de entrada con `generateDoc20gb.cpp`.

### ✏️ Personaliza los parámetros

Abre `generateDoc20gb.cpp` y modifica las líneas 13 a 19 para configurar el tamaño y las rutas:

```cpp
const size_t BLOCK_SIZE = 1024 * 1024; // 1 MB por bloque
size_t FILE_SIZE_GB = 20;
const size_t FILE_SIZE_BYTES = FILE_SIZE_GB * size_t(1024) * 1024 * 1024;

string input_file = "../00_Inputs/most-common-spanish-words-v5.txt"; // Archivo base de palabras
string output_file = "archivo_20GB.txt";                        // Archivo a generar
string output_dir = "outputs_test";                             // Directorio de salida
```

### 🚀 Compilar y ejecutar

```bash
g++ -std=c++17 -pthread generateDoc20gb.cpp -o generateDoc
./generateDoc
```

---

## 🧮 Contador de palabras (Word Count)

Una vez generado el archivo, ejecuta el word count con múltiples hilos.

### 🚀 Compilar y ejecutar

```bash
g++ -std=c++17 -pthread countWords.cpp -o countWords
./countWords <input_file> <output_file> [chunk_size_MB] [num_threads] [memory_limit_MB]
```

### 📌 Ejemplo

```bash
./countWords outputs_test/archivo_20GB.txt resultados.txt 64 8 4096
```

> Este ejemplo procesará el archivo usando:
>
> - Chunk de 64 MB
> - 8 hilos
> - Límite de memoria de 4 GB

---

## 📁 Estructura del proyecto

```
.
├── generateDoc20gb.cpp      # Generador de archivo grande
├── countWords.cpp           # Lógica de word count multithread
├── inputs/                  # Archivo base de palabras
├── outputs_test/            # Carpeta de salida, NO ESTÁ INCLUDO EN EL REPO
└── README.md                # Este archivo
```

---

## ✅ Requisitos

- C++17 o superior
- Compilador compatible con `std::thread`
- Memoria suficiente para manejar archivos grandes (RAM recomendada: 8GB+)
