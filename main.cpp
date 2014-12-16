#include <iostream>
#include <bits/stl_algo.h>
#include <string.h>

using namespace std;

typedef uint32_t elem_t;
const size_t elemSize = sizeof(elem_t);
const size_t B = 10000000;
const size_t B_BYTES = B * elemSize;
const size_t M_BYTES = 1 * 1024 * 1024 * 1536; //1.5GB
const size_t M = M_BYTES / elemSize;
const size_t m = M / B;

double getTime() {
    struct timespec tp1;
    clock_gettime(CLOCK_REALTIME, &tp1);
    double theseSecs = tp1.tv_sec + tp1.tv_nsec / 1e9;
    return theseSecs * 1000;
}

class DataStream {
public:
    DataStream(string name) : filename(name) {
        bool exists = false;
        if (FILE *test = fopen64(filename.data(), "r")) {
            fclose(test);
            exists = true;
        }
        if (!exists) {
            throw "Stream does not exist";
        }
        file = fopen64(filename.data(), "rb");
        if (!file) {
            cout << strerror(errno) << endl;
            throw errno;
        }
    }

    virtual ~DataStream() {
        fclose(file);
    }

    void open() {
        buffer = (elem_t *) malloc(B_BYTES);
        position = 0;
        buffer_size = 0;
        current_index = 0;
    }

    elem_t read() {
        if (buffer_size == 0) {
            buffer_size = readB(buffer, position++);
            current_index = 0;
        }
        if (buffer_size == 0) throw "Error";
        buffer_size--;
        return buffer[current_index++];
    }

    void close() {
        free(buffer);
    }

    size_t readB(void *ptr, uint64_t pos) {
        double startTime = getTime();
        fseeko64(file, pos * B_BYTES, SEEK_SET);
        size_t readCount = fread(ptr, elemSize, B, file);
        ioTime += (getTime() - startTime);
        return readCount;
    }

    uint64_t get_N() {
        fseeko64(file, 0, SEEK_END);
        __off64_t size = ftello64(file);
        if (size < 0) throw "Error";
        return (uint64_t) (size / elemSize);
    }

    uint64_t get_n() {
        uint64_t N = get_N();
        uint64_t n = N / B;
        if (N % B) {
            n += 1;
        }
        return n;
    }

    void printFile(size_t maxItems) {
        open();
        size_t readTo = (size_t) (maxItems ? maxItems : get_N());
        printf("[");
        for (size_t i = 0; i < readTo; i++) {
            cout << " " << read() << " ";
        }
        printf("]\n");
        close();
    }

    double getTotalIoTime() const {
        return ioTime;
    }

    string getFilename() const {
        return filename;
    }

private:
    FILE *file;
    string filename;
    elem_t *buffer;
    uint64_t position;
    size_t buffer_size;
    size_t current_index;
    double ioTime = 0;
};

class Layers {

public:
    Layers() {
    }

    virtual ~Layers() {
    }

    void addLayer(vector<elem_t> elems, size_t layer_num) {
        vector<elem_t> filteredElems = filterElems(elems);
        if (layers.size() < layer_num) {
            throw "Layer does not exist";
        } else if (layers.size() == layer_num) {
            layers.push_back(filteredElems);
        } else {
            vector<elem_t> existingElems = layers[layer_num];
            if (existingElems.size() == 0) {
                layers[layer_num] = filteredElems;
            } else {
                layers[layer_num] = vector<elem_t>();
                existingElems.insert(existingElems.end(), filteredElems.begin(), filteredElems.end());
                addLayer(existingElems.data(), existingElems.size(), layer_num + 1);
            }
        }
    }

    void print() {
        cout << "{\n";
        for (auto layer : layers) {
            cout << "\t[";
            for (elem_t elem : layer) {
                cout << elem << ", ";
            }
            cout << "]\n";
        }
        cout << "}\n";
    }

private:
    vector<vector<elem_t>> layers;

    vector<elem_t> filterElems(vector<elem_t> elems) {
        sort(elems.begin(), elems.end());
        size_t size = elems.size();
        vector<elem_t> layer(size / 2);
        for (size_t i = 1; i < size; i += 2) {
            layer[i / 2] = elems[i];
        }
        return layer;
    }
};

void find_k_seq(DataStream stream, size_t k) {
    Layers layers;
    stream.open();
    elem_t elem;
    vector<elem_t> elems(m);
    size_t i = 0;
    while ((elem = stream.read())) {
        elems[i++] = elem;
        if (i == m) {
            i = 0;
            layers.addLayer(elems, 0);
        }
    }
    stream.close();
}

int main() {
    DataStream stream("input.bin");
    find_k_seq(stream, 5);
    return 0;
}