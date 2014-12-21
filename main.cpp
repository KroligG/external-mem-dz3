#include <iostream>
#include <bits/stl_algo.h>
#include "BigFile.h"

using namespace std;

typedef uint32_t elem_t;
const size_t elemSize = sizeof(elem_t);
const size_t B = 10000000;
const size_t B_BYTES = B * elemSize;
const size_t M_BYTES = 1 * 1024 * 1024 * 1536; //1.5GB
const size_t M = M_BYTES / elemSize;
const size_t m = M / B;

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
                addLayer(existingElems, layer_num + 1);
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

void find_k_seq(BigFile<elem_t> stream, size_t k) {
    Layers layers;
    stream.startRead();
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
    stream.finishRead();
}

int main() {
    BigFile<elem_t> stream("input.bin", false, B);
    find_k_seq(stream, 5);
    return 0;
}