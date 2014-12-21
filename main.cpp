#include <iostream>
#include <bits/stl_algo.h>
#include "BigFile.h"

using namespace std;

typedef uint32_t elem_t;
const size_t elemSize = sizeof(elem_t);
const elem_t LAST_ELEM = UINT32_MAX;
const elem_t END_OF_STREAM = 0;

const size_t B = 10000000;
const size_t B_BYTES = B * elemSize;
const size_t M_BYTES = 1 * 1024 * 1024 * 1536; //1.5GB
const size_t M = M_BYTES / elemSize;

class Layers {

public:
    Layers(size_t s) : s(s) {
    }

    virtual ~Layers() {
    }

    void add(vector<elem_t> elems) {
        if (elems.size() != s) {
            throw "Wrong layer size";
        }
        sort(elems.begin(), elems.end());
        vector<elem_t> filteredElems = filterElems(elems);
        addLayer(filteredElems, 0);
    }

    vector<elem_t> flush() {

    }

    size_t size() {
        return layers.size();
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
    size_t s;

    void addLayer(vector<elem_t> elems, size_t layer_num) {
        if (layers.size() < layer_num) {
            throw "Layer does not exist";
        } else if (layers.size() == layer_num) {
            layers.push_back(elems);
        } else {
            vector<elem_t> existingElems = layers[layer_num];
            if (existingElems.size() == 0) {
                layers[layer_num] = elems;
            } else {
                layers[layer_num] = vector<elem_t>();
                vector<elem_t> result = filterElems(merge(existingElems, elems));
                addLayer(result, layer_num + 1);
            }
        }
    }

    vector<elem_t> filterElems(vector<elem_t> elems) {
        size_t size = elems.size();
        vector<elem_t> layer(size / 2);
        for (size_t i = 1; i < size; i += 2) {
            layer[i / 2] = elems[i];
        }
        return layer;
    }

    vector<elem_t> merge(vector<elem_t> e1, vector<elem_t> e2) {
        vector<elem_t> result;
        int i1 = 0, i2 = 0;
        while (i1 < e1.size() || i2 < e2.size()) {
            if ((e1[i1] <= e2[i2] && i1 < e1.size()) || i2 >= e2.size()) {
                result.push_back(e1[i1++]);
            }
            else {
                result.push_back(e2[i2++]);
            }
        }
        return result;
    }
};

void execute_pass(BigFile<elem_t> stream, elem_t *first, elem_t *last, size_t *count) {
    uint64_t N = stream.get_N();
    size_t s = (size_t) (M / log(N));
    Layers layers(s);
    stream.startRead();
    elem_t elem;
    vector<elem_t> elems(s, LAST_ELEM);
    size_t i = 0;
    while ((elem = stream.read()) != END_OF_STREAM) {
        if (elem < *first || elem > *last) {
            continue;
        }
        elems[i++] = elem;
        (*count)++;
        if (i == s) {
            layers.add(elems);
            elems = vector<elem_t>(s, LAST_ELEM);
            i = 0;
        }
    }
    layers.add(elems);
    stream.finishRead();
}

elem_t inmemory_k_seq(BigFile<elem_t> stream, elem_t first, elem_t last, size_t k) {
    elem_t elem;
    vector<elem_t> elems;
    stream.startRead();
    while ((elem = stream.read()) != END_OF_STREAM) {
        if (elem >= first && elem <= last) {
            elems.push_back(elem);
        }
    }
    stream.finishRead();
    sort(elems.begin(), elems.end());
    return elems.at(k);
}

elem_t find_k_seq(BigFile<elem_t> stream, size_t k) {
    elem_t first = 0;
    elem_t last = LAST_ELEM;
    size_t count = 0;
    execute_pass(stream, &first, &last, &count);
    elem_t k_seq = inmemory_k_seq(stream, first, last, k);
    return k_seq;
}

int main(int argc, char **argv) {
    char *filename = argv[1];
    size_t k = (size_t) atoi(argv[2]);
    BigFile<elem_t> stream(filename, false, B);
    elem_t k_seq = find_k_seq(stream, k);
    cout << "k=" << k << " K_seq=" << k_seq << endl;
    return 0;
}