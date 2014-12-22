#include <iostream>
#include <bits/stl_algo.h>
#include "BigFile.h"

using namespace std;

typedef uint32_t elem_t;
const size_t elemSize = sizeof(elem_t);
const elem_t LAST_ELEM = UINT32_MAX;
const elem_t END_OF_STREAM = 0;

const size_t B = 10000000;
//const size_t M_BYTES = 1 * 1024 * 1024 * 1536; //1.5GB
const size_t M_BYTES = 1 * 1024 * 1024 * elemSize;
const size_t M = M_BYTES / elemSize;

class Layers {

public:
    Layers(size_t s) : s(s) {
        layers.push_back(vector<elem_t>(s));
        firstLayerIndex = 0;
    }

    virtual ~Layers() {
    }

    void add(elem_t elem) {
        vector<elem_t> &first = layers[0];
        first[firstLayerIndex++] = elem;
        if (firstLayerIndex == s) {
            addLayer(filterElems(first), 1);
            firstLayerIndex = 0;
        }
    }

    vector<elem_t> flush() {
        if (firstLayerIndex > 0) {
            vector<elem_t> &first = layers[0];
            while (firstLayerIndex != s) {
                first[firstLayerIndex++] = LAST_ELEM;
            }
            addLayer(filterElems(first), 1);
            firstLayerIndex = 0;
        }
        for (size_t i = 1; i < layers.size(); ++i) {
            vector<elem_t> &layer = layers[i];
            if (layer.size() == 0) continue;
            if (layer.size() == s / 2) {
                layer.insert(layer.end(), s / 2, LAST_ELEM);
                addLayer(filterElems(layer), i + 1);
                layers[i] = vector<elem_t>();
            } else {
                throw "Layer size is invalid";
            }
        }
        vector<elem_t> &last = layers[layers.size() - 1];
        if(last.size() == s / 2) {
            last.insert(last.end(), s / 2, LAST_ELEM);
        }
        if(last.size() != s) throw "Last level has wrong size";
        return last;
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
    size_t firstLayerIndex;

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

void execute_pass(BigFile<elem_t> stream, elem_t prev_first, size_t k, elem_t *first, elem_t *last, size_t *count) {
    uint64_t N = stream.get_N();
    size_t s = (((size_t) (M / log(N))) / 2) * 2;
    Layers layers(s);
    stream.startRead();
    elem_t elem;
    *count = 0;
    while ((elem = stream.read()) != END_OF_STREAM) {
        if (elem < *first || elem > *last) {
            continue;
        }
        if(elem > prev_first && elem < *first) {
            (*count)++;
        }
        layers.add(elem);
    }
    stream.finishRead();
    vector<elem_t> result = layers.flush();
    size_t t = layers.size();
    double t2 = pow(2.0, t);
    size_t l = (size_t) (floor(k / t2)) - t;
    size_t u = (size_t) ceil(k / t2);
    *first = result[l];
    *last = result[u];
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
    size_t count;
    execute_pass(stream, first, k, &first, &last, &count);
    k -= count;
    elem_t k_seq = inmemory_k_seq(stream, first, last, k);
    return k_seq;
}

void writeTestFile(string name, size_t size) {
    BigFile<elem_t> test(name, false, B);
    test.startWrite(0);
    for (int i = 1; i < size; ++i) {
        test.write((elem_t) i);
    }
    test.write(END_OF_STREAM);
    test.finishWrite();
}

int main(int argc, char **argv) {
    string filename = argv[1];
    writeTestFile(filename, 10000000);
    size_t k = (size_t) atoi(argv[2]);
    BigFile<elem_t> stream(filename, false, B);
    elem_t k_seq = find_k_seq(stream, k);
    cout << "k=" << k << " K_seq=" << k_seq << endl;
    return 0;
}