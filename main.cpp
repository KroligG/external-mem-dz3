#include <iostream>
#include <bits/stl_algo.h>
#include "BigFile.h"

using namespace std;

typedef uint32_t elem_t;
const size_t elemSize = sizeof(elem_t);
const elem_t LAST_ELEM = UINT32_MAX;
const elem_t END_OF_STREAM = 0;

const size_t B = 10000000;
const uint64_t M_BYTES = 2 * 1024 * 1024 * 1024ull;
const uint64_t M = M_BYTES / elemSize;

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
            sort(first.begin(), first.end());
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
            sort(first.begin(), first.end());
            addLayer(filterElems(first), 1);
            firstLayerIndex = 0;
        }
        for (size_t i = 1; i < layers.size() - 1; ++i) {
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
        if (last.size() == s / 2) {
            last.insert(last.end(), s / 2, LAST_ELEM);
        }
        if (last.size() != s) throw "Last level has wrong size";
        return last;
    }

    size_t size() {
        return layers.size();
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

void execute_pass(BigFile<elem_t> *stream, elem_t prev_first, uint64_t k, elem_t *first, elem_t *last, uint64_t *count) {
    printf("Executing pass\n");
    uint64_t N = stream->get_N();
    size_t s = (size_t) (M / log(N));
    s -= s % 2;
    Layers layers(s);
    stream->startRead();
    elem_t elem;
    *count = 0;
    while ((elem = stream->read()) != END_OF_STREAM) {
        if (elem < *first || elem > *last) {
            continue;
        }
        if (elem > prev_first && elem < *first) {
            (*count)++;
        }
        layers.add(elem);
    }
    stream->finishRead();
    printf("Pass finished. Flushing layers\n");
    vector<elem_t> result = layers.flush();
    size_t t = layers.size() - 1;
    printf("Layers flushed. Size=%ld\n", t);
    double t2 = pow(2.0, t);
    uint64_t l = (uint64_t) (floor(k / t2));
    uint64_t u = (uint64_t) ceil(k / t2);
    if (l >= t) {
        *first = result[l - t];
    }
    *last = result[u];
    printf("New first: %d, last: %d\n", *first, *last);
}

elem_t inmemory_k_seq(BigFile<elem_t> *stream, elem_t first, elem_t last, uint64_t k) {
    printf("Last step. Inmemory k seq. k=%ld\n", k);
    elem_t elem;
    vector<elem_t> elems;
    uint64_t count = 0;
    stream->startRead();
    while ((elem = stream->read()) != END_OF_STREAM) {
        if (elem < first) {
            count++;
            continue;
        }
        if (elem <= last) {
            elems.push_back(elem);
        }
    }
    stream->finishRead();
    printf("Read finished. Sorting elements. size=%ld\n", elems.size());
    sort(elems.begin(), elems.end());
    return elems.at(k - count - 1);
}

elem_t find_k_seq(BigFile<elem_t> *stream, uint64_t k) {
    printf("Find k seq. k=%ld\n", k);
    double startTime = util::getTime();
    elem_t first = 0;
    elem_t last = LAST_ELEM;
    uint64_t count;
    execute_pass(stream, first, k, &first, &last, &count);
    k -= count;
    elem_t k_seq = inmemory_k_seq(stream, first, last, k);
    double allTime = util::getTime() - startTime;
    double ioTime = stream->getTotalIoTime();
    double percentIO = ioTime / allTime * 100;
    printf("Total time:%f, IO: %f (%f%%)\n", allTime, ioTime, percentIO);
    return k_seq;
}

void writeTestFile(string name, uint64_t size) {
    printf("Writing file. Size=%ld\n", size);
    double startTime = util::getTime();
    BigFile<elem_t> *file = new BigFile<elem_t>(name, false, B);
    file->startWrite(0);
//    vector<elem_t> elems(size - 1);
    for (uint64_t i = 1; i < size; ++i) {
        file->write((elem_t) size - i);
//        file->write((elem_t) i);
//        elems[i - 1] = (elem_t) i;
    }
//    srand((unsigned int) startTime);
//    random_shuffle(elems.begin(), elems.end());
//    for(auto e : elems) {
//        file->write(e);
//    }
    file->write(END_OF_STREAM);
    file->finishWrite();
    double allTime = util::getTime() - startTime;
    double ioTime = file->getTotalIoTime();
    double percentIO = ioTime / allTime * 100;
    double speed = ((double) (size * elemSize)) / (1024.0 * 1024.0) / (ioTime / 1000.0);
    printf("Total time:%f, IO: %f (%f%%), speed: %f Mb/s\n", allTime, ioTime, percentIO, speed);
    delete file;
}

int main(int argc, char **argv) {
    string filename = argv[1];
    writeTestFile(filename, 1e9);
    uint64_t k = (uint64_t) atoll(argv[2]);
    BigFile<elem_t> *stream = new BigFile<elem_t>(filename, false, B);
    try{
        elem_t k_seq = find_k_seq(stream, k);
        cout << "k=" << k << " K_seq=" << k_seq << endl;
    } catch(const char* ex) {
        cerr << "Failed with exeption: " << ex << endl;
        exit(1);
    }
    delete stream;

    return 0;
}