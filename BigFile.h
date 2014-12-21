#include <iostream>
#include <bits/stl_algo.h>
#include <sstream>
#include <string.h>
#include "util.h"

template<class elem_t>
class BigFile {
public:
    static BigFile<elem_t> *getTempFile(std::string tempPath, size_t B) {
        static long tempfileCounter = 0;
        std::string name;
        std::stringstream strstream;
        strstream << tempPath << "temp_" << tempfileCounter;
        strstream << ".bin";
        strstream >> name;
        tempfileCounter++;
        return new BigFile(name, true, B);
    }

    BigFile(std::string name, bool isTemp, size_t B) : filename(name), isTemp(isTemp), B(B) {
        bool exists = false;
        if (FILE *test = fopen64(filename.data(), "r")) {
            fclose(test);
            exists = true;
        }
        file = fopen64(filename.data(), exists ? "r+b" : "w+b");
        if (!file) {
            std::cout << strerror(errno) << std::endl;
            throw errno;
        }
        elemSize = sizeof(elem_t);
        B_BYTES = B * elemSize;
    }

    virtual ~BigFile() {
        fclose(file);
        if (isTemp) {
            remove(filename.data());
        }
    }

    void writeB(void *ptr, size_t size, uint64_t pos) {
        double startTime = util::getTime();
        fseeko64(file, pos * B_BYTES, SEEK_SET);
        fwrite(ptr, 1, size * elemSize, file);
        ioTime += (util::getTime() - startTime);
    }

    void startWrite(uint64_t pos) {
        buffer = (elem_t *) malloc(B_BYTES);
        position = pos;
        buffer_size = 0;
    }

    void write(elem_t e) {
        buffer[buffer_size++] = e;
        if (buffer_size == B) {
            writeB(buffer, buffer_size, position++);
            buffer_size = 0;
        }
    }

    void finishWrite() {
        if (buffer_size > 0) {
            writeB(buffer, buffer_size, position);
        }
        free(buffer);
    }

    void startRead() {
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

    void finishRead() {
        free(buffer);
    }

    size_t readB(void *ptr, uint64_t pos) {
        double startTime = util::getTime();
        fseeko64(file, pos * B_BYTES, SEEK_SET);
        size_t readCount = fread(ptr, elemSize, B, file);
        ioTime += (util::getTime() - startTime);
        return readCount;
    }

    uint64_t readMore(void *ptr, uint64_t pos, size_t count) {
        uint64_t readElems = 0;
        for (size_t i = 0; i < count; i++) {
            size_t read_count = readB(ptr + readElems * elemSize, pos++);
            if (read_count == 0) {
                break;
            }
            readElems += read_count;
        }
        return readElems;
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
        startRead();
        size_t readTo = (size_t) (maxItems ? maxItems : get_N());
        printf("[");
        for (size_t i = 0; i < readTo; i++) {
            std::cout << " " << read() << " ";
        }
        printf("]\n");
        finishRead();
    }

    void addAccumuatedIoTime(double t) {
        accumulatedIoTime += t;
    }

    double getTotalIoTime() const {
        return ioTime + accumulatedIoTime;
    }

    std::string getFilename() const {
        return filename;
    }

private:
    FILE *file;
    std::string filename;
    size_t B_BYTES;
    size_t B;
    size_t elemSize;
    elem_t *buffer;
    uint64_t position;
    size_t buffer_size;
    size_t current_index;
    double ioTime = 0;
    double accumulatedIoTime = 0;
    bool isTemp = false;
};