#include <time.h>
#include "util.h"

namespace util {
    double getTime() {
        struct timespec tp1;
        clock_gettime(CLOCK_REALTIME, &tp1);
        double theseSecs = tp1.tv_sec + tp1.tv_nsec / 1e9;
        return theseSecs * 1000;
    }
}