#include <cstdint>

#define DEVICE_PATH "/dev/sdb"
#define BAND_SIZE 4194304
#define MAX_FILES 1000

class Metadata {
  public:
    bool exists;
    char filename[100];
    size_t size;
    uint64_t band;
};
