//
// Created by Aneesh Neelam on 11/14/15.
//

#ifndef LEVELDB_DATA_MODEL_H
#define LEVELDB_DATA_MODEL_H


#include "device.h"

struct Metafile {
  int fileexists = 0;
  char filename[50];
  uint64_t index;
  size_t size;
};

struct Metadirectory {
  char directory[50];
  uint64_t metafileindices[(DEVICE_SIZE / 4) - 5];
};

struct Metaband {
  uint64_t endindex;
  Metadirectory metadirectories[(DEVICE_SIZE / 4) - 5];
  Metafile metafiles[(DEVICE_SIZE / 4) - 5];
};

struct Band {
  char file[BAND_SIZE];
};

struct Device {
  Metaband metaband;
  Band bands[(DEVICE_SIZE / 4) - 5];
};

#endif //LEVELDB_DATA_MODEL_H
