//
// Created by Aneesh Neelam on 11/07/15.
//

#include <include/leveldb/env.h>

namespace leveldb {

    class ModSequentialFile: public SequentialFile {

    };

    class ModRandomAccessFile: public RandomAccessFile {

    };

    class ModWritableFile: public WritableFile {

    };

    class ModEnv: public Env {

    };
}
