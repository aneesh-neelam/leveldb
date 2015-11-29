//
// Created by Aneesh Neelam on 11/07/15.
//

#include <cstring>
#include <deque>

#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <time.h>
#include <unistd.h>

#include <sys/mman.h>
#include <sys/time.h>

#include "data_model.h"
#include "device.h"
#include "leveldb/env.h"
#include "leveldb/slice.h"

namespace leveldb {
    namespace {

        static Status IOError(const std::string &context, int err_number) {
            return Status::IOError(context, strerror(err_number));
        }

        class ModSequentialFile : public SequentialFile {
        private:
            Metafile metafile_;
            Band band_;

            uint64_t start_ = 0;
        public:
            ModSequentialFile(Metafile &metafile, Band &band)
                    : metafile_(metafile), band_(band) { }

            virtual ~ModSequentialFile() { }

            virtual Status Read(size_t n, Slice *result, char *scratch) {
                Status s;
                if (n > metafile_.size) {
                    *result = Slice();
                    s = IOError(metafile_.filename, errno);
                }
                else {
                    *result = Slice(band_.file + start_, n);
                    s = Status::OK();
                }
                return s;
            }

            virtual Status Skip(uint64_t n) {
                if (n > metafile_.size) {
                    return IOError(metafile_.filename, errno);
                }
                start_ = n;
                return Status::OK();
            }
        };

        class ModRandomAccessFile : public RandomAccessFile {
        private:
            Metafile metafile_;
            Band band_;
        public:
            ModRandomAccessFile(Metafile &metafile, Band &band)
                    : metafile_(metafile), band_(band) { }

            virtual ~ModRandomAccessFile() { }

            virtual Status Read(uint64_t offset, size_t n, Slice* result, char* scratch) const {
                Status s;
                if (offset + n > metafile_.size) {
                    *result = Slice();
                    s = IOError(metafile_.filename, errno);
                }
                else {
                    *result = Slice(band_.file + offset, n);
                    s = Status::OK();
                }
                return s;
            }
        };

        class ModWritableFile : public WritableFile {
        private:
            Metaband metaband_;
            Metafile metafile_;
            Band band_;
            std::string filename_;
            uint64_t index_;
        public:
            ModWritableFile(const std::string filename, Metafile &metafile, Band &band, uint64_t index)
                    : filename_(filename), metafile_(metafile), band_(band), index_(index) {
                if (metafile.fileexists == 1) {
                    std::memset(&band_, 0, metafile_.size);
                    metafile_.size = 0;
                    metafile_.index = index;
                }
                metafile_.fileexists = 1;
                metafile_.size = 0;
            }

            virtual ~ModWritableFile() { }

            virtual Status Append(const Slice &data) {
                std::memcpy(band_.file, data.data(), data.size());
                metafile_.size = data.size();
                return Status::OK();
            }

            virtual Status Close() {
                return Status::OK();
            }

            virtual Status Flush() {
                return Status::OK();
            }

            virtual Status Sync() {
                return Status::OK();
            }
        };

        class ModFileLock : public FileLock {
        public:
            Metafile metafile;
        };

        namespace {
            struct StartThreadState {
                void (*user_function)(void *);

                void *arg;
            };
        }

        static void *StartThreadWrapper(void *arg) {
            StartThreadState *state = reinterpret_cast<StartThreadState *>(arg);
            state->user_function(state->arg);
            delete state;
            return NULL;
        }

        class ModEnv : public Env {
        private:
            Device *memblock;

            void PthreadCall(const char *label, int result) {
                if (result != 0) {
                    fprintf(stderr, "pthread %s: %s\n", label, strerror(result));
                    abort();
                }
            }

            void BGThread() {
                while (true) {
                    // Wait until there is an item that is ready to run
                    PthreadCall("lock", pthread_mutex_lock(&mu_));
                    while (queue_.empty()) {
                        PthreadCall("wait", pthread_cond_wait(&bgsignal_, &mu_));
                    }

                    void (*function)(void *) = queue_.front().function;
                    void *arg = queue_.front().arg;
                    queue_.pop_front();

                    PthreadCall("unlock", pthread_mutex_unlock(&mu_));
                    (*function)(arg);
                }
            }

            static void *BGThreadWrapper(void *arg) {
                reinterpret_cast<ModEnv *>(arg)->BGThread();
                return NULL;
            }

            pthread_mutex_t mu_;
            pthread_cond_t bgsignal_;
            pthread_t bgthread_;
            bool started_bgthread_;

            // Entry per Schedule() call
            struct BGItem {
                void *arg;

                void (*function)(void *);
            };

            typedef std::deque<BGItem> BGQueue;
            BGQueue queue_;
        public:
            ModEnv()
                    : started_bgthread_(false) {
                PthreadCall("mutex_init", pthread_mutex_init(&mu_, NULL));
                PthreadCall("cvar_init", pthread_cond_init(&bgsignal_, NULL));

                int fd = open(DEVICE_PATH, O_RDWR);
                memblock = reinterpret_cast<Device *>(mmap(NULL, DEVICE_SIZE, PROT_READ | PROT_WRITE,
                                                           MAP_SHARED | MAP_ANONYMOUS, fd, 0));
                if (memblock == MAP_FAILED) {
                    char msg[] = "Cannot open device\n";
                    fwrite(msg, sizeof(char), sizeof(msg), stderr);
                    abort();
                }
            }

            ~ModEnv() {
                munmap(memblock, DEVICE_SIZE);
                char msg[] = "Destroying ModEnv\n";
                fwrite(msg, 1, sizeof(msg), stderr);
                abort();
            }

            Status NewSequentialFile(const std::string &fname, SequentialFile **result) {
                for (int i; i <= memblock->metaband.endindex; ++i) {
                    if (std::strncmp(fname.c_str(), memblock->metaband.metafiles[i].filename, fname.length()) == 0) {
                        *result = new ModSequentialFile(memblock->metaband.metafiles[i], memblock->bands[i]);
                        return Status::OK();
                    }
                }
                *result = NULL;
                return IOError(fname, errno);
            }

            Status NewRandomAccessFile(const std::string &fname, RandomAccessFile **result) {
                for (int i; i <= memblock->metaband.endindex; ++i) {
                    if (std::strncmp(fname.c_str(), memblock->metaband.metafiles[i].filename, fname.length()) == 0) {
                        *result = new ModRandomAccessFile(memblock->metaband.metafiles[i], memblock->bands[i]);
                        return Status::OK();
                    }
                }
                *result = NULL;
                return IOError(fname, errno);
            }

            Status NewWritableFile(const std::string &fname, WritableFile **result) {
                int index = 0;
                bool fragmented = false;
                for (int i = 0; i <= memblock->metaband.endindex; ++i) {
                    if (memblock->metaband.metafiles[index].fileexists == 0) {
                        fragmented = true;
                        index = i;
                    }
                    if (std::strncmp(fname.c_str(), memblock->metaband.metafiles[i].filename, fname.length()) == 0) {
                        *result = new ModWritableFile(fname, memblock->metaband.metafiles[i], memblock->bands[i], i);
                        return Status::OK();
                    }
                }
                if (fragmented == false) {
                    ++memblock->metaband.endindex;
                    *result = new ModWritableFile(fname, memblock->metaband.metafiles[memblock->metaband.endindex], memblock->bands[memblock->metaband.endindex], memblock->metaband.endindex);
                    return Status::OK();
                }
                else {
                    *result = new ModWritableFile(fname, memblock->metaband.metafiles[index], memblock->bands[index], index);
                    return Status::OK();
                }
            }

            bool FileExists(const std::string &fname) {
                for (int i; i <= memblock->metaband.endindex; ++i) {
                    if (std::strncmp(fname.c_str(), memblock->metaband.metafiles[i].filename, fname.length()) == 0) {
                        return (memblock->metaband.metafiles[i].fileexists != 0);
                    }
                }
                return false;
            }

            Status GetChildren(const std::string &dir, std::vector<std::string> *result) {
                return leveldb::Status();
            }

            Status DeleteFile(const std::string &fname) {
                return leveldb::Status();
            }

            Status CreateDir(const std::string &dirname) {
                return leveldb::Status();
            }

            Status DeleteDir(const std::string &dirname) {
                return leveldb::Status();
            }

            Status GetFileSize(const std::string &fname, uint64_t *file_size) {
                return leveldb::Status();
            }

            Status RenameFile(const std::string &src, const std::string &target) {
                return leveldb::Status();
            }

            Status LockFile(const std::string &fname, FileLock **lock) {
                return leveldb::Status();
            }

            Status UnlockFile(FileLock *lock) {
                return leveldb::Status();
            }

            void Schedule(void (*function)(void *), void *arg) {
                PthreadCall("lock", pthread_mutex_lock(&mu_));

                // Start background thread if necessary
                if (!started_bgthread_) {
                    started_bgthread_ = true;
                    PthreadCall(
                            "create thread",
                            pthread_create(&bgthread_, NULL, &ModEnv::BGThreadWrapper, this));
                }

                // If the queue is currently empty, the background thread may currently be
                // waiting.
                if (queue_.empty()) {
                    PthreadCall("signal", pthread_cond_signal(&bgsignal_));
                }

                // Add to priority queue
                queue_.push_back(BGItem());
                queue_.back().function = function;
                queue_.back().arg = arg;

                PthreadCall("unlock", pthread_mutex_unlock(&mu_));
            }

            void StartThread(void (*function)(void *), void *arg) {
                pthread_t t;
                StartThreadState *state = new StartThreadState;
                state->user_function = function;
                state->arg = arg;
                PthreadCall("start thread",
                            pthread_create(&t, NULL, &StartThreadWrapper, state));
            }

            Status GetTestDirectory(std::string *path) {
                return leveldb::Status();
            }

            Status NewLogger(const std::string &fname, Logger **result) {
                return leveldb::Status();
            }

            uint64_t NowMicros() {
                struct timeval tv;
                gettimeofday(&tv, NULL);
                return static_cast<uint64_t>(tv.tv_sec) * 1000000 + tv.tv_usec;
            }

            void SleepForMicroseconds(int micros) {
                usleep(micros);
            }
        };
    }

    static pthread_once_t once = PTHREAD_ONCE_INIT;
    static Env *default_env;

    static void InitDefaultEnv() {
        default_env = new ModEnv();
    }

    Env *Env::Default() {
        pthread_once(&once, InitDefaultEnv);
        return default_env;
    }
}
