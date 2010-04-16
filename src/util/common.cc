#include "util/common.h"
#include "util/file.h"
#include "util/static-initializers.h"

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <signal.h>
#include <execinfo.h>

#include <math.h>

#include <asm/msr.h>
#include <sys/time.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <google/profiler.h>
#include <google/heap-profiler.h>
#include <google/malloc_extension.h>

#include <lzo/lzo1x.h>
#include <mpi.h>

#ifdef CPUPROF
DEFINE_bool(cpu_profile, false, "");
#endif

DEFINE_bool(dump_stacktrace, true, "");
DEFINE_bool(localtest, false, "");

namespace dsm {

StringPiece::StringPiece() : data(NULL), len(0) {}
StringPiece::StringPiece(const string& s) : data(s.data()), len(s.size()) {}
StringPiece::StringPiece(const string& s, int len) : data(s.data()), len(len) {}
StringPiece::StringPiece(const char* c) : data(c), len(strlen(c)) {}
StringPiece::StringPiece(const char* c, int len) : data(c), len(len) {}
uint32_t StringPiece::hash() const { return Hash32(data, len); }
string StringPiece::AsString() const { return string(data, len); }

vector<StringPiece> StringPiece::split(StringPiece sp, StringPiece delim) {
  vector<StringPiece> out;
  const char* c = sp.data;
  while (c < sp.data + sp.len) {
    const char* next = c;

    bool found = false;

    while (next < sp.data + sp.len) {
      for (int i = 0; i < delim.len; ++i) {
        if (*next == delim.data[i]) {
          found = true;
        }
      }
      if (found)
        break;

      ++next;
    }

    if (found || c < sp.data + sp.len) {
      StringPiece part(c, next - c);
      out.push_back(part);
    }

    c = next + 1;
  }

  return out;
}

REGISTER_TEST(StringPiece, {
  vector<StringPiece> sp = StringPiece::split("a,b,c,d", ",");
  CHECK_EQ(sp[0].AsString(), "a");
  CHECK_EQ(sp[1].AsString(), "b");
  CHECK_EQ(sp[2].AsString(), "c");
  CHECK_EQ(sp[3].AsString(), "d");
});

string StringPrintf(StringPiece fmt, ...) {
  va_list l;
  va_start(l, fmt.AsString().c_str());
  string result = VStringPrintf(fmt, l);
  va_end(l);

  return result;
}

string VStringPrintf(StringPiece fmt, va_list l) {
  char buffer[32768];
  vsnprintf(buffer, 32768, fmt.AsString().c_str(), l);
  return string(buffer);
}

int Histogram::bucketForVal(double v) {
  if (v < kMinVal) { return 0; }

  v /= kMinVal;
  v += kLogBase;

  return 1 + static_cast<int>(log(v) / log(kLogBase));
}

double Histogram::valForBucket(int b) {
  if (b == 0) { return 0; }
  return exp(log(kLogBase) * (b - 1)) * kMinVal;
}

void Histogram::add(double val) {
  int b = bucketForVal(val);
//  LOG_EVERY_N(INFO, 1000) << "Adding... " << val << " : " << b;
  if (buckets.size() <= b) { buckets.resize(b + 1); }
  ++buckets[b];
  ++count;
}

void DumpProfile() {
#ifdef CPUPROF
ProfilerFlush();
#endif

#ifdef HEAPPROF
string heap_prof;
MallocExtension::instance()->GetHeapSample(&heap_prof);
File::Dump(StringPrintf("heap.profile.%d", getpid()), heap_prof);
#endif
}

string Histogram::summary() {
  string out;
  int total = 0;
  for (int i = 0; i < buckets.size(); ++i) { total += buckets[i]; }
  string hashes = string(100, '#');

  for (int i = 0; i < buckets.size(); ++i) {
    if (buckets[i] == 0) { continue; }
    out += StringPrintf("%-20.3g %6d %.*s\n", valForBucket(i), buckets[i], buckets[i] * 80 / total, hashes.c_str());
  }
  return out;
}

void Timer::Reset() {
  start_time_ = Now();
  start_cycle_ = rdtsc();
}

double Timer::elapsed() const {
  return Now() - start_time_;
}

uint64_t Timer::cycles_elapsed() const {
  return rdtsc() - start_cycle_;
}

uint64_t get_memory_total() {
  uint64_t m = -1;
  FILE* procinfo = fopen(StringPrintf("/proc/meminfo", getpid()).c_str(), "r");
  while (fscanf(procinfo, "MemTotal: %ld kB", &m) != 1) {
    if (fgetc(procinfo) == EOF) { break; }
  }
  fclose(procinfo);

  return m * 1024;
}

uint64_t get_memory_rss() {
  uint64_t m = -1;
  FILE* procinfo = fopen(StringPrintf("/proc/%d/status", getpid()).c_str(), "r");
  while (fscanf(procinfo, "VmRSS: %ld kB", &m) != 1) {
    if (fgetc(procinfo) == EOF) { break; }
  }
  fclose(procinfo);

  return m * 1024;
}

double get_processor_frequency() {
  double freq;
  int a, b;
  FILE* procinfo = fopen("/proc/cpuinfo", "r");
  while (fscanf(procinfo, "cpu MHz : %d.%d", &a, &b) != 2) {
    fgetc(procinfo);
  }

  freq = a * 1e6 + b * 1e-4;
  fclose(procinfo);
  return freq;
}

void Sleep(double t) {
  timespec req;
  req.tv_sec = (int)t;
  req.tv_nsec = (int64_t)(1e9 * (t - (int64_t)t));
  nanosleep(&req, NULL);
}

void SpinLock::lock() volatile {
  while (!__sync_bool_compare_and_swap(&d, 0, 1));
}

void SpinLock::unlock() volatile {
  d = 0;
}

static void CrashOnMPIError(MPI_Comm * c, int * errorCode, ...) {
  static dsm::SpinLock l;
  l.lock();

  char buffer[1024];
  int size = 1024;
  MPI_Error_string(*errorCode, buffer, &size);
  LOG(ERROR) << "MPI function failed: " << buffer;
  raise(SIGINT);
}

static void FatalSignalHandler(int sig) {
  static SpinLock lock;
  static void* stack[128];

  lock.lock();


  if (!FLAGS_dump_stacktrace) {
    _exit(1);
  }

  int count = backtrace(stack, 128);
  backtrace_symbols_fd(stack, count, STDERR_FILENO);

  static char cmdbuffer[1024];
  snprintf(cmdbuffer, 1024,
           "gdb "
           "-p %d "
           "-ex 'set print pretty' "
           "-ex 'set pagination 0' "
           "-ex 'thread apply all bt ' "
           "-batch ", getpid());

  fprintf(stderr, "Calling gdb with: %s", cmdbuffer);

  system(cmdbuffer);
  _exit(1);
}

void Init(int argc, char** argv) {
  FLAGS_logtostderr = true;
  FLAGS_logbuflevel = -1;

  CHECK_EQ(lzo_init(), 0);

  google::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);
  google::InstallFailureSignalHandler();

  RunInitializers();
  RunTests();

  MPI::Init_thread(argc, argv, MPI_THREAD_MULTIPLE);

  MPI::Comm *world = &MPI::COMM_WORLD;
  MPI_Errhandler handler;
  MPI_Errhandler_create(&CrashOnMPIError, &handler);
  world->Set_errhandler(handler);

  atexit(&MPI::Finalize);

  struct sigaction sig_action;
  bzero(&sig_action, sizeof(sig_action));
  sigfillset(&sig_action.sa_mask);
  sig_action.sa_flags |= SA_ONSTACK;
  sig_action.sa_handler = &FatalSignalHandler;

  sigaction(SIGSEGV, &sig_action, NULL);
  sigaction(SIGILL, &sig_action, NULL);
  sigaction(SIGFPE, &sig_action, NULL);
  sigaction(SIGABRT, &sig_action, NULL);
  sigaction(SIGBUS, &sig_action, NULL);
  sigaction(SIGTERM, &sig_action, NULL);

  srandom(time(NULL));
#ifdef CPUPROF
  if (FLAGS_cpu_profile) {
    mkdir("profile/", 0755);
    char buf[100];
    gethostname(buf, 100);
    ProfilerStart(StringPrintf("profile/worker.%d", world->Get_rank()).c_str());
  }
#endif
}
}
