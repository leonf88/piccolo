#ifndef COMMON_H_
#define COMMON_H_

#include <time.h>

#include <stdarg.h>
#include <glog/logging.h>
#include <gflags/gflags.h>

#include <map>
#include <vector>
#include <deque>
#include <string>
#include <list>
#include <set>
#include <tr1/unordered_map>
#include <tr1/unordered_set>

#ifdef SWIG
#define __attribute__(X)
#endif

#include "util/hash.h"

namespace dsm {

using std::tr1::unordered_map;
using std::tr1::unordered_multimap;
using std::tr1::unordered_set;

using std::set;
using std::map;
using std::vector;
using std::deque;
using std::list;
using std::string;
using std::pair;
using std::make_pair;
using std::min;
using std::max;

extern void Init(int argc, char** argv);
extern uint64_t get_memory_rss();
extern uint64_t get_memory_total();

// Log-bucketed histogram.
class Histogram {
public:
  Histogram() : count(0) {}

  void add(double val);
  string summary();

  int bucketForVal(double v);
  double valForBucket(int b);

  int getCount() { return count; }
private:

  int count;
  vector<int> buckets;
  static const double kMinVal = 1e-9;
  static const double kLogBase = 1.1;
};

template <class T>
class Pool {
public:
  Pool(int capacity=100) : c_(capacity) {
    for (int i = 0; i < c_; ++i) {
      entries_.push_back(new T);
    }
  }

  ~Pool() {
    for (int i = 0; i < c_; ++i) {
      delete entries_[i];
    }
  }

  T* get() {
    T* t;
    if (!entries_.empty()) {
      t = entries_.back();
      entries_.pop_back();
    } else {
      t = new T;
    }

    return t;
  }

  void free(T* t) {
    entries_.push_back(t);
  }

private:
  int c_;
  vector<T*> entries_;
};

class StringPiece {
public:
  StringPiece();
  StringPiece(const string& s);
  StringPiece(const string& s, int len);
  StringPiece(const char* c);
  StringPiece(const char* c, int len);
  uint32_t hash() const;
  string AsString() const;

  const char* data;
  int len;
};

static bool operator==(const StringPiece& a, const StringPiece& b) {
  return a.data == b.data && a.len == b.len;
}

extern string StringPrintf(StringPiece fmt, ...);
extern string VStringPrintf(StringPiece fmt, va_list args);

extern void Sleep(double t);

extern void DumpHeapProfile(const string& file);

static uint64_t rdtsc() {
  uint32_t hi, lo;
  __asm__ __volatile__ ("rdtsc" : "=a"(lo), "=d"(hi));
  return (((uint64_t)hi)<<32) | ((uint64_t)lo);
}

inline double Now() {
  timespec tp;
  clock_gettime(CLOCK_MONOTONIC, &tp);
  return tp.tv_sec + 1e-9 * tp.tv_nsec;
}

class SpinLock {
public:
  SpinLock() : d(0) {}
  void lock() volatile;
  void unlock() volatile;
private:
  volatile int d;
};


class Timer {
public:
  Timer() {
    Reset();
  }

  void Reset();
  double elapsed() const;
  uint64_t cycles_elapsed() const;

private:
  double start_time_;
  uint64_t start_cycle_;
};

#define EVERY_N(interval, operation)\
{ static int COUNT = 0;\
  if (COUNT++ % interval == 0) {\
    operation;\
  }\
}

#define PERIODIC(interval, operation)\
{ static double last = 0;\
  static int COUNT = 0; \
  ++COUNT; \
  double now = dsm::Now();\
  if (now - last > interval) {\
    last = now;\
    operation;\
  }\
}

#define CALL_MEMBER_FN(object,ptrToMember) ((object)->*(ptrToMember))
#define IN(container, item) (std::find(container.begin(), container.end(), item) != container.end())

template<class A, class B>
inline pair<A, B> MP(A x, B y) { return pair<A, B>(x, y); }

}

namespace std {  namespace tr1 {
template <>
struct hash<dsm::StringPiece> : public unary_function<dsm::StringPiece, size_t> {
  size_t operator()(const dsm::StringPiece& k) const {
    return k.hash();
  }
};

template <class A, class B>
struct hash<pair<A, B> > : public unary_function<pair<A, B> , size_t> {
  hash<A> ha;
  hash<B> hb;

  size_t operator()(const pair<A, B> & k) const {
    return ha ^ hb;
  }
};

}}

// operator<< overload to allow protocol buffers to be output from the logging methods.
#include <google/protobuf/message.h>
namespace std{
static ostream & operator<< (ostream &out, const google::protobuf::Message &q) {
  string s = q.ShortDebugString();
  out << s;
  return out;
}

template <class A, class B>
static ostream & operator<< (ostream &out, const std::pair<A, B> &p) {
  out << "(" << p.first << "," << p.second << ")";
  return out;
}


}



#endif /* COMMON_H_ */
