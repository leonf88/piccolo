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

#include "util/hash.h"
#include "util/common.pb.h"
#include "util/static-initializers.h"

#ifdef SWIG

#define __attribute__(X)

#else
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

#endif


namespace dsm {

void Init(int argc, char** argv);

uint64_t get_memory_rss();
uint64_t get_memory_total();

void Sleep(double t);
void DumpProfile();

double get_processor_frequency();

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

  static vector<StringPiece> split(StringPiece sp, StringPiece delim);
};

static bool operator==(const StringPiece& a, const StringPiece& b) {
  return a.data == b.data && a.len == b.len;
}

static const char* strnstr(const char* haystack, const char* needle, int len) {
  int nlen = strlen(needle);
  for (int i = 0; i < len - nlen; ++i) {
    if (strncmp(haystack + i, needle, nlen) == 0) {
      return haystack + i;
    }
  }
  return NULL;
}

#ifndef SWIG
string StringPrintf(StringPiece fmt, ...);
string VStringPrintf(StringPiece fmt, va_list args);
#endif

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

  // Rate at which an event occurs.
  double rate(int count) {
    return count / (Now() - start_time_);
  }

  double cycle_rate(int count) {
    return double(cycles_elapsed()) / count;
  }

private:
  double start_time_;
  uint64_t start_cycle_;
};

namespace data {
  // I really, really, really hate C++.
  template <class T>
  static void marshal(const T& t, string* out) {
    GOOGLE_GLOG_COMPILE_ASSERT(std::tr1::is_pod<T>::value, Invalid_Value_Type);
    out->assign(reinterpret_cast<const char*>(&t), sizeof(t));
  }

  template <class T>
  static void unmarshal(const StringPiece& s, T *t) {
    GOOGLE_GLOG_COMPILE_ASSERT(std::tr1::is_pod<T>::value, Invalid_Value_Type);
    *t = *reinterpret_cast<const T*>(s.data);
  }

  // strings
  template <>
  void marshal(const string& t, string *out) {
    *out = t;
  }

  template <>
  void unmarshal(const StringPiece& s, string *t) {
    t->assign(s.data, s.len);
  }

  // protocol messages
  typedef google::protobuf::Message Message;
  template <>
  void marshal(const Message& t, string *out) {
    t.SerializePartialToString(out);
  }

  template <>
  void unmarshal(const StringPiece& s, Message* t) {
    t->ParseFromArray(s.data, s.len);
  }

  template <class T>
  static string to_string(const T& t) {
    string t_marshal;
    marshal(t, &t_marshal);
    return t_marshal;
  }

  template <class T>
  static T from_string(const StringPiece& t) {
    T t_marshal;
    unmarshal(t, &t_marshal);
    return t_marshal;
  }
};

#define EVERY_N(interval, operation)\
{ static int COUNT = 0;\
  if (COUNT++ % interval == 0) {\
    operation;\
  }\
}

#define PERIODIC(interval, operation)\
{ static int64_t last = 0;\
  static int64_t cycles = (int64_t)(interval * get_processor_frequency());\
  static int COUNT = 0; \
  ++COUNT; \
  int64_t now = rdtsc(); \
  if (now - last > cycles) {\
    last = now;\
    operation;\
    COUNT = 0;\
  }\
}

static double rand_double() {
  return double(random()) / RAND_MAX;
}

#define CALL_MEMBER_FN(object,ptrToMember) ((object)->*(ptrToMember))
#define IN(container, item) (std::find(container.begin(), container.end(), item) != container.end())

template <class A, class B, class C>
struct tuple3 {
  A a_; B b_; C c_;
  tuple3(const A& a, const B& b, const C& c) : a_(a), b_(b), c_(c) {}
};

template <class A, class B, class C, class D>
struct tuple4 {
  A a_; B b_; C c_; D d_;
  tuple4(const A& a, const B& b, const C& c, const D& d) : a_(a), b_(b), c_(c), d_(d) {}
};

template<class A, class B>
inline pair<A, B> MP(A x, B y) { return pair<A, B>(x, y); }

template<class A, class B, class C>
inline tuple3<A, B, C> MP(A x, B y, C z) { return tuple3<A, B, C>(x, y, z); }

template<class A, class B, class C, class D>
inline tuple4<A, B, C, D> MP(A x, B y, C z, D a) { return tuple4<A, B, C, D>(x, y, z, a); }
}

template<class A>
inline vector<A> MakeVector(const A&x) {
  vector<A> out;
  out.push_back(x);
  return out;
}

template<class A>
inline vector<A> MakeVector(const A&x, const A&y) {
  vector<A> out;
  out.push_back(x);
  out.push_back(y);
  return out;
}

template<class A>
inline vector<A> MakeVector(const A&x, const A&y, const A &z) {
  vector<A> out;
  out.push_back(x);
  out.push_back(y);
  out.push_back(z);
  return out;
}

#ifndef SWIG
static vector<int> range(int from, int to, int step=1) {
  vector<int> out;
  for (int i = from; i < to; ++i) {
    out.push_back(i);
  }
  return out;
}

static vector<int> range(int to) {
  return range(0, to);
}
#endif

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

#ifndef SWIG
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

template <class A, class B, class C>
static ostream & operator<< (ostream &out, const dsm::tuple3<A, B, C> &p) {
  out << "(" << p.a_ << "," << p.b_ << "," << p.c_ << ")";
  return out;
}

template <class A, class B, class C, class D>
static ostream & operator<< (ostream &out, const dsm::tuple4<A, B, C, D> &p) {
  out << "(" << p.a_ << "," << p.b_ << "," << p.c_ << "," << p.d_ << ")";
  return out;
}
}
#endif

#define COMPILE_ASSERT(x) extern int __dummy[(int)x]

#endif /* COMMON_H_ */
