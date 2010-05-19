#ifndef COMMON_H_
#define COMMON_H_

#include <time.h>

#include <cstdarg>
#include <map>
#include <vector>
#include <deque>
#include <string>
#include <list>
#include <set>
#include <tr1/unordered_map>
#include <tr1/unordered_set>

#include "util/common.pb.h"
#include "util/hash.h"
#include "util/logging.h"
#include "util/static-initializers.h"
#include "util/timer.h"

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

namespace dsm {

void Init(int argc, char** argv);

uint64_t get_memory_rss();
uint64_t get_memory_total();

void Sleep(double t);
void DumpProfile();

double get_processor_frequency();

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

class SpinLock {
public:
  SpinLock() : d(0) {}
  void lock() volatile;
  void unlock() volatile;
private:
  volatile int d;
};

static double rand_double() {
  return double(random()) / RAND_MAX;
}

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
#include <google/protobuf/message.h>

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

static string ToString(const google::protobuf::Message &q) { return q.ShortDebugString(); }

template <class A, class B>
static string ToString(const std::pair<A, B> &p) {
  return StringPrintf("(%s, %s)", ToString(p.first).c_str(), ToString(p.second).c_str());
}

template <class A, class B, class C>
static string ToString(const dsm::tuple3<A, B, C> &p) {
  return StringPrintf("(%s, %s, %s)",
                      ToString(p.a_).c_str(),
                      ToString(p.b_).c_str(),
                      ToString(p.c_).c_str());
}

template <class A, class B, class C, class D>
static string ToString(const dsm::tuple4<A, B, C, D> &p) {
  return StringPrintf("(%s, %s, %s, %s)",
                      ToString(p.a_).c_str(),
                      ToString(p.b_).c_str(),
                      ToString(p.c_).c_str(),
                      ToString(p.d_).c_str());
}
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
    return ha(k.a) ^ hb(k.b);
  }
};
}}

#define COMPILE_ASSERT(x) extern int __dummy[(int)x]

#endif /* COMMON_H_ */
