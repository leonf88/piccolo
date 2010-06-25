#ifndef COMMON_H_
#define COMMON_H_

#include <time.h>
#include <vector>
#include <string>

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "glog/logging.h"
#include "google/gflags.h"

#include "util/common.pb.h"
#include "util/hash.h"
#include "util/static-initializers.h"
#include "util/stringpiece.h"
#include "util/timer.h"
#include "util/hashmap.h"

#include <tr1/unordered_map>
#include <tr1/unordered_set>

using std::map;
using std::vector;
using std::string;
using std::pair;
using std::make_pair;
using std::tr1::unordered_map;
using std::tr1::unordered_set;

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
  static const double kMinVal;
  static const double kLogBase;
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

template <class A, class B>
struct tuple2 {
  A a_; B b_;
  bool operator==(const tuple2& o) { return o.a_ == a_ && o.b_ == b_; }
};

template <class A, class B, class C>
struct tuple3 {
  A a_; B b_; C c_;
  bool operator==(const tuple3& o) { return o.a_ == a_ && o.b_ == b_ && o.c_ == c_; }
};

template <class A, class B, class C, class D>
struct tuple4 {
  A a_; B b_; C c_; D d_;
  bool operator==(const tuple4& o) { return o.a_ == a_ && o.b_ == b_ && o.c_ == c_ && o.d_ == d_; }
};

template<class A, class B>
inline tuple2<A, B> MP(A x, B y) {
  tuple2<A, B> t = { x, y };
  return t;
}

template<class A, class B, class C>
inline tuple3<A, B, C> MP(A x, B y, C z) {
  tuple3<A, B, C> t = { x, y, z };
  return t;
}

template<class A, class B, class C, class D>
inline tuple4<A, B, C, D> MP(A x, B y, C z, D a) {
  tuple4<A, B, C, D> t = {x, y, z, a};
  return t;
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
}

#ifndef SWIG
#include <google/protobuf/message.h>


namespace std { namespace tr1 {
template <>
struct hash<dsm::StringPiece> {
  size_t operator()(const dsm::StringPiece& k) { return k.hash(); }
};

template <class A, class B>
struct hash<pair<A, B> > : public unary_function<pair<A, B> , size_t> {
  hash<A> ha;
  hash<B> hb;

  size_t operator()(const pair<A, B> & k) const {
    return ha(k.first) ^ hb(k.second);
  }
};

template <class A, class B>
struct hash<dsm::tuple2<A, B> > : public unary_function<dsm::tuple2<A, B> , size_t> {
  hash<A> ha;
  hash<B> hb;

  size_t operator()(const dsm::tuple2<A, B> & k) const {
    return ha(k.a_) ^ hb(k.b_);
  }
};

template <class A, class B, class C>
struct hash<dsm::tuple3<A, B, C> > : public unary_function<dsm::tuple3<A, B, C> , size_t> {
  hash<A> ha;
  hash<B> hb;
  hash<C> hc;

  size_t operator()(const dsm::tuple3<A, B, C> & k) const {
    return ha(k.a_) ^ hb(k.b_) ^ hc(k.c_);
  }
};

} }

namespace dsm {
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
}

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

template <class A, class B>
static ostream & operator<< (ostream &out, const dsm::tuple2<A, B> &p) {
  out << "(" << p.a_ << "," << p.b_ << ")";
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
