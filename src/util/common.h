#ifndef COMMON_H_
#define COMMON_H_

#include <stdarg.h>
#include <glog/logging.h>
#include <gflags/gflags.h>

#include <boost/shared_ptr.hpp>
#include <boost/shared_array.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/scoped_array.hpp>
#include <boost/thread.hpp>

#include <map>
#include <vector>
#include <deque>
#include <string>
#include <list>
#include <tr1/unordered_map>

#ifdef SWIG
#define __attribute__(X)
#endif

#include "util/hash.h"

namespace upc {

using std::tr1::unordered_map;
using std::tr1::unordered_multimap;

using std::map;
using std::vector;
using std::deque;
using std::list;
using std::string;
using std::pair;
using std::make_pair;
using std::min;
using std::max;

using boost::scoped_array;
using boost::scoped_ptr;
using boost::shared_array;
using boost::shared_ptr;

extern void Init(int argc, char** argv);

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

class StringPiece {
public:
  StringPiece();
  StringPiece(const string& s);
  StringPiece(const string& s, int len);
  StringPiece(const char* c, int len);
  uint32_t hash() const;
  string AsString() const;

  const char* data;
  int len;
};

static bool operator==(const StringPiece& a, const StringPiece& b) {
  return a.data == b.data && a.len == b.len;
}

extern string StringPrintf(const char* fmt, ...) __attribute__ ((format (printf, 1, 2)));
extern string VStringPrintf(const char* fmt, va_list args);

extern double Now();
extern void Sleep(double t);

extern void DumpHeapProfile(const string& file);

extern boost::thread_group programThreads;

class SpinLock {
public:
  SpinLock() : d(0) {}
  void lock() volatile;
  void unlock() volatile;
private:
  volatile int d;
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
  if (Now() - last > interval) {\
    last = Now();\
    operation;\
  }\
}

}

namespace std {  namespace tr1 {
template <>
struct hash<upc::StringPiece> : public unary_function<upc::StringPiece, size_t> {
  size_t operator()(const upc::StringPiece& k) const {
    return k.hash();
  }
};

}}

#include <google/protobuf/message.h>
namespace std{
static ostream & operator<< (ostream &out, const google::protobuf::Message &q) {
  string s = q.ShortDebugString();
  out << s;
  return out;
}

}



#endif /* COMMON_H_ */
