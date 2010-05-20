#include "client.h"
#include "examples/examples.pb.h"

using namespace dsm;

namespace dsm {
template <>
struct Marshal<Bucket> {
  static void marshal(const Bucket& t, string *out) { t.SerializePartialToString(out); }
  static void unmarshal(const StringPiece& s, Bucket* t) { t->ParseFromArray(s.data, s.len); }
};
}
