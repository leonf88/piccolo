#ifndef TABLEMSGS_H_
#define TABLEMSGS_H_

#include "util/rpc.h"

namespace upc {
// Hand defined serialization for hash request/update messages.  Painful, but
// necessary as the protocol buffer serialization is too slow to be usable.

#define SETGET(id, type)\
  const type& id () const;\
  void set_ ## id(const type& v);

#define SETGET_LIST(id, type)\
  SETGET(id, vector<type>)\
  const type& id(int idx) const;\
  int id ## _size() const;\
  void add_ ## id(const type &e);\
  vector< type >* mutable_ ## id();

struct HashRequest : public RPCMessage {
private:
  uint32_t table_id_;
  string key_;

public:
  HashRequest() { Clear(); }
  SETGET(table_id, uint32_t);
  SETGET(key, string);

  void Clear();
  int32_t ByteSize();

  void AppendToCoder(Encoder *e) const;
  void ParseFromCoder(Decoder *d);
};

struct HashUpdate : public RPCMessage {
private:
  uint32_t source_;
  uint32_t table_id_;

  // Typedef to allow the macros to work properly :p
  typedef pair<string, string> KVPair;

  vector<KVPair> put_;
  vector<string> remove_;
public:
  HashUpdate() { Clear(); }

  SETGET(source, uint32_t);
  SETGET(table_id, uint32_t);
  SETGET_LIST(put, KVPair);
  SETGET_LIST(remove, string);

  void Clear();
  int32_t ByteSize();

  void AppendToCoder(Encoder *e) const;
  void ParseFromCoder(Decoder *d);
};

#undef SETGET_LIST
#undef SETGET

extern void test_messages();
}

#endif /* TABLEMSGS_H_ */
