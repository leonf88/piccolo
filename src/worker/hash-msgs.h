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
  uint32_t shard_;
  string key_;

public:
  HashRequest() { Clear(); }
  SETGET(table_id, uint32_t);
  SETGET(shard, uint32_t);
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
  uint32_t shard_;

  string encoded_pairs_;

  Encoder e_;

  vector<pair<uint32_t, uint32_t> > offsets_;
public:
  HashUpdate();

  SETGET(source, uint32_t);
  SETGET(table_id, uint32_t);
  SETGET(shard, uint32_t);

  void add_put(const string& k, const string& v);
  int put_size() const {
    return offsets_.size();
  }

  StringPiece key(int idx) const;
  StringPiece value(int idx) const;

  void AppendToCoder(Encoder *e) const;
  void ParseFromCoder(Decoder *d);

  void Clear();
  int32_t ByteSize();
};

#undef SETGET_LIST
#undef SETGET

extern void test_messages();
}

#endif /* TABLEMSGS_H_ */
