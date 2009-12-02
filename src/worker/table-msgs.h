#ifndef TABLEMSGS_H_
#define TABLEMSGS_H_

#include "util/rpc.h"

namespace upc {
// Hand defined serialization for hash request/update messages.  Painful, but
// necessary as the protocol buffer serialization is too slow to be usable.

#define SETGET(id)\
  const typeof(id ## _)& id () const;\
  void set_ ## id(const typeof(id ## _)& v);

#define SETGET_LIST(id)\
  SETGET(id)\
  const typeof(id ## _[0]) id(int idx) const;\
  int id ## _size() const;\
  void add_ ## id(const typeof(id ##_[0]) &e);\
  typeof(id ## _)*  mutable_ ## id();

struct HashRequest : public RPCMessage {
private:
  int32_t table_id_;
  string key_;

public:
  HashRequest() { Clear(); }
  SETGET(table_id);
  SETGET(key);

  void Clear();
  int32_t ByteSize();

  void AppendToCoder(Encoder *e) const;
  void ParseFromCoder(Decoder *d);
};

struct HashUpdate : public RPCMessage {
private:
  int32_t source_;
  int32_t table_id_;

  vector<pair<string, string> > put_;
  vector<string> remove_;
public:
  HashUpdate() { Clear(); }

  SETGET(source);
  SETGET(table_id);
  SETGET_LIST(put);
  SETGET_LIST(remove);

  void Clear();
  int32_t ByteSize();

  void AppendToCoder(Encoder *e) const;
  void ParseFromCoder(Decoder *d);
};

#undef SETGET_LIST
#undef SETGET

}

#endif /* TABLEMSGS_H_ */
