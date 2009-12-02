#include "worker/table-msgs.h"

namespace upc {
// Hand defined serialization for hash request/update messages.  Painful, but
// necessary as the protocol buffer serialization is too slow to be usable.

#define CTYPE(klass, id) typeof(klass::id ## _)

#define SETGET(klass, id)\
  const CTYPE(klass, id)& klass:: id () const { return id ## _; }\
  void klass::set_ ## id(const CTYPE(klass, id)& v) { id ## _ = v; }

#define SETGET_LIST(klass, id)\
  SETGET(klass, id)\
  const typeof(klass::id ## _[0]) klass:: id(int idx) const { return id ## _[idx]; }\
  int klass::id ## _size() const { return id ## _.size(); }\
  void klass::add_ ## id(const typeof(klass::id ##_[0]) &e) { id ## _.push_back(e); }\
  CTYPE(klass, id)*  klass::mutable_ ## id() { return &id ##_; }

SETGET(HashRequest, table_id);
SETGET(HashRequest, key);

void HashRequest::Clear() {
  table_id_ = 0;
  key_.clear();
}

int32_t HashRequest::ByteSize() { return key_.size() + sizeof(table_id_); }

void HashRequest::AppendToCoder(Encoder *e) const {
  e->write(table_id_);
  e->write(key_);

//  LOG(INFO) << table_id_ << " : " << key_.size() << e->data()->size();

  Decoder d(*e->data());
  string key2;
  int tid;
  d.read(&tid);
  d.read(&key2);
//  LOG(INFO) << tid << " : " << key2.size();
}

void HashRequest::ParseFromCoder(Decoder *d) {
  d->read(&table_id_);
  d->read(&key_);

//  LOG(INFO) << table_id_ << " : " << key_;
}

SETGET(HashUpdate, source);
SETGET(HashUpdate, table_id);
SETGET_LIST(HashUpdate, put);
SETGET_LIST(HashUpdate, remove);

void HashUpdate::Clear() {
  remove_.clear();
  put_.clear();
  source_ = table_id_ = 0;
}

int32_t HashUpdate::ByteSize() {
  int32_t b;
  for (int i = 0; i < put_.size(); ++i) { b += put_[i].first.size() + put_[i].second.size(); }
  for (int i = 0; i < remove_.size(); ++i) { b += remove_[i].size(); }
  b += sizeof(table_id_);
  b += sizeof(source_);
  return b;
}

void HashUpdate::AppendToCoder(Encoder *e) const {
  e->write(source_);
  e->write(table_id_);
  e->write((int32_t)put_.size());
  for (int i = 0; i < put_.size(); ++i) {
    e->write(put_[i].first);
    e->write(put_[i].second);
  }

  e->write((int32_t)remove_.size());
  for (int i = 0; i < remove_.size(); ++i) {
    e->write(remove_[i]);
  }
}

void HashUpdate::ParseFromCoder(Decoder *d) {
  d->read(&source_);
  d->read(&table_id_);
  int put_size, remove_size;
  d->read(&put_size);
  put_.resize(put_size);
  for (int i = 0; i < put_.size(); ++i) {
    d->read(&put_[i].first);
    d->read(&put_[i].second);
  }

  d->read(&remove_size);
  remove_.resize(remove_size);
  for (int i = 0; i < remove_.size(); ++i) {
    d->read(&remove_[i]);
  }
}

static void test_messages() {
  HashUpdate h;
  pair<string, string> put = make_pair("hahahahah", "hahahahahaha");
  h.add_put(put);

  string s;
  h.AppendToString(&s);
  h.ParseFromString(s);

  CHECK(h.put(0) == put);

  HashRequest req, req2;
  req.set_key("abc");
  req.set_table_id(0);

  s.clear();
  req.AppendToString(&s);
  req2.ParseFromString(s);

  CHECK_EQ(req2.table_id(), 0);
  CHECK_EQ(req2.key(), "abc");
}

}
