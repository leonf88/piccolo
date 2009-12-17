#include "worker/table-msgs.h"

namespace upc {
// Hand defined serialization for hash request/update messages.  Painful, but
// necessary as the protocol buffer serialization is too slow to be usable.

#define SETGET(klass, id, type)\
  const type& klass:: id () const { return id ## _; }\
  void klass::set_ ## id(const type& v) { id ## _ = v; }

#define SETGET_LIST(klass, id, type)\
  SETGET(klass, id, vector<type>)\
  const type& klass:: id(int idx) const { return id ## _[idx]; }\
  int klass::id ## _size() const { return id ## _.size(); }\
  void klass::add_ ## id(const type &e) { id ## _.push_back(e); }\
  vector<type>*  klass::mutable_ ## id() { return &id ##_; }

SETGET(HashRequest, table_id, uint32_t);
SETGET(HashRequest, key, string);

void HashRequest::Clear() {
  table_id_ = 0;
  key_.clear();
}

int32_t HashRequest::ByteSize() { return key_.size() + sizeof(table_id_); }

void HashRequest::AppendToCoder(Encoder *e) const {
  e->write(table_id_);
  e->write(key_);
}

void HashRequest::ParseFromCoder(Decoder *d) {
  d->read(&table_id_);
  d->read(&key_);
}

HashUpdate::HashUpdate() :e_(&encoded_pairs_) {}

SETGET(HashUpdate, source, uint32_t);
SETGET(HashUpdate, table_id, uint32_t);

void HashUpdate::Clear() {
  encoded_pairs_.clear();
  offsets_.clear();
  source_ = table_id_ = 0;
}

int32_t HashUpdate::ByteSize() {
  return 8 + encoded_pairs_.size() + offsets_.size() * 8;
}

void HashUpdate::add_put(const string& k, const string& v) {
  offsets_.push_back(make_pair(encoded_pairs_.size(), encoded_pairs_.size() + k.size()));

  e_.write_bytes(k);
  e_.write_bytes(v);
}

void HashUpdate::AppendToCoder(Encoder *e) const {
  e->write(source_);
  e->write(table_id_);

  e->write((uint32_t)offsets_.size());
  for (int i = 0; i < offsets_.size(); ++i) {
    e->write(offsets_[i].first);
    e->write(offsets_[i].second);
  }

  e->write(encoded_pairs_);
}

void HashUpdate::ParseFromCoder(Decoder *d) {
  d->read(&source_);
  d->read(&table_id_);

  int num_offsets = d->read_uint32();
  offsets_.resize(num_offsets);
  for (int i = 0; i < num_offsets; ++i) {
    d->read(&offsets_[i].first);
    d->read(&offsets_[i].second);
  }

  d->read(&encoded_pairs_);
}

StringPiece HashUpdate::value(int idx) const {
  int start = offsets_[idx].second;
  int end = idx == offsets_.size() - 1 ? encoded_pairs_.size() : offsets_[idx + 1].first;
  return StringPiece(&encoded_pairs_[0] + start, end - start);
}

StringPiece HashUpdate::key(int idx) const {
  int start = offsets_[idx].first;
  int end = offsets_[idx].second;
  return StringPiece(&encoded_pairs_[0] + start, end - start);
}

}
