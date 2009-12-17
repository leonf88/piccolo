#include "util/coder.h"

namespace upc {
#define WRITE_POD(T)\
void Encoder::write(const T& v) {\
  out_->append((const char*)&v, (size_t)sizeof(v));\
}

WRITE_POD(uint32_t);
WRITE_POD(float);
WRITE_POD(double);

void Encoder::write(const string& v) {
  write((uint32_t)v.size());
  out_->append(v);
}

void Encoder::write_bytes(StringPiece s) {
  out_->append(s.data, s.len);
}

void Encoder::write_bytes(const char* a, int len) {
  out_->append(a, len);
}

#define READ_POD(T, NAME)\
void Decoder::read(T* v) {\
  memcpy(v, pos_, sizeof(T));\
  pos_ += sizeof(T);\
}\
T Decoder::read_ ## NAME () {\
  T t;\
  read(&t);\
  return t;\
}


READ_POD(uint32_t, uint32);
READ_POD(float, float);
READ_POD(double, double);

void Decoder::read(string *v) {
  uint32_t len;
  read(&len);

  CHECK_LE(pos_ - &data_[0] + len, data_.size()) << "tried to read past buffer - len: " << len;
  v->assign(pos_, len);
  pos_ += len;
}

}
