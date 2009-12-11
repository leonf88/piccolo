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

void Encoder::write_bytes(const char* a, int len) {
  out_->append(a, len);
}

#define READ_POD(T)\
void Decoder::read(T* v) {\
  memcpy(v, pos, sizeof(T));\
  pos += sizeof(T);\
}

READ_POD(uint32_t);
READ_POD(float);
READ_POD(double);

void Decoder::read(string *v) {
  uint32_t len;
  read(&len);

  CHECK_LE(pos - &data_[0] + len, data_.size()) << "tried to read past buffer - len: " << len;
  v->assign(pos, len);
  pos += len;
}

}
