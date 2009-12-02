#include "util/coder.h"

namespace upc {
#define WRITE_POD(T)\
void Encoder::write(const T& v) {\
/*    LOG(INFO) << "Writing " << v;\*/\
  out_->append((const char*)&v, (size_t)sizeof(v));\
}

WRITE_POD(int32_t);
WRITE_POD(float);
WRITE_POD(double);

void Encoder::write(const string& v) {
//    LOG(INFO) << "Writing string of length: " << v.size();
  write((int32_t)v.size());
  out_->append(v);
}

void Encoder::write_bytes(const char* a, int len) {
//    LOG(INFO) << "Writing string of length: " << v.size();
  out_->append(a, len);
}

#define READ_POD(T)\
void Decoder::read(T* v) {\
  *v = *(T*)pos;\
  /*LOG(INFO) << "Read: " << v << " : " << cur_.data + cur_.len - pos;*/\
  pos += sizeof(T);\
}

READ_POD(int32_t);
READ_POD(float);
READ_POD(double);

void Decoder::read(string *v) {
  int32_t len;
  read(&len);
//    LOG(INFO) << "Reading string of length: " << len << " : " << cur_.data + cur_.len - pos;
  v->assign(pos, len);
  pos += len;
}

}
