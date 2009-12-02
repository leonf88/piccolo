#ifndef CODER_H_
#define CODER_H_

#include "util/common.h"

namespace upc {
class Encoder {
public:
  Encoder(string *s) : out_(s) {}
  void write(const int32_t & v);
  void write(const float & v);
  void write(const double & v);
  void write(const string& v);
  void write_bytes(const char *a, int len);

  string *data() { return out_; }

private:
  string *out_;
};

struct Decoder {
  StringPiece data_;
  const char* pos;
  Decoder(StringPiece data) : data_(data), pos(data_.data) {}

  void read(int32_t *v);
  void read(float *v);
  void read(double *v);
  void read(string *v);
};

}

#endif /* CODER_H_ */
