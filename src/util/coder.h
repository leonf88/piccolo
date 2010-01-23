#ifndef CODER_H_
#define CODER_H_

#include "util/common.h"

namespace dsm {
class Encoder {
public:
  Encoder(string *s) : out_(s) {}
  void write(const uint32_t & v);
  void write(const float & v);
  void write(const double & v);
  void write(const string& v);

  void write_bytes(StringPiece s);
  void write_bytes(const char *a, int len);

  string *data() { return out_; }

private:
  string *out_;
};

class Decoder {
private:
  const string& data_;
  const char* pos_;
public:
  Decoder(const string& data) : data_(data), pos_(&data_[0]) {}

  void read(uint32_t *v);
  void read(float *v);
  void read(double *v);
  void read(string *v);

  uint32_t read_uint32();
  float read_float();
  double read_double();

  bool done() { return pos_ == &data_[0] + data_.size(); }

  const string& data() { return data_; }
  size_t pos() { return pos_ - &data_[0]; }

  void seek(int p) { pos_ = &data_[0] + p; }
};

}

#endif /* CODER_H_ */
