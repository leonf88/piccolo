#ifndef FILE_H_
#define FILE_H_

#include "util/common.h"
#include "util/common.pb.h"

#include <stdio.h>

namespace google { namespace protobuf { class Message; } }

namespace dsm {

class File {
public:
  virtual int read(char *buffer, int len) = 0;
  virtual bool readLine(string *out) = 0;
  virtual bool eof() = 0;
  virtual void seek(int64_t pos) = 0;

  int writeString(const string& buffer) { return write(buffer.data(), buffer.size()); }

  virtual int write(const char* buffer, int len) = 0;

  string readLine() {
    string out;
    readLine(&out);
    return out;
  }

  static string Slurp(const string& file);
  static void Dump(const string& file, StringPiece data);
  static void Mkdirs(const string& path);
private:
};

class LocalFile : public File {
public:
  LocalFile(const string& path, const string& mode);
  ~LocalFile() { fflush(fp); fclose(fp); }


  bool readLine(string *out);
  int read(char *buffer, int len);
  int write(const char* buffer, int len);
  void seek(int64_t pos) { fseek(fp, pos, SEEK_SET); }

  void Printf(const char* p, ...);
  virtual FILE* filePointer() { return fp; }

  const char* name() { return path.c_str(); }

  bool eof();

private:
  FILE* fp;
  string path;
};

class Encoder {
public:
  Encoder(string *s) : out_(s), out_f_(NULL) {}
  Encoder(File *f) : out_(NULL), out_f_(f) {}

  template <class T>
  void write(const T& v);

  void write_string(const string& v);
  void write_bytes(StringPiece s);
  void write_bytes(const char *a, int len);

  string *data() { return out_; }

  size_t pos() { return out_->size(); }

private:
  string *out_;
  File *out_f_;
};

class Decoder {
private:
  const string* src_;
  const char* src_p_;
  File* f_src_;

  int pos_;
public:
  Decoder(const string& data) : src_(&data), src_p_(data.data()), f_src_(NULL), pos_(0) {}
  Decoder(File* f) : src_(NULL), src_p_(NULL), f_src_(f), pos_(0) {}

  template <class V> void read(V* t) {
    if (src_) {
      memcpy(t, src_p_ + pos_, sizeof(V));
      pos_ += sizeof(V);
    } else {
      f_src_->read((char*)t, sizeof(t));
    }
  }

  template <class V> V read() {
    V v;
    read<V>(&v);
    return v;
  }

  void read_bytes(char* b, int len) {
    if (src_p_) { memcpy(b, src_p_ + pos_, len); }
    else { f_src_->read(b, len); }

    pos_ += len;
  }

  void read_string(string* v) {
    uint32_t len;
    read<uint32_t>(&len);
    v->resize(len);

    read_bytes(&(*v)[0], len);
  }

  bool done() {
    if (src_) { return pos_ == src_->size(); }
    else { return f_src_->eof(); }
  }

  size_t pos() { return pos_; }

  void seek(int p) {
    if (src_) { pos_ = p; }
    else { pos_ = p; f_src_->seek(pos_); }
  }
};


class RecordFile {
public:
  RecordFile(const string& path, const string& mode,
             int compression=NONE);
  ~RecordFile() {
    fflush(fp.filePointer());
  }

  // Arbitrary key-value pairs to be attached to this file; these are written
  // prior to any message data.
  typedef unordered_map<string, string> AttrMap;
  AttrMap attributes;

  void write(const google::protobuf::Message &m);
  bool read(google::protobuf::Message *m);
  const char* name() { return fp.name(); }

  bool eof() { return fp.eof(); }

private:
  void writeChunk(const string &s);
  string readChunk();

  void writeHeader();
  string temp;
  LocalFile fp;
  bool firstWrite;
  FileParams params_;
};
}

#endif /* FILE_H_ */
