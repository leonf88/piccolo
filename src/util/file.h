#ifndef FILE_H_
#define FILE_H_

#include "util/common.h"
#include "util/common.pb.h"

#include <stdio.h>

namespace google { namespace protobuf { class Message; } }

namespace dsm {

extern string Slurp(const string& file);
extern void Dump(const string& file, StringPiece data);
extern void Mkdirs(const string& path);

class File {
public:
  virtual int read(char *buffer, int len) = 0;
  virtual void readLine(string *out) = 0;

  int writeString(const string& buffer) {
    return write(buffer.data(), buffer.size());
  }

  virtual int write(const char* buffer, int len) = 0;

    struct Error {
    Error(string r="Generic error");
    string reason;
    char sys_error[1000];
  };

  string readLine() {
    string out;
    readLine(&out);
    return out;
  }

private:
};

class LocalFile : public File {
public:
  LocalFile(const string& path, const string& mode);
  ~LocalFile() { fflush(fp); fclose(fp); }


  virtual void readLine(string *out);
  virtual int read(char *buffer, int len);
  virtual int write(const char* buffer, int len);

  void Printf(const char* p, ...);
  virtual FILE* filePointer() { return fp; }

  const char* name() { return path.c_str(); }

  bool eof();

private:
  FILE* fp;
  string path;
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
