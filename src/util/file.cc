#include "util/file.h"
#include "util/common.h"
#include "google/protobuf/message.h"

namespace asyncgraph {

string Slurp(const string& f) {
  FILE* fp = fopen(f.c_str(), "r");
  if (!fp) { LOG(FATAL) << "Failed to read input file " << f.c_str(); }

  string out;
  char buffer[32768];

  while (!feof(fp) && !ferror(fp)) {
    int read = fread(buffer, 1, 32768, fp);
    if (read > 0) {
      out.append(buffer, read);
    } else {
      break;
    }
  }

  return out;
}

void Dump(const string& f, StringPiece data) {
  FILE* fp = fopen(f.c_str(), "w+");
  if (!fp) { LOG(FATAL) << "Failed to open output file " << f.c_str(); }
  fwrite(data.data, 1, data.len, fp);
  fflush(fp);
  fclose(fp);
}


File::Error::Error(string r) : reason(r) {
  strcpy(sys_error, strerror(errno));
  LOG(ERROR) << "File exception: " << reason << " :: " << sys_error;
}

void LocalFile::readLine(string *out) {
  out->resize(8192);
  fgets((char*)out->data(), out->size(), fp);
}

int LocalFile::read(char *buffer, int len) {
  return fread(buffer, 1, len, fp);
}

int LocalFile::write(const char *buffer, int len) {
  return fwrite(buffer, 1, len, fp);
}

void LocalFile::Printf(const char* p, ...) {
  va_list args;
  va_start(args, p);
  writeString(VStringPrintf(p, args));
  va_end(args);
}

bool LocalFile::eof() {
  return feof(fp);
}

LocalFile::LocalFile(const string &name, const string& mode) {
  fp = fopen(name.c_str(), mode.c_str());
  path = name;
  if (!fp) {
    throw new File::Error(StringPrintf("Failed to open file! %s with mode %s.", name.c_str(), mode.c_str()));
  }
}

RecordFile::RecordFile(const string& path, const string& mode) : fp(path, mode), firstWrite(true) {
  if (strstr(mode.c_str(), "r")) {
    FileParams pp;
    pp.ParseFromString(readChunk());

    for (int i = 0; i < pp.param_size(); ++i) {
      params[pp.param(i).key()] = pp.param(i).value();
    }
  }
}

void RecordFile::writeHeader() {
  FileParams pproto;
  for (ParamMap::iterator i = params.begin(); i != params.end(); ++i) {
    FileParam *p = pproto.add_param();
    p->set_key(i->first);
    p->set_value(i->second);
  }

  writeChunk(pproto.SerializeAsString());
}

void RecordFile::write(const google::protobuf::Message & m) {
  if (firstWrite) {
    writeHeader();
    firstWrite = false;
  }

  //LOG_EVERY_N(DEBUG, 1000) << "Writing... " << m.ByteSize() << " bytes at pos " << ftell(fp.filePointer());
  writeChunk(m.SerializeAsString());
  //LOG_EVERY_N(DEBUG, 1000) << "New pos: " <<  ftell(fp.filePointer());
}

void RecordFile::writeChunk(const string& data) {
  int len = data.size();
  fp.write((char*)&len, sizeof(len));
  fp.write(data.data(), data.size());
}

string RecordFile::readChunk() {
  int len;
  int bytes_read = fp.read((char*)&len, sizeof(len));

  if (bytes_read < sizeof(int)) { return ""; }

  string buf;
  buf.resize(len);
  fp.read(&buf[0], len);

  return buf;
}

bool RecordFile::read(google::protobuf::Message *m) {
  string s = readChunk();
  if (s.empty()) { return false; }
  m->ParseFromString(s);
  return true;
}
}
