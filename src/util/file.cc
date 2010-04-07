#include "util/file.h"
#include "util/common.h"
#include "google/protobuf/message.h"
#include <stdio.h>
#include <glob.h>

namespace dsm {

vector<string> File::Glob(const string& pattern) {
  glob_t globbuf;
  globbuf.gl_offs = 0;
  glob(pattern.c_str(), 0, NULL, &globbuf);
  vector<string> out;
  for (int i = 0; i < globbuf.gl_pathc; ++i) {
    out.push_back(globbuf.gl_pathv[i]);
  }
  globfree(&globbuf);
  return out;
}

void File::Mkdirs(const string& path) {
  system(StringPrintf("mkdir -p '%s'", path.c_str()).c_str());
}

string File::Slurp(const string& f) {
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

bool File::Exists(const string& f) {
  FILE* fp = fopen(f.c_str(), "r");
  if (fp) {
    fclose(fp);
    return true;
  }
  return false;
}
void File::Dump(const string& f, StringPiece data) {
  FILE* fp = fopen(f.c_str(), "w+");
  if (!fp) { LOG(FATAL) << "Failed to open output file " << f.c_str(); }
  fwrite(data.data, 1, data.len, fp);
  fflush(fp);
  fclose(fp);
}

bool LocalFile::readLine(string *out) {
  out->resize(8192);
  char* res = fgets(&(*out)[0], out->size(), fp);
  out->resize(strlen(out->data()));
  return res == NULL;
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

LocalFile::LocalFile(FILE* stream) {
  CHECK(stream != NULL);
  fp = stream;
  path = "<EXTERNAL FILE>";
  close_on_delete = false;
}

LocalFile::LocalFile(const string &name, const string& mode) {
  fp = fopen(name.c_str(), mode.c_str());
  path = name;
  close_on_delete = true;
  if (!fp) {
    LOG(FATAL) << StringPrintf("Failed to open file! %s with mode %s.", name.c_str(), mode.c_str());
  }
}

template <class T>
void Encoder::write(const T& v) {
  if (out_) {
    out_->append((const char*)&v, (size_t)sizeof(v));
  } else {
    out_f_->write((const char*)&v, (size_t)sizeof(v));
  }
}

#define INSTANTIATE(T) template void Encoder::write<T>(const T& t)
INSTANTIATE(double);
INSTANTIATE(uint64_t);
INSTANTIATE(uint32_t);
INSTANTIATE(float);
#undef INSTANTIATE

void Encoder::write_string(const string& v) {
  write((uint32_t)v.size());
  write_bytes(v);
}

void Encoder::write_bytes(StringPiece s) {
  if (out_) { out_->append(s.data, s.len); }
  else { out_f_->write(s.data, s.len); }
}

void Encoder::write_bytes(const char* a, int len) {
  if (out_) { out_->append(a, len); }
  else { out_f_->write(a, len); }
}

int LZOFile::write(const char* data, int len) {
  if (block.len + len > kBlockSize) {
    write_block();
  }

  memcpy(block.raw + block.len, data, len);
  block.len += len;

  return len;
}

int LZOFile::read(char* data, int len) {
  if (block.pos == block.len) {
    read_block();
  }
  int read = min(len, block.len - block.pos);

  memcpy(data, block.raw + block.pos, read);
  block.pos += read;
  pos_ += read;
  return read;
}

void LZOFile::write_block() {
  if (block.len == 0)
    return;

//  LOG(INFO) << "Writing... " << block.len << " : " << f_->tell();

  lzo_uint comp_size = kCompressedBlockSize;
  CHECK_EQ(0, lzo1x_1_15_compress((unsigned char*)block.raw, block.len,
                                  (unsigned char*)block.comp, (lzo_uint*)&comp_size,
                                  (unsigned char*)block.scratch));

  f_->write((char*)&comp_size, sizeof(int));
  f_->write(block.comp, comp_size);
  block.len = 0;
}

void LZOFile::read_block() {
  block.len = kBlockSize;
  int comp_size;
  if (f_->read((char*)&comp_size, sizeof(int)) != sizeof(int)) {
    block.len = 0;
    block.pos = 0;
    return;
  }

  CHECK_EQ(f_->read(block.comp, comp_size), comp_size);
  CHECK_GT(comp_size, 0);
  CHECK_EQ(0, lzo1x_decompress_safe((unsigned char*)block.comp, comp_size,
                                    (unsigned char*)block.raw, (lzo_uint*)&block.len,
                                    (unsigned char*)block.scratch));

//  LOG(INFO) << "Read block of size: " << MP(block.len, comp_size);
  block.pos = 0;
}

RecordFile::RecordFile(FILE *stream, const string& mode) : firstWrite(true) {
  fp = new LocalFile(stream);
  Init(mode);
}

RecordFile::RecordFile(const string& path, const string& mode, int compression) : firstWrite(true) {
  if (compression == LZO) {
    fp = new LZOFile(new LocalFile(path + ".lzo", mode), mode);
  } else {
    fp = new LocalFile(path, mode);
  }

  Init(mode);
}

void RecordFile::Init(const string& mode) {
  if (strstr(mode.c_str(), "r")) {
    params_.ParseFromString(readChunk());

    for (int i = 0; i < params_.attr_size(); ++i) {
      attributes[params_.attr(i).key()] = params_.attr(i).value();
    }
  }
}

void RecordFile::writeHeader() {
  for (AttrMap::iterator i = attributes.begin(); i != attributes.end(); ++i) {
    FileAttribute *p = params_.add_attr();
    p->set_key(i->first);
    p->set_value(i->second);
  }

  writeChunk(params_.SerializeAsString());
}

void RecordFile::write(const google::protobuf::Message & m) {
  if (firstWrite) {
    writeHeader();
    firstWrite = false;
  }

  //LOG_EVERY_N(DEBUG, 1000) << "Writing... " << m.ByteSize() << " bytes at pos " << ftell(fp->filePointer());
  writeChunk(m.SerializeAsString());
  //LOG_EVERY_N(DEBUG, 1000) << "New pos: " <<  ftell(fp->filePointer());
}

void RecordFile::writeChunk(const string& data) {
  int len = data.size();
  fp->write((char*)&len, sizeof(int));
  fp->write(data.data(), data.size());
}

const string& RecordFile::readChunk() {
  buf_.clear();

  int len;
  int bytes_read = fp->read((char*)&len, sizeof(len));

  if (bytes_read < sizeof(int)) { return buf_; }

  buf_.resize(len);
  fp->read(&buf_[0], len);
  return buf_;
}

bool RecordFile::read(google::protobuf::Message *m) {
  buf_ = readChunk();
  if (buf_.empty()) { 
    return false; 
  }
  CHECK(m->ParseFromString(buf_));
  return true;
}
}
