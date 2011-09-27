#include "table.h"
#include "local-table.h"

namespace dsm {

// Encodes or decodes table entries, reading and writing from the
// specified file.
struct LocalTableCoder : public TableCoder {
  LocalTableCoder(const string &f, const string& mode);
  virtual ~LocalTableCoder();

  virtual void WriteEntry(StringPiece k, StringPiece v);
  virtual bool ReadEntry(string* k, string *v);

  RecordFile *f_;
};

void LocalTable::start_checkpoint(const string& f) {
  VLOG(1) << "Start checkpoint " << f;
  Timer t;

  LocalTableCoder c(f, "w");
  Serialize(&c);

  delta_file_ = new LocalTableCoder(f + ".delta", "w");
  VLOG(1) << "Flushed to disk in: " << t.elapsed();
}

void LocalTable::finish_checkpoint() {
//  VLOG(1) << "FStart.";
  if (delta_file_) {
    delete delta_file_;
    delta_file_ = NULL;
  }
//  VLOG(1) << "FEnd.";
}

void LocalTable::restore(const string& f) {
  if (!File::Exists(f)) {
    VLOG(1) << "Skipping restore of non-existent shard " << f;
    return;
  }

  TableData p;

  LocalTableCoder rf(f, "r");
  string k, v;
  while (rf.ReadEntry(&k, &v)) {
   update_str(k, v);
  }

  // Replay delta log.
  LocalTableCoder df(f + ".delta", "r");
  while (df.ReadEntry(&k, &v)) {
    update_str(k, v);
  } 
}

//Dummy stub
//void LocalTable::DecodeUpdates(TableCoder *in, DecodeIteratorBase *itbase) { return; }

void LocalTable::write_delta(const TableData& put) {
  for (int i = 0; i < put.kv_data_size(); ++i) {
    delta_file_->WriteEntry(put.kv_data(i).key(), put.kv_data(i).value());
  }
}

LocalTableCoder::LocalTableCoder(const string& f, const string &mode) :
    f_(new RecordFile(f, mode, RecordFile::LZO)) {
}

LocalTableCoder::~LocalTableCoder() {
  delete f_;
}

bool LocalTableCoder::ReadEntry(string *k, string *v) {
  if (f_->readChunk(k)) {
    f_->readChunk(v);
    return true;
  }

  return false;
}

void LocalTableCoder::WriteEntry(StringPiece k, StringPiece v) {
  f_->writeChunk(k);
  f_->writeChunk(v);
}

}
