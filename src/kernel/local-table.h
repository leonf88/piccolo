#ifndef LOCALTABLE_H_
#define LOCALTABLE_H_

#include "table.h"
#include "util/file.h"
#include "util/rpc.h"

namespace dsm {


// Represents a single shard of a global table.
class LocalTable : public TableBase, public Checkpointable {
public:
  LocalTable() : delta_file_(NULL) {}

  virtual TableBase::Iterator *get_iterator() = 0;

  virtual void resize(int64_t new_size) = 0;
  virtual void clear() = 0;

  virtual int64_t size() = 0;
  bool empty() { return size() == 0; }

  virtual bool contains_str(const StringPiece& k) = 0;
  virtual string get_str(const StringPiece &k) = 0;
  virtual void update_str(const StringPiece &k, const StringPiece &v) = 0;

  virtual void Serialize(TableData *req) = 0;
  virtual void ApplyUpdates(const TableData& req) = 0;

  void start_checkpoint(const string& f) {
    VLOG(1) << "Start.";
    Timer t;
    RecordFile rf(f, "w", RecordFile::LZO);

    TableData data;

    data.set_source(0);
    data.set_table(id());
    data.set_shard(shard());

    Serialize(&data);
    rf.write(data);

    delta_file_ = new RecordFile(f + ".delta", "w", RecordFile::LZO);
    VLOG(1) << "End.";
    //  LOG(INFO) << "Flushed " << file << " to disk in: " << t.elapsed();
  }

  void finish_checkpoint() {
    VLOG(1) << "FStart.";
    if (delta_file_) {
      delete delta_file_;
      delta_file_ = NULL;
    }
    VLOG(1) << "FEnd.";
  }

  void restore(const string& f) {
    if (!File::Exists(f)) {
      LOG(INFO) << "Skipping restore of non-existent shard " << id() << " : " << shard();
      return;
    }

    TableData p;

    RecordFile rf(f, "r", RecordFile::LZO);
    while (rf.read(&p)) { ApplyUpdates(p); }

    // Replay delta log.
    RecordFile df(f + ".delta", "r", RecordFile::LZO);
    while (df.read(&p)) { ApplyUpdates(p); }
  }

  void write_delta(const TableData& put) {
    delta_file_->write(put);
  }

protected:
  friend class GlobalTable;
  RecordFile *delta_file_;
};

}

#endif /* LOCALTABLE_H_ */
