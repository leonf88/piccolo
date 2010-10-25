#include "disk-table.h"
#include "util/file.h"

using google::protobuf::Message;
namespace dsm {

DiskTable::DiskTable(StringPiece file_pattern, uint64_t split_files_at) {
  vector<File::Info> files = File::MatchingFileinfo(file_pattern);
  if (split_files_at == 0) { split_files_at = ULONG_MAX; }

  for (int i = 0; i < files.size(); ++i) {
    File::Info fi = files[i];
    for (uint64_t j = 0; j < fi.stat.st_size; j += split_files_at) {
      Partition *p = new Partition();
      p->info = fi;
      p->start_pos = j;
      p->end_pos = min(j + split_files_at, (uint64_t)fi.stat.st_size);
      partitions_.push_back(p);
    }
  }
}

int64_t DiskTable::shard_size(int shard) {
  return partitions_[shard]->end_pos - partitions_[shard]->start_pos;
}

void DiskTable::Init(const TableDescriptor *tinfo) {
  ((TableDescriptor*)tinfo)->num_shards = partitions_.size();
  GlobalTable::Init(tinfo);
}

struct RecordIterator : public TypedTableIterator<uint64_t, Message*> {
  RecordIterator(const DiskTable::Partition& p, Message *msg) : p_(p), r_(p.info.name, "r") {
    r_.seek(p.start_pos);
    data_ = msg;
    Next();
  }

  const uint64_t& key() { return pos_; }
  Message*& value() { return data_; }

  void key_str(string *out) { kmarshal_.marshal(pos_, out); }
  void value_str(string *out) { vmarshal_.marshal(*data_, out); }

  bool done() {
//    LOG(INFO) << "RecordIterator: " << p_.info.name << " : " << (pos_ >= p_.end_pos) << ":: " << done_;
    return done_ || pos_ >= p_.end_pos;
  }

  void Next() {
    done_ = !r_.read(data_);
    pos_ = r_.fp->tell();
  }

  uint64_t pos_;
  bool done_;
  Message *data_;
  DiskTable::Partition p_;
  RecordFile r_;

  Marshal<uint64_t> kmarshal_;
  Marshal<Message> vmarshal_;
};

TypedTableIterator<uint64_t, Message*>* CreateRecordIterator(DiskTable::Partition p, Message *msg) {
  return new RecordIterator(p, msg);
}


struct TextIterator : public TypedTableIterator<uint64_t, string> {
  TextIterator(const DiskTable::Partition& p) : p_(p), f_(p.info.name, "r") {
    f_.seek(p.start_pos);
    done_ = false;
    Next();
  }

  const uint64_t& key() { return pos_; }
  string& value() { return line_; }
  void key_str(string *out) { return kmarshal_.marshal(pos_, out); }
  void value_str(string *out) { vmarshal_.marshal(line_, out); }

  bool done() { return done_ || f_.eof() || f_.tell() >= p_.end_pos; }

  void Next() {
    if (!f_.read_line(&line_)) { done_ = true; }
  }

  bool done_;
  uint64_t pos_;
  string line_;
  DiskTable::Partition p_;
  LocalFile f_;

  Marshal<uint64_t> kmarshal_;
  Marshal<string> vmarshal_;
};

TypedTableIterator<uint64_t, string> *TextTable::get_iterator(int shard) {
  return new TextIterator(*partitions_[shard]);
}

}


