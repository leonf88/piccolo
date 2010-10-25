#ifndef DISKTABLE_H_
#define DISKTABLE_H_

// DiskTables present a globally accessible, read-only interface to a set of
// files stored in a GFS.
//
// RecordTables wrap the RecordFile interface, and provide convenient access for
// streaming and random-access reads. TextTables read values from newline-delimited files;
// keys are simply the position of a line within it's shard.
//
// All DiskTables types can optionally sub-divide large files into smaller chunks.  These
// are presented to the user as separate input shards which can be mapped over.

#include "table.h"
#include "global-table.h"

namespace google { namespace protobuf { class Message; } }

using google::protobuf::Message;

namespace dsm {

struct PartitionInfo {
  File::Info info;
  uint64_t start_pos;
  uint64_t end_pos;
};

template <class K, class V>
class DiskTable : public TypedGlobalTable<K, V> {
public:
  DiskTable(StringPiece filepattern, uint64_t split_files_at);

  void Init(const TableDescriptor *tinfo);

  virtual TableIterator* get_iterator(int shard) = 0;
  int64_t shard_size(int shard);

  // These are not currently implemented or not necessary for disk based tables.
  int get_shard_str(StringPiece k) { return -1; }
  void start_checkpoint(const string& f) {}
  void write_delta(const TableData& d) {}
  void finish_checkpoint() {}
  void restore(const string& f) {}
protected:
  vector<PartitionInfo*> partitions_;
};

TypedTableIterator<uint64_t, Message>* CreateRecordIterator(PartitionInfo info, Message* msg);

template <class MessageClass>
class RecordTable : public DiskTable<uint64_t, MessageClass> {
public:
  typedef TypedTableIterator<uint64_t, MessageClass> Iterator;
  RecordTable(StringPiece filepattern, uint64_t split_files_at=0) : DiskTable<uint64_t, MessageClass>(filepattern, split_files_at) {}

  Iterator *get_iterator(int shard) {
    return (Iterator*)CreateRecordIterator(*this->partitions_[shard], new MessageClass);
  }
private:
};

class TextTable : public DiskTable<uint64_t, string> {
public:
  typedef TypedTableIterator<uint64_t, string> Iterator;

  TextTable(StringPiece filepattern, uint64_t split_files_at=0) : DiskTable<uint64_t, string>(filepattern, split_files_at) {}
  TypedTableIterator<uint64_t, string> *get_iterator(int shard);
};


template <class K, class V>
DiskTable<K, V>::DiskTable(StringPiece file_pattern, uint64_t split_files_at) {
  vector<File::Info> files = File::MatchingFileinfo(file_pattern);
  if (split_files_at == 0) { split_files_at = ULONG_MAX; }

  for (int i = 0; i < files.size(); ++i) {
    File::Info fi = files[i];
    for (uint64_t j = 0; j < fi.stat.st_size; j += split_files_at) {
      PartitionInfo *p = new PartitionInfo();
      p->info = fi;
      p->start_pos = j;
      p->end_pos = min(j + split_files_at, (uint64_t)fi.stat.st_size);
      partitions_.push_back(p);
    }
  }
}

template <class K, class V>
int64_t DiskTable<K, V>::shard_size(int shard) {
  return partitions_[shard]->end_pos - partitions_[shard]->start_pos;
}

template <class K, class V>
void DiskTable<K, V>::Init(const TableDescriptor *tinfo) {
  ((TableDescriptor*)tinfo)->num_shards = partitions_.size();
  GlobalTable::Init(tinfo);
}

}


#endif /* DISKTABLE_H_ */
