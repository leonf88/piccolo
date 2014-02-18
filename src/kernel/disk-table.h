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

class DiskTable : public GlobalTable {
public:
  struct Partition {
    File::Info info;
    uint64_t start_pos;
    uint64_t end_pos;
  };

  DiskTable(StringPiece filepattern, uint64_t split_files_at);

  void Init(const TableDescriptor *tinfo);

  int64_t shard_size(int shard);

  // These are not currently implemented for disk based tables.
  int get_shard_str(StringPiece k) { return -1; }
  void start_checkpoint(const string& f) {}
  void write_delta(const TableData& d) {}
  void finish_checkpoint() {}
  void restore(const string& f) {}

protected:
  vector<Partition*> partitions_;
};

TypedTableIterator<uint64_t, Message*>* CreateRecordIterator(DiskTable::Partition info, Message* msg);

template <class MessageClass>
class RecordTable : public DiskTable, private boost::noncopyable {
public:
  typedef TypedTableIterator<uint64_t, MessageClass*> Iterator;
  RecordTable(StringPiece filepattern, uint64_t split_files_at=0) : DiskTable(filepattern, split_files_at) {}

  Iterator *get_iterator(int shard) {
    return (Iterator*)CreateRecordIterator(*partitions_[shard], new MessageClass);
  }

  Iterator *get_typed_iterator(int shard) {
    return get_iterator(shard);
  }
private:
};

class TextTable : public DiskTable, private boost::noncopyable {
public:
  typedef TypedTableIterator<uint64_t, string> Iterator;
  TextTable(StringPiece filepattern, uint64_t split_files_at=0) : DiskTable(filepattern, split_files_at) {}
  TypedTableIterator<uint64_t, string> *get_iterator(int shard);
};
}


#endif /* DISKTABLE_H_ */
