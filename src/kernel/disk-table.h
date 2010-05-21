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

namespace dsm {

class DiskTable : public GlobalTable {
public:
  class Partition;

  DiskTable(StringPiece filepattern, uint64_t split_files_at);

  int64_t shard_size(int shard);
  void UpdateShardinfo(const ShardInfo & sinfo);
protected:
  vector<Partition*> partitions_;
};

TypedIterator<uint64_t, google::protobuf::Message*>*
  CreateRecordIterator(DiskTable::Partition info, google::protobuf::Message* msg);

template <class MessageClass>
class RecordTable : public DiskTable, private boost::noncopyable {
public:
  RecordTable(StringPiece filepattern, uint64_t split_files_at=0) : DiskTable(filepattern, split_files_at) {}
  TypedIterator<uint64_t, google::protobuf::Message*> *get_iterator(int shard) {
    return CreateRecordIterator(partitions_[shard], new MessageClass);
  }
private:
};

class TextTable : public DiskTable, private boost::noncopyable {
public:
  TextTable(StringPiece filepattern, uint64_t split_files_at=0) : DiskTable(filepattern, split_files_at) {}
  TypedIterator<uint64_t, string> *get_iterator(int shard);
};
}


#endif /* DISKTABLE_H_ */
