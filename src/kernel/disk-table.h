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

namespace dsm {
class DiskTable : public GlobalView {
public:
  DiskTable(StringPiece filepattern);

  virtual TableView::Iterator *get_iterator(int shard) = 0;

  int64_t shard_size(int shard);
  void UpdateShardinfo(const ShardInfo & sinfo);
private:
  HashMap<int, int> owner_map_;
};

template<class T>
class RecordTable : public DiskTable, private boost::noncopyable {
public:
  TypedIterator<string, T*> *get_iterator(int shard);
private:

};

class TextTable : public DiskTable, private boost::noncopyable {
public:
  TypedIterator<int, string> *get_iterator(int shard);

};
}


#endif /* DISKTABLE_H_ */
