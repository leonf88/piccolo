#ifndef KERNEL_H_
#define KERNEL_H_

#include "util/common.h"
#include "kernel/table.h"
#include "kernel/global-table.h"
#include "kernel/local-table.h"
#include "kernel/disk-table.h"
#include "kernel/sparse-table.h"
#include "kernel/dense-table.h"

namespace dsm {

class GlobalTable;

class TableRegistry : private boost::noncopyable {
private:
  TableRegistry() {}
public:
  typedef map<int, GlobalTable*> Map;

  static TableRegistry* Get();

  Map& tables();
  GlobalTable* table(int id);

private:
  Map tmap_;
};

template <class T>
static RecordTable<T>* CreateRecordTable(int id, StringPiece file_pattern, bool split_large_files=true) {
  RecordTable<T>* t = new RecordTable<T>(file_pattern, split_large_files);
  TypedTableDescriptor<string, T> *info = new TypedTableDescriptor<string, T>;
  info->num_shards = -1;
  info->table_id = id;

  t->Init(info);
  TableRegistry::Get()->tables().insert(make_pair(id, t));
  return t;
}

static TextTable* CreateTextTable(int id, StringPiece file_pattern, bool split_large_files=true) {
  TextTable* t = new TextTable(file_pattern, split_large_files);
  TypedTableDescriptor<string, string> *info = new TypedTableDescriptor<string, string>(id, -1);

  t->Init(info);
  TableRegistry::Get()->tables().insert(make_pair(id, t));
  return t;
}

// Swig doesn't like templatized default arguments; work around that here.
template<class K, class V>
static TypedGlobalTable<K, V>* CreateTable(int id,
                                           int shards,
                                           Sharder<K>* sharding,
                                           Accumulator<V>* accum) {
  TableDescriptor *info = new TypedTableDescriptor<K, V>(id, shards);
  info->sharder = sharding;
  info->partition_factory = new typename SparseTable<K, V>::Factory;

  return CreateTable<K, V>(info);
}

template<class K, class V>
static TypedGlobalTable<K, V>* CreateTable(TableDescriptor *info) {
  TypedGlobalTable<K, V> *t = new TypedGlobalTable<K, V>();
  t->Init(info);
  TableRegistry::Get()->tables().insert(make_pair(info->table_id, t));
  return t;
}

} // end namespace
#endif /* KERNEL_H_ */
