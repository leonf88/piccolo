#ifndef KERNEL_H_
#define KERNEL_H_

#include "util/common.h"
#include "kernel/table.h"
#include "kernel/global-table.h"
#include "kernel/local-table.h"
#include "kernel/disk-table.h"

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
  TableDescriptor info;
  info.num_shards = -1;
  info.table_id = id;

  info.key_marshal = NULL;
  info.value_marshal = NULL;
  info.accum = NULL;
  info.sharder = NULL;

  t->Init(info);
  TableRegistry::Get()->tables().insert(make_pair(id, t));
  return t;
}

static TextTable* CreateTextTable(int id, StringPiece file_pattern, bool split_large_files=true) {
  TextTable* t = new TextTable(file_pattern, split_large_files);
  TableDescriptor info;
  info.num_shards = -1;
  info.table_id = id;

  info.key_marshal = NULL;
  info.value_marshal = NULL;
  info.accum = NULL;
  info.sharder = NULL;

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
return CreateTable(id, shards, sharding, accum, new Marshal<K>, new Marshal<V>);
}

template<class K, class V>
static TypedGlobalTable<K, V>* CreateTable(int id,
                                      int shards,
                                      Sharder<K>* sharding,
                                      Accumulator<V>* accum,
                                      Marshal<K> *key_marshal,
                                      Marshal<V> *value_marshal) {
  TableDescriptor info;
  info.num_shards = shards;
  info.accum = (void*) accum;
  info.sharder = (void*) sharding;
  info.table_id = id;
  info.key_marshal = key_marshal;
  info.value_marshal = value_marshal;

  TypedGlobalTable<K, V> *t = new TypedGlobalTable<K, V>();
  t->Init(info);
  TableRegistry::Get()->tables().insert(make_pair(id, t));
  return t;
}

} // end namespace
#endif /* KERNEL_H_ */
