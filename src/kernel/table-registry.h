#ifndef KERNEL_H_
#define KERNEL_H_

#include "util/common.h"
#include "kernel/table.h"
#include "kernel/global-table.h"
#include "kernel/local-table.h"

namespace dsm {

class GlobalTable;

class TableRegistry : private boost::noncopyable {
private:
  TableRegistry() {}
public:
  typedef map<int, GlobalView*> Map;

  static TableRegistry* Get();

  Map& tables();
  GlobalView* table(int id);

private:
  Map tmap_;
};

template<class K, class V>
TypedGlobalTable<K, V>* CreateTable(int id,
                                      int shards,
                                      Sharder<K>* sharding,
                                      Accumulator<V>* accum) {
return CreateTable(id, shards, sharding, accum, new Marshal<K>, new Marshal<V>);
}

template<class K, class V>
TypedGlobalTable<K, V>* CreateTable(int id,
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

  TypedGlobalTable<K, V> *t = new TypedGlobalTable<K, V>(info);
  TableRegistry::Get()->tables().insert(make_pair(id, t));
  return t;
}

} // end namespace
#endif /* KERNEL_H_ */
