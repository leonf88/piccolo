#ifndef KERNEL_H_
#define KERNEL_H_

#include "util/common.h"
#include "kernel/table.h"

namespace dsm {

class GlobalTable;

namespace Registry {
typedef map<int, GlobalTable*> TableMap;

TableMap& get_tables();
GlobalTable* get_table(int id);

template<class K, class V>
TypedGlobalTable<K, V>* create_table(
                                     int id,
                                     int shards,
                                     typename TypedTable<K, V>::ShardingFunction sharding,
                                     typename TypedTable<K, V>::AccumFunction accum) {
  TableDescriptor info;
  info.num_shards = shards;
  info.accum_function = (void*) accum;
  info.sharding_function = (void*) sharding;
  info.table_id = id;

  TypedGlobalTable<K, V> *t = TypedGlobalTable<K, V>::Create(info);
  get_tables().insert(make_pair(id, t));
  return t;
}
}

} // end namespace
#endif /* KERNEL_H_ */
