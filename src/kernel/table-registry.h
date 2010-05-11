#ifndef KERNEL_H_
#define KERNEL_H_

#include "util/common.h"
#include "kernel/table.h"

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

  template<class K, class V>
  TypedGlobalTable<K, V>* create_table(
                                       int id,
                                       int shards,
                                       void* sharding,
                                       void *accum) {
    TableDescriptor info;
    info.num_shards = shards;
    info.accum = (void*) accum;
    info.sharder = (void*) sharding;
    info.table_id = id;

    TypedGlobalTable<K, V> *t = new TypedGlobalTable<K, V>(info);
    tmap_.insert(make_pair(id, t));
    return t;
  }

private:
  Map tmap_;
};

} // end namespace
#endif /* KERNEL_H_ */
