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


#ifndef SWIG
  template<class K, class V>
  TypedGlobalTable<K, V>* create_table(int id,
                                       int shards,
                                       Sharder<K>* sharding,
                                       Accumulator<V>* accum,
                                       Marshal<K> *key_marshal = new Marshal<K>,
                                       Marshal<V> *value_marshal = new Marshal<V>) {
    TableDescriptor info;
    info.num_shards = shards;
    info.accum = (void*) accum;
    info.sharder = (void*) sharding;
    info.table_id = id;
    info.key_marshal = key_marshal;
    info.value_marshal = value_marshal;

    TypedGlobalTable<K, V> *t = new TypedGlobalTable<K, V>(info);
    tmap_.insert(make_pair(id, t));
    return t;
  }
#endif

private:
  Map tmap_;
};

} // end namespace
#endif /* KERNEL_H_ */
