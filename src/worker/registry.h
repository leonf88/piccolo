#ifndef KERNEL_H_
#define KERNEL_H_

#include "util/common.h"
#include "worker/table.h"
#include "worker/table-internal.h"

namespace upc {

typedef void (*KernelFunction)(void);

struct KernelRegistry {
  struct StaticHelper {
    StaticHelper(const char* name, KernelFunction kf);
  };

  static KernelFunction get_kernel(int id);
  static int get_id(KernelFunction kf);

  static map<int, KernelFunction>* get_mapping();
};

struct TableRegistry {
  struct TableCreator {
    virtual Table* create_table() = 0;
  };

  template <class K, class V>
  struct TemplateTableCreator {
    TemplateTableCreator(TableInfo info) : info_(info) {}
    ~TemplateTableCreator() {}

    Table* create_table() {
      return new TypedPartitionedTable<K, V>(info_);
    }

    TableInfo info_;
  };

  template <class K, class V>
  static void register_table(int id,int num_shards,
                             void *sharding_function, void *accum_function) {
    TableInfo info;
    info.af = accum_function;
    info.sf = sharding_function;
    info.num_shards = num_shards;
    info.table_id = id;

    TableCreator *t = new TemplateTableCreator<K, V>(info);

    register_table_creator(id, t);
  }

  static void register_table_creator(int id, TableCreator* t);
  static Table* get_instance(int id, int shard);
};

#define REGISTER_KERNEL(kf)\
  namespace {\
    struct StaticHelper_ ## kf : public KernelRegistry::StaticHelper {\
      StaticHelper_ ## kf () : StaticHelper(#kf, kf) {}\
    };\
    static StaticHelper_ ## kf reg_helper_ ## kf;\
  }


} // end namespace
#endif /* KERNEL_H_ */
