#include <stdio.h>

#include "kernel/table-registry.h"
#include "kernel/table-internal.h"

static const int kStatsTableId = 1000000;

namespace dsm {

TableRegistry* TableRegistry::Get() {
  static TableRegistry* t = new TableRegistry;
  return t;
}

TableRegistry::Map& TableRegistry::tables() {
  return tmap_;
}

GlobalView* TableRegistry::table(int id) {
  CHECK(tmap_.find(id) != tmap_.end());
  return tmap_[id];
}


static void CreateStatsTable() {
  TableRegistry::Get()->create_table<string, string>(
      kStatsTableId, 1, new Sharding::String, new Accumulators<string>::Replace);
}
}

REGISTER_INITIALIZER(CreateStatsTable, dsm::CreateStatsTable());
