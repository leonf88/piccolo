#include <stdio.h>

#include "kernel/table-registry.h"
#include "kernel/table.h"

static const int kStatsTableId = 1000000;

namespace dsm {

namespace Registry {
static TableMap *tables = NULL;

TableMap& get_tables() {
  if (tables == NULL) {
    tables = new map<int, GlobalView*>;
  }
  return *tables;
}

GlobalView* get_table(int id) {
  CHECK(get_tables().find(id) != get_tables().end());
  return get_tables()[id];
}
}

}

static void CreateStatsTable() {
  dsm::Registry::create_table<string, string>(kStatsTableId, 1,
                                              &dsm::StringSharding, &dsm::Accumulator<string>::replace);
}

REGISTER_INITIALIZER(CreateStatsTable, CreateStatsTable());
