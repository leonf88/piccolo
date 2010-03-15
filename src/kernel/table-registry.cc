#include <stdio.h>

#include "kernel/table-registry.h"
#include "kernel/table.h"

namespace dsm {

namespace Registry {
static TableMap *tables = NULL;

TableMap& get_tables() {
  if (tables == NULL) {
    tables = new map<int, GlobalTable*>;
  }
  return *tables;
}

GlobalTable* get_table(int id) {
  return get_tables()[id];
}
}

}
