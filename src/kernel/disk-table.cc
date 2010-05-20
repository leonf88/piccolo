#include "disk-table.h"

namespace dsm {

TableView::Iterator *DiskTable::get_iterator(int shard) {
  return NULL;
}

int64_t DiskTable::shard_size(int shard) {
  return 0;
}

void DiskTable::UpdateShardinfo(const ShardInfo & sinfo) {}

}
