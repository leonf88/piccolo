#include "client.h"

using namespace std;

dsm::TypedGlobalTable<string, int>* get_table(int table_id);
int get_shard();
