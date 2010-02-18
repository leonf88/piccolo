#include "client.h"

using namespace std;

class CrawlTable_Iterator {
public:
  CrawlTable_Iterator(dsm::TypedTable<string, int>::Iterator *it) : i(it) {}
  void Next() { i->Next(); }
  bool done() { return i->done(); }
  int value() { return i->value(); }
  string key() { return i->key(); }
private:
  dsm::TypedTable<string, int>::Iterator *i;  
};

class CrawlTable {
public:
  CrawlTable(dsm::TypedGlobalTable<string, int> *table) : t(table) {}
  int get(const string& k) { return t->get(k); }
  void put(const string& k, int v) { t->put(k, v); }
private:
  dsm::TypedGlobalTable<string, int> *t; 
};

CrawlTable& get_table();
int get_shard();
void Initialize(int argc, const char** argv);
