#include "client/client.h"

using namespace std;
using namespace dsm;

DEFINE_string(book_source, "/home/yavcular/books/520.txt", "");

static TextTable* books;
static TypedGlobalTable<string, int>* counts;

class WordcountKernel: public DSMKernel {
public:
  void InitKernel() {
    counts = this->get_table<string, int> (0);
  }

  void runWordcount() {
    int linec = 0, wordc = 0;
    TextTable::Iterator *i = books->get_iterator(current_shard());
    for (; !i->done(); i->Next(), ++linec) {
      vector<StringPiece> words = StringPiece::split(i->value(), " ");
      wordc += words.size();
      for (int j = 0; j < words.size(); ++j) {
        words[j].strip();
        counts->update(words[j].AsString(), 1);
      }
    }
    LOG(INFO) << "Done: " << linec << "; " << wordc;
  }

  void printResults() {
    TypedTableIterator<string, int> *it = counts->get_typed_iterator(current_shard());
    for (; !it->done(); it->Next()) {
      if (it->value() > 50) {
        printf("%20s : %d\n", it->key().c_str(), it->value());
      }
    }
  }
};
REGISTER_KERNEL(WordcountKernel);
REGISTER_METHOD(WordcountKernel, runWordcount);
REGISTER_METHOD(WordcountKernel, printResults);

static int WordCount(ConfigData& conf) {
  counts = CreateTable(0, 1, new Sharding::String, new Accumulators<int>::Sum);
  TextTable* books = CreateTextTable(1, FLAGS_book_source, false);

  if (!StartWorker(conf)) {
    Master m(conf);
    m.run_all("WordcountKernel", "runWordcount", books);
    m.run_all("WordcountKernel", "printResults", books);
  }
  return 0;
}
REGISTER_RUNNER(WordCount);
