/*
 * wordcount.cpp
 *
 *  Created on: May 19, 2010
 *      Author: yavcular
 */

#include "client.h"

using namespace std;
using namespace dsm;

static	TypedGlobalTable <string, int>* wcount;
class WordcountKernel : public DSMKernel {

public:
	  void InitKernel() {
	    wcount = this->get_table<string, int>(0);
	  }

	  LocalFile* get_reader() {
	      string file = StringPrintf("/home/yavcular/books/520.txt");
	      //FILE* lzo = popen(StringPrintf("lzop -d -c %s", file.c_str()).c_str(), "r");
	      //RecordFile * r = new RecordFile(lzo, "r");
	      return new LocalFile(file, "r");
	  }

	  void free_reader(LocalFile* r) {
	    //pclose(r->fp.filePointer());
	    delete r;
	  }

	  void runWordcount() {
		printf("in runWordCOunt!\n");
		string w;
	    Timer t;

	    LocalFile *r = get_reader();

	    int linec = 0, wordc = 0;
	    while (!r->readLine(&w)) {
	    	linec++;
	    	char warr[w.length()];
	    	for(int i = 0;i < w.length();i++)
	    	    warr[i] = w[i];

	    	char * split;
	    	split = strtok(warr," 	");
	    	while(split != NULL)
	    	{
	    		wordc++;
				wcount->update(split, 1);
	    		split = strtok (NULL, " 	");
	    	}
	    }
	    printf("%d lines %d words\n", linec, wordc);

	    printf("------------HOPP2\n");
	    free_reader(r);
	    printf("------------HOPP3\n");
	    TypedIterator<string, int> *it = wcount->get_typed_iterator(current_shard());
	    printf("------------HOPP4\n");
		for (; !it->done(); it->Next()) {
		  if (it->value() > 50)
		  {
			  printf("%s", ((string)it->key()).c_str());
			  printf(":%d\n", (int)it->value());
		  }
		}
	    printf("------------HOPP5\n");

	    char host[1024];
	    gethostname(host, 1024);
	    VLOG(1) << "Finished shard " << current_shard() << " on " << host << " in " << t.elapsed();
	  }
};

//I need to resister the methods that are used in main
//more info about configuration settings maybe ?
//why GlobakView in pagerank ? what is the difference ?
//so main fuc runs on all the machines, how do I partition the table | how they read the data

REGISTER_KERNEL(WordcountKernel);
REGISTER_METHOD(WordcountKernel, InitKernel);
REGISTER_METHOD(WordcountKernel, runWordcount);

static int WordCount(ConfigData& conf){

	conf.set_slots(FLAGS_shards * 2 / conf.num_workers());
	wcount = TableRegistry::Get()->create_table<string, int>(0, 1, new Sharding::String, new Accumulators<int>::Sum);
	if (!StartWorker(conf))
	{
		Master m(conf);
		m.run_all("WordcountKernel", "InitKernel", wcount);
		m.run_all("WordcountKernel", "runWordcount", wcount);
	}
	return 0;
}
REGISTER_RUNNER(WordCount);
