#include <upc.h>
#include <sys/types.h>
#include <dirent.h>

#include "test/file-helper.h"

/* caveats
1. all web pages are numbered from 1..N
   there is no hash table implementation of pr on shared memory.
2. the entire pagerank array must be ble to fit in the memory of a single node
*/

#define N 64
#define BLK 2
#define ITERN 50
#define PROP 0.8

#define EIDX(p) ((p/(BLK*THREADS))*BLK + (p % BLK))

int TOTALRANK=0;
shared [N] double tmp_pr[THREADS][N/BLK][BLK];
shared [BLK] double pr[N/BLK][BLK];
GraphEntry entries[N/THREADS];

void load_graph() {

	char srcfile[1000];
  int current_entry = 0, k;
  struct RFile *r;
  GraphEntry *e;

	sprintf(srcfile, "testdata/pr-graph.rec-%05d-of-%05d-N%05d", MYTHREAD, THREADS, N);

  r = RecordFile_Open(srcfile, "r");
  while ((e = RecordFile_ReadGraphEntry(r))) {
    entries[current_entry++] = *e;
		assert(e->id >= 0 && e->id < N);
		assert(((e->id/BLK) % THREADS) == MYTHREAD);
  }
	assert(current_entry == N/THREADS);
  RecordFile_Close(r);
}

void
WriteStatus(int iter, int firstn) {
	int i, k;

	assert(firstn < N);
	if (MYTHREAD == 0) {
		printf("PR (%d)::", iter);
		for (i = 0; i < firstn; i++) {
			printf("%.2f ", pr[i/BLK][i%BLK]);
		}
		printf("\n");
	}
}

void
Initialize()
{
	//only master initializes, matching that in test-pr.cc
	int i;
	if (MYTHREAD == 0) {
		for (i = 0; i < N; i++) {
			pr[i/BLK][i%BLK] = (1-PROP)*(TOTALRANK/N);
		}
	}
}


int 
main(int argc, char **argv) {
	int i, j, k, iter;
	double *local_pr;
	double (*local_tmp_pr)[BLK];
	double buf[BLK];
  GraphEntry *e;

	assert(N % (BLK*THREADS) == 0); //current code does not work when this is otherwise
	TOTALRANK = N;

	load_graph();

	srand(0);

	Initialize();

	upc_barrier;
	WriteStatus(-1,10);

	if (MYTHREAD == 0) {
		printf("finish initialization ..pr[0]=%.2f N=%d\n", pr[0][0],N);
	}

	//hopefully, this is legal
	local_tmp_pr = (double (*)[BLK]) tmp_pr[MYTHREAD];

	for (iter = 0; iter <  ITERN; iter++) {
		bzero(local_tmp_pr, sizeof(double)*N);

		upc_forall(i = 0; i < N/BLK; i++; &pr[i][0]) {
			local_pr = (double (*))pr[i]; 
			e = &entries[EIDX(i*BLK)];
			for (j = 0; j < BLK; j++) {
				assert(e->id == i * BLK + j);
				for (k = 0; k < e->num_neighbors; k++) {
					local_tmp_pr[e->neighbors[k]/BLK][e->neighbors[k]%BLK] += PROP*(local_pr[j]/e->num_neighbors); //this should be all local
				}
				e++;
			}
		}

		upc_barrier;

		upc_forall(i = 0; i < N/BLK; i++; &pr[i][0]) {
			local_pr = (double (*))pr[i];
			bzero(local_pr, sizeof(double)*BLK);
			for (j = 0; j < THREADS; j++) {
				upc_memget(buf, &tmp_pr[j][i], BLK*sizeof(double));
				for (k = 0; k < BLK; k++) {
					local_pr[k] += buf[k];
				}
			}
			for (k = 0; k < BLK; k++) {
				local_pr[k] += (1-PROP)*(TOTALRANK/N);
			}
		}

		upc_barrier;
		WriteStatus(iter,10);

	}
}
