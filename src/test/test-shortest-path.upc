#include <upc.h>
#include "test/file-helper.h"

#define BS 1000
#define NUM_NODES 1000000
#define IDX(id) ((id / BS) * THREADS + MYTHREAD)

// Entries are sharded according to the following scheme, assuming
// 4 nodes:
//
// 1: [OWNER][COPY][COPY][COPY]
// 2: [COPY][OWNER][COPY][COPY]
// 3: [COPY][COPY][OWNER][COPY]
// ...
// Each thread has a copy of a block; the canonical copy for block K
// is located at (BS * K * THREADS) {start of the block} +
// (K % THREADS) * BS {offset in the block}.

shared [BS] int distance[THREADS * NUM_NODES];

int main(int argc, char** argv) {
  char srcfile[1000];
  int current_entry, num_entries;
  int i, j, k, idx;
  struct RFile *r;
  GraphEntry *e, *entries;

  int *local_copy;

  // Load graph from disk...
  current_entry = 0;

  local_copy = malloc(sizeof(int) * BS * THREADS);

  entries = (GraphEntry*)malloc(NUM_NODES * sizeof(GraphEntry*));
  sprintf(srcfile, "testdata/graph-%05d-of-%05d", MYTHREAD, THREADS);

  r = RecordFile_Open(srcfile, "r");
  while ((e = RecordFile_ReadGraphEntry(r))) {
    entries[current_entry++] = *e;
  }

  RecordFile_Close(r);

  for (i = 0; i < 100; ++i) {
    // Propagate any updates into our local section of the address space
    for (j = 0; j < num_entries; ++i) {
      e = &entries[j];
      for (k = 0; k < e->num_neighbors; ++k) {
        distance[IDX(e->neighbors[k])] = MIN(distance[IDX(e->neighbors[k] * THREADS + MYTHREAD)],
                                             distance[IDX(e->id)] + 1);
      }
    }

    upc_barrier;

    // For each block of entries, fetch and compare with the remote blocks corresponding to our local block
    for (j = BS * MYTHREAD; j < NUM_NODES; j += BS * THREADS) {
      upc_memget(local_copy, &distance[j], BS * THREADS * sizeof(int));
      for (k = 0; k < BS * THREADS * sizeof(int); ++k) {
        idx = j + k % BS;
        distance[idx] = MIN(local_copy[j], distance[idx]);
      }
    }

    upc_barrier;
  }
}
