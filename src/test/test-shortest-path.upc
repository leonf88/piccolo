#include <upc.h>
#include <stdio.h>
#include <stdlib.h>
#include "test/file-helper.h"

#define BS 1000
#define NUM_NODES 100000

static int IDX(int id) {
  int row = id / (BS * THREADS);
//  fprintf(stderr, "Returning row: %d, col %d for id %d; %d\n",
//          row, MYTHREAD, id, (row * BS * THREADS) + MYTHREAD * BS + id % BS);
  return (row * BS * THREADS) + MYTHREAD * BS + id % BS;
}

static int PRIMARY_IDX(int id) {
  int row = id / (BS * THREADS);
  int block = (id / BS) * BS;
  int primary = block % THREADS;
//  fprintf(stderr, "Returning row: %d, col %d for id %d; %d\n",
//          row, primary, id, (row * BS * THREADS) + primary * BS + id % BS);
  return (row * BS * THREADS) + primary * BS + id % BS;
}

#define MIN(a, b) ( (a < b) ? a : b )

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

  entries = (GraphEntry*)malloc(NUM_NODES * sizeof(GraphEntry));
  sprintf(srcfile, "testdata/graph.rec-%05d-of-%05d", MYTHREAD, THREADS);

  fprintf(stderr, "Reading from file %s\n", srcfile);
  r = RecordFile_Open(srcfile, "r");
  fprintf(stderr, "Pointer %p\n", r);
  while ((e = RecordFile_ReadGraphEntry(r))) {
    entries[current_entry++] = *e;
  }
  RecordFile_Close(r);

  num_entries = current_entry;
  fprintf(stderr, "Done reading: %d entries\n", num_entries);

  for (i = 0; i < num_entries; ++i) {
    distance[IDX(entries[i].id)] = 1000000;
  }

  distance[0] = 0;

  upc_barrier;

  for (i = 0; i < 100; ++i) {
    fprintf(stderr, "Iteration: %d\n", i);
    // Propagate any updates into our local section of the address space
    for (j = 0; j < num_entries; ++j) {
      e = &entries[j];
      for (k = 0; k < e->num_neighbors; ++k) {
        if (e->id == 0 || e->neighbors[k] == 0) {
//          fprintf(stderr, "Propagating... %d (%d) -> %d (%d)\n",
//                  e->id, distance[IDX(e->id)],
//                  e->neighbors[k], distance[IDX(e->neighbors[k])]);
        }

        distance[IDX(e->neighbors[k])] = MIN(distance[IDX(e->neighbors[k])], distance[IDX(e->id)] + 1);
      }
    }

    upc_barrier;

    // For each block of entries, fetch and compare with the remote blocks corresponding to our local block
    for (j = BS * MYTHREAD; j < NUM_NODES; j += BS * THREADS) {
      upc_memget(local_copy, &distance[j], BS * THREADS * sizeof(int));
      for (k = 0; k < BS * THREADS; ++k) {
        idx = j + (k % BS);
        if (idx == 0) {
//          fprintf(stderr, "Checking: %d %d %d (%d) -> %d (%d)\n", j, k, idx, distance[IDX(idx)], k, local_copy[k]);
        }
        distance[IDX(idx)] = MIN(local_copy[k], distance[IDX(idx)]);
      }
    }

    upc_barrier;
//    fprintf(stderr, "Done iteration: %d\n", i);
  }

  if (MYTHREAD == 0) {
    for (i = 0; i < num_entries; ++i) {
      if (i % 40 == 0) { printf("\n%d: ", i); }
      printf("%d ", distance[PRIMARY_IDX(i)]);
    }
  }
  printf("\n");
}
