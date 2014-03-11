#ifndef CLIENT_H_
#define CLIENT_H_

#include "piccolo/common.h"
#include "piccolo/file.h"

#include "piccolo/worker.h"
#include "piccolo/master.h"

#include "piccolo/kernel.h"
#include "piccolo/table-registry.h"

#ifndef SWIG
DECLARE_int32(shards);
DECLARE_int32(iterations);
#endif

// These are expanded by the preprocessor; these macro definitions
// are just for documentation.

// Run the given block of code on a single shard of 'table'.
#define PRunOne(table, code)

// Run the given block of code once for all shards of 'table'.
#define PRunAll(table, code)

// The (value : table) entries in bindings are evaluated once for
// each entry in table.  'code' is a code block that is
// executed with the bindings provided, once for each table entry.
#define PMap(bindings, code)

#endif /* CLIENT_H_ */
