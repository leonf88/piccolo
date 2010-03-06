#ifndef CLIENT_H_
#define CLIENT_H_

#include "util/common.h"
#include "util/file.h"

#include "worker/worker.h"
#include "master/master.h"
#include "kernel/table.h"
#include "kernel/table-internal.h"
#include "kernel/kernel-registry.h"
#include "kernel/table-registry.h"

DECLARE_int32(shards);
DECLARE_int32(iterations);

#endif /* CLIENT_H_ */
