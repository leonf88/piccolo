#ifndef HASH_H_
#define HASH_H_

extern "C" {
  uint32_t hashlittle(const void *key, size_t length, uint32_t initval);
}

namespace asyncgraph {
  static inline uint32_t Hash32( const char *key, size_t length) {
    return hashlittle((void*)key, length, 0);
  }
}

#endif /* HASH_H_ */
