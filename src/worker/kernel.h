#ifndef GRAPH_H_
#define GRAPH_H_

#include "util/common.h"
#include "worker/worker.pb.h"
#include <boost/thread.hpp>

namespace asyncgraph {

template <class K, class V>
class CastingIterator {
public:
  CastingIterator(const UpdateRequest &req) : u(req), i(0) {}

  int source() { return u.source(); }
  void next() { ++i; }
  bool done() { return i == u.update_size(); }
  K key() { return *reinterpret_cast<const K*>(u.update(i).key().data()); }
  V value() { return *reinterpret_cast<const V*>(u.update(i).value().data()); }
private:
  const UpdateRequest& u;
  int i;
};

class Accumulator;

class KernelRunner {
public:
  virtual void output(int shard, const StringPiece &k, const StringPiece &v) = 0;
};

class Kernel  {
public:
  Kernel();
  virtual ~Kernel();
  
  void _init(KernelRunner* kr, const ConfigData& conf) {
    owner = kr;
    numShards = conf.num_shards();
    init(conf);
  }

  void outputToShard(int shard, const StringPiece& k, const StringPiece &v);
  void output(const StringPiece& k, const StringPiece &v);

  // Convenience method for POD.
  template <class K, class V>
  void output(const K& k, const V& v) {
    outputToShard(k % numShards,
                  StringPiece((char*)&k, sizeof(k)),
                  StringPiece((char*)&v, sizeof(v)));
  }

  virtual Accumulator* newAccumulator() = 0;

  virtual void init(const ConfigData& conf) {}
  virtual bool done() { return false; }

  virtual void runIteration() = 0;
  virtual void handleUpdates(const UpdateRequest& updates) = 0;

  virtual void flush() {}
  virtual void status(KernelStatus *status) {}

protected:
  int numShards;
  KernelRunner* owner;
};

struct KernelRegistry {
  struct Factory {
    virtual Kernel *newKernel() = 0;
  };

  struct StaticHelper {
    StaticHelper(const char* name, Factory *kf);
  };

  static KernelRegistry* get();
  void registerKernel(const string& name, Factory *f);
  Kernel* getKernel(const string& name);

  map<string, Factory*> factories;
};

#define REGISTER_KERNEL(Klassname)\
  struct Klassname ## StaticHelper : public KernelRegistry::StaticHelper {\
    Klassname ## StaticHelper () : StaticHelper(#Klassname, new MyFactory()) {}\
    struct MyFactory : public KernelRegistry::Factory {\
      Kernel* newKernel() { return new Klassname; }\
    };\
  };\
  static Klassname ## StaticHelper registryHelper ## Klassname;

} // end namespace
#endif /* GRAPH_H_ */
