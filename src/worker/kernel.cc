#include "worker/kernel.h"


namespace asyncgraph {

KernelRegistry* KernelRegistry::get() {
  static KernelRegistry* k = NULL;
  if (!k) { k = new KernelRegistry; }
  return k;
}

Kernel::Kernel() {
}

Kernel::~Kernel() {
}


void Kernel::output(const StringPiece& k, const StringPiece &v) {
  owner->output(k.hash() % numShards, k, v);
}

void Kernel::outputToShard(int shard, const StringPiece& k, const StringPiece &v) {
  owner->output(shard, k, v);
}

KernelRegistry::StaticHelper::StaticHelper(const char* name, asyncgraph::KernelRegistry::Factory *kf) {
  KernelRegistry::get()->registerKernel(name, kf);
}

void KernelRegistry::registerKernel(const string& name, Factory *f) {
  factories[name] = f;
}

Kernel* KernelRegistry::getKernel(const string& name) {
  if (factories.find(name) == factories.end()) {
    LOG(ERROR) << "Failed to find kernel " << name;
    for (map<string, Factory*>::iterator i = factories.begin(); i != factories.end(); ++i) {
      LOG(INFO) << "Kernel: " << name.c_str();
    }
    LOG(FATAL) << "Aborting.";
  }
  return factories[name]->newKernel();
}

}
