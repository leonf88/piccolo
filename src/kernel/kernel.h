#ifndef KERNELREGISTRY_H_
#define KERNELREGISTRY_H_

#include "kernel/table.h"
#include "util/common.h"
#include <boost/function.hpp>

namespace dsm {

template <class K, class V>
class TypedGlobalTable;

class Table;
class Worker;

class DSMKernel {
public:
  // Called upon creation of this kernel.
  virtual void InitKernel() {}

  // Called before worker begins writing checkpoint data for the table
  // this kernel is working on.  Values stored in 'params' will be made
  // available in the corresponding Restore() call.
  virtual void Checkpoint(Params* params) {}

  // Called after worker has restored table state from a previous checkpoint
  // with this kernel active.
  virtual void Restore(const Params& params) {}

  // The table and shard being processed.
  int current_shard() const { return shard_; }
  int current_table() const { return table_id_; }

  GlobalTable* get_table(int id);

  template <class K, class V>
  TypedGlobalTable<K, V>* get_table(int id) {
    return (TypedGlobalTable<K, V>*)get_table(id);
  }
private:
  friend class Worker;
  friend class Master;

  void initialize_internal(Worker* w, int table_id, int shard);

  Worker *w_;
  int shard_;
  int table_id_;
};

struct KernelInfo {
  KernelInfo(const char* name) : name_(name) {}

  virtual DSMKernel* create() = 0;
  virtual void Run(DSMKernel* obj, const string& method_name) = 0;
  virtual bool has_method(const string& method_name) = 0;

  string name_;
};

template <class C>
struct KernelInfoT : public KernelInfo {
  typedef void (C::*Method)();
  map<string, Method> methods_;

  KernelInfoT(const char* name) : KernelInfo(name) {}

  DSMKernel* create() { return new C; }

  void Run(DSMKernel* obj, const string& method_id) {
    boost::function<void (C*)> m(methods_[method_id]);
    m((C*)obj);
  }

  bool has_method(const string& name) {
    return methods_.find(name) != methods_.end();
  }

  void register_method(const char* mname, Method m) { methods_[mname] = m; }
};

class ConfigData;
class KernelRegistry {
public:
  typedef map<string, KernelInfo*> Map;
  Map& kernels() { return m_; }
  KernelInfo* kernel(const string& name) { return m_[name]; }

  static KernelRegistry* Get();
private:
  KernelRegistry() {}
  Map m_;
};

template <class C>
struct KernelRegistrationHelper {
  KernelRegistrationHelper(const char* name) {
    KernelRegistry::Get()->kernels().insert(make_pair(name, new KernelInfoT<C>(name)));
  }
};

template <class C>
struct MethodRegistrationHelper {
  MethodRegistrationHelper(const char* klass, const char* mname, void (C::*m)()) {
    ((KernelInfoT<C>*)KernelRegistry::Get()->kernel(klass))->register_method(mname, m);
  }
};

class RunnerRegistry {
public:
  typedef int (*KernelRunner)(ConfigData&);
  typedef map<string, KernelRunner> Map;

  KernelRunner runner(const string& name) { return m_[name]; }
  Map& runners() { return m_; }

  static RunnerRegistry* Get();
private:
  RunnerRegistry() {}
  Map m_;
};

struct RunnerRegistrationHelper {
  RunnerRegistrationHelper(RunnerRegistry::KernelRunner k, const char* name) {
    RunnerRegistry::Get()->runners().insert(MP(name, k));
  }
};

#define REGISTER_KERNEL(klass)\
  static KernelRegistrationHelper<klass> k_helper_ ## klass(#klass);

#define REGISTER_METHOD(klass, method)\
  static MethodRegistrationHelper<klass> m_helper_ ## klass ## _ ## method(#klass, #method, &klass::method);

#define REGISTER_RUNNER(r)\
  static RunnerRegistrationHelper r_helper_ ## r ## _(&r, #r);
}
#endif /* KERNELREGISTRY_H_ */
