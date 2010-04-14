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
  virtual void Init() {}

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
namespace Registry {
  typedef int (*KernelRunner)(ConfigData&);

  typedef map<string, KernelInfo*> KernelMap;
  typedef map<string, KernelRunner> RunnerMap;

  KernelMap& get_kernels();
  RunnerMap& get_runners();

  KernelInfo* get_kernel(const string& name);
  KernelRunner get_runner(const string& name);

  template <class C>
  struct KernelRegistrationHelper {
    KernelRegistrationHelper(const char* name) {
      get_kernels().insert(make_pair(name, new KernelInfoT<C>(name)));
    }
  };

  template <class C>
  struct MethodRegistrationHelper {
    MethodRegistrationHelper(const char* klass, const char* mname, void (C::*m)()) {
      ((KernelInfoT<C>*)get_kernel(klass))->register_method(mname, m);
    }
  };

  struct RunnerRegistrationHelper {
    RunnerRegistrationHelper(KernelRunner k, const char* name) {
      get_runners().insert(MP(name, k));
    }
  };
}

#define REGISTER_KERNEL(klass)\
  Registry::KernelRegistrationHelper<klass> k_helper_ ## klass(#klass);

#define REGISTER_METHOD(klass, method)\
  static Registry::MethodRegistrationHelper<klass> m_helper_ ## klass ## _ ## method(#klass, #method, &klass::method);

#define REGISTER_RUNNER(r)\
  static Registry::RunnerRegistrationHelper r_helper_ ## r ## _(&r, #r);
}
#endif /* KERNELREGISTRY_H_ */
