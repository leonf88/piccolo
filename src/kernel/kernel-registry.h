#ifndef KERNELREGISTRY_H_
#define KERNELREGISTRY_H_

#include "util/common.h"

namespace dsm {

template <class K, class V>
class TypedGlobalTable;

class Table;
class Worker;

class DSMKernel {
public:
  // The table and shard to be processed.
  int current_shard() const { return shard_; }
  int table_id() const { return table_id_; }

  Table* get_table(int id);

  template <class K, class V>
  TypedGlobalTable<K, V>* get_table(int id) {
    return (TypedGlobalTable<K, V>*)get_table(id);
  }

  // Called once upon construction of the kernel, after the worker
  // and table information has been setup.
  virtual void KernelInit() {}

private:
  friend class Worker;
  friend class Master;

  void Init(Worker* w, int table_id, int shard);

  Worker *w_;
  int shard_;
  int table_id_;
};

struct KernelInfo {
  KernelInfo(const char* name) : name_(name) {}

  virtual DSMKernel* create() = 0;
  virtual void invoke_method(DSMKernel* obj, const string& method_name) = 0;

  string name_;
};

template <class C>
struct KernelInfoT : public KernelInfo {
  typedef void (C::*Method)();
  map<string, Method> methods_;

  KernelInfoT(const char* name) : KernelInfo(name) {}

  DSMKernel* create() { return new C; }

  void invoke_method(DSMKernel* obj, const string& method_id) {
    boost::function<void (C*)> m(methods_[method_id]);
    m((C*)obj);
  }

  void register_method(const char* mname, Method m) { methods_[mname] = m; }
};

namespace Registry {
  typedef map<string, KernelInfo*> KernelMap;
  KernelMap& get_kernels();
  KernelInfo* get_kernel(const string& name);

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
}

#define REGISTER_KERNEL(klass)\
  Registry::KernelRegistrationHelper<klass> k_helper_ ## klass(#klass);

#define REGISTER_METHOD(klass, method)\
  static Registry::MethodRegistrationHelper<klass> m_helper_ ## klass ## _ ## method(#klass, #method, &klass::method);

}
#endif /* KERNELREGISTRY_H_ */
