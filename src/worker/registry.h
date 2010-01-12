#ifndef KERNEL_H_
#define KERNEL_H_

#include "util/common.h"
#include "worker/table.h"
#include "worker/table-internal.h"

namespace upc {

class Worker;
class Master;

class DSMKernel {
public:
  // The table and shard to be processed.
  int shard() const { return shard_; }
  int table_id() const { return table_id_; }

  Table* get_table(int id);

  template <class K, class V>
  TypedTable<K, V>* get_table(int id) {
    return (TypedTable<K, V>*)get_table(id);
  }

  // Called once upon construction of the kernel, after the worker
  // and table information has been setup.
  virtual void KernelInit() = 0;

private:
  friend class Worker;
  friend class Master;

  void Init(Worker* w, int table_id, int shard);

  Worker *w_;
  int shard_;
  int table_id_;
};

struct KernelHelperBase {
  KernelHelperBase(const char* name) : name_(name) {}

  virtual DSMKernel* create() = 0;
  virtual void invoke_method(DSMKernel* obj, int method_idx) = 0;

  string name_;
};

template <class C>
struct KernelHelper : public KernelHelperBase {
  KernelHelper(const char* name) : KernelHelperBase(name) {}

  DSMKernel* create() { return new C; }

  void invoke_method(DSMKernel* obj, int method_idx) {
    boost::function<void (C*)> m(methods_[method_idx]);
    m((C*)obj);
  }

  typedef void (C::*Method)();
  int method_id(Method f) {
     for (int i = 0; i < methods_.size(); ++i) {
       if (methods_[i] == f) {
         return i;
       }
     }

     LOG(FATAL) << "Method not found!";
  }

  void register_method(Method m) {
    methods_.push_back(m);
  }

  vector<Method> methods_;
};

namespace Registry {
  KernelHelperBase* get_helper(const string& name);
  DSMKernel* create_kernel(const string& name);
  map<string, KernelHelperBase*>* get_mapping();

  template <class C>
  struct KernelRegistrationHelper {
    KernelRegistrationHelper(const char* name) {
      get_mapping()->insert(make_pair(name, new KernelHelper<C>(name)));
    }
  };

  template <class C>
  struct MethodRegistrationHelper {
    MethodRegistrationHelper(const char* klass, void (C::*m)()) {
      ((KernelHelper<C>*)get_helper(klass))->register_method(m);
    }
  };
}

#define REGISTER_KERNEL(klass)\
  Registry::KernelRegistrationHelper<klass> k_helper_ ## klass(#klass);

#define REGISTER_METHOD(klass, method)\
  static Registry::MethodRegistrationHelper<klass> m_helper_ ## klass ## _ ## method(#klass, &klass::method);

} // end namespace
#endif /* KERNEL_H_ */
