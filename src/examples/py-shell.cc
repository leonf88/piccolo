#include <python2.6/Python.h>
#include <boost/python.hpp>

#include "client.h"
#include "examples/python_support.i"
using namespace dsm;
using namespace boost::python;

DSMKernel* kernel() { return NULL; }

extern "C" void init_python_support();
int main(int argc, const char* argv[]) {
  Init(argc, (char**)argv);

  Py_Initialize();
  init_python_support();
  try {
    object sys_module = import("sys");
    object sys_ns = sys_module.attr("__dict__");
    sys_ns["argv"] = boost::python::list();
    exec("path += ['src/examples/crawler', 'src/examples/']", sys_ns, sys_ns);
    exec("print path", sys_ns, sys_ns);
    object code_module = import("IPython.Shell");
    object code_ns = code_module.attr("__dict__");

    exec("import python_support as ps", code_ns, code_ns);
    exec("start().mainloop()", code_ns, code_ns);
 } catch (error_already_set e) {
    PyErr_Print();
    exit(1);
  }
  return 0;
}
