#pragma once
#include <Python.h>
#include <stdexcept>
#include <string>

namespace mawmaw::executor::python {

// Owns the CPython interpreter lifecycle.
// Construct once at startup, destroy at shutdown. Non-copyable, non-movable.
class PythonEngine {
public:
    PythonEngine() {
        if (Py_IsInitialized())
            throw std::runtime_error("PythonEngine: interpreter already initialised");
        Py_Initialize();
        if (!Py_IsInitialized())
            throw std::runtime_error("PythonEngine: Py_Initialize() failed");
        // Release GIL — allows worker threads to acquire via PyGILState_Ensure
        saved_thread_ = PyEval_SaveThread();
    }

    ~PythonEngine() {
        if (Py_IsInitialized()) {
            PyEval_RestoreThread(saved_thread_);
            Py_Finalize();
        }
    }

    PythonEngine(const PythonEngine&)            = delete;
    PythonEngine& operator=(const PythonEngine&) = delete;
    PythonEngine(PythonEngine&&)                 = delete;
    PythonEngine& operator=(PythonEngine&&)      = delete;

    void add_to_path(const std::string& dir) {
        PyGILState_STATE state = PyGILState_Ensure();
        PyObject* sys_path = PySys_GetObject("path");
        PyObject* py_dir   = PyUnicode_FromString(dir.c_str());
        PyList_Append(sys_path, py_dir);
        Py_DECREF(py_dir);
        PyGILState_Release(state);
    }

private:
    PyThreadState* saved_thread_ = nullptr;
};

} // namespace mawmaw::executor::python
