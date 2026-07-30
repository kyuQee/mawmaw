#pragma once
#include <cstdlib>
#include <dlfcn.h>
#include <Python.h>
#include <stdexcept>
#include <string>

namespace mawmaw::executor::python {

class PythonEngine {
public:
    PythonEngine() {
        load_library();
        if (Py_IsInitialized())
            throw std::runtime_error("PythonEngine: interpreter already initialised");
        Py_Initialize();
        if (!Py_IsInitialized())
            throw std::runtime_error("PythonEngine: Py_Initialize() failed");
        saved_thread_ = PyEval_SaveThread();
    }

    ~PythonEngine() {
        if (Py_IsInitialized()) {
            PyEval_RestoreThread(saved_thread_);
            Py_Finalize();
        }
    }

    PythonEngine(const PythonEngine&) = delete;
    PythonEngine& operator=(const PythonEngine&) = delete;
    PythonEngine(PythonEngine&&) = delete;
    PythonEngine& operator=(PythonEngine&&) = delete;

    void add_to_path(const std::string& dir) {
        PyGILState_STATE state = PyGILState_Ensure();
        PyObject* sys_path = PySys_GetObject("path");
        PyObject* py_dir = PyUnicode_FromString(dir.c_str());
        PyList_Append(sys_path, py_dir);
        Py_DECREF(py_dir);
        PyGILState_Release(state);
    }

private:
    PyThreadState* saved_thread_ = nullptr;

    static void load_library() {
        static bool loaded = false;
        if (loaded) return;

        void* handle = nullptr;
        std::string last_error;

        // Honour PYTHON_LIBRARY environment variable if set
        if (const char* env_path = std::getenv("PYTHON_LIBRARY")) {
            handle = dlopen(env_path, RTLD_NOW | RTLD_GLOBAL);
            if (handle) {
                loaded = true;
                return;
            }
            last_error = dlerror();
        }

        // Try common sonames
        const char* lib_names[] = {
            "libpython3.so",
            "libpython3.10.so",
            "libpython3.11.so",
            "libpython3.12.so",
            "libpython3.13.so"
        };
        const int num_names = sizeof(lib_names) / sizeof(lib_names[0]);

        for (int i = 0; i < num_names; ++i) {
            handle = dlopen(lib_names[i], RTLD_NOW | RTLD_GLOBAL);
            if (handle) {
                loaded = true;
                return;
            }
            last_error = dlerror();
        }

        // Build a readable list of tried names for the error message
        std::string tried;
        for (int i = 0; i < num_names; ++i) {
            if (i) tried += ", ";
            tried += lib_names[i];
        }

        throw std::runtime_error(
            "Failed to load libpython: " + last_error +
            " (tried: " + tried + ")"
        );
    }
};

} // namespace mawmaw::executor::python
