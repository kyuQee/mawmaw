#pragma once
#include "executor/i_script.hpp"
#include <Python.h>
#include <cstring>
#include <stdexcept>
#include <string>

namespace mawmaw::executor::python {

struct GilGuard {
    PyGILState_STATE state;
    GilGuard()  : state(PyGILState_Ensure())  {}
    ~GilGuard()  { PyGILState_Release(state); }
};

// ── marshal helpers ───────────────────────────────────────────────────────────

inline PyObject* event_to_pydict(const core::Event& ev) {
    PyObject* d = PyDict_New();
    PyDict_SetItemString(d, "stream_id",    PyUnicode_FromString(ev.stream_id));
    PyDict_SetItemString(d, "schema_id",    PyUnicode_FromString(ev.schema_id));
    PyDict_SetItemString(d, "sequence",     PyLong_FromUnsignedLongLong(ev.sequence));
    PyDict_SetItemString(d, "timestamp_ns", PyLong_FromUnsignedLongLong(ev.timestamp_ns));
    PyDict_SetItemString(d, "lineage_depth",PyLong_FromUnsignedLong(ev.lineage_depth));
    PyDict_SetItemString(d, "payload",
        PyBytes_FromStringAndSize(
            reinterpret_cast<const char*>(ev.payload), ev.payload_size));
    return d;
}

inline core::Event pydict_to_event(PyObject* d) {
    core::Event ev;
    auto get_str = [&](const char* key, char* dst, size_t max) {
        PyObject* v = PyDict_GetItemString(d, key);
        if (v && PyUnicode_Check(v)) strncpy(dst, PyUnicode_AsUTF8(v), max - 1);
    };
    auto get_u64 = [&](const char* key) -> uint64_t {
        PyObject* v = PyDict_GetItemString(d, key);
        return (v && PyLong_Check(v)) ? PyLong_AsUnsignedLongLong(v) : 0;
    };
    get_str("stream_id", ev.stream_id, core::STREAM_ID_MAX);
    get_str("schema_id", ev.schema_id, core::SCHEMA_ID_MAX);
    ev.sequence     = get_u64("sequence");
    ev.timestamp_ns = get_u64("timestamp_ns");
    PyObject* pl = PyDict_GetItemString(d, "payload");
    if (pl && PyBytes_Check(pl))
        ev.set_payload(PyBytes_AS_STRING(pl), static_cast<size_t>(PyBytes_GET_SIZE(pl)));
    return ev;
}

// ── PythonScript ─────────────────────────────────────────────────────────────

class PythonScript final : public IScript {
public:
    PythonScript(const std::string& script_id, const std::string& filepath)
        : id_(script_id), filepath_(filepath)
    {
        GilGuard gil;
        PyObject* util    = PyImport_ImportModule("importlib.util");
        if (!util) { PyErr_Print(); throw std::runtime_error("importlib.util missing"); }
        PyObject* spec_fn = PyObject_GetAttrString(util, "spec_from_file_location");
        PyObject* spec    = PyObject_CallFunction(spec_fn, "ss",
                                script_id.c_str(), filepath.c_str());
        Py_DECREF(spec_fn); Py_DECREF(util);
        if (!spec || spec == Py_None) {
            Py_XDECREF(spec);
            throw std::runtime_error("Cannot load spec: " + filepath);
        }
        PyObject* util2     = PyImport_ImportModule("importlib.util");
        PyObject* from_spec = PyObject_GetAttrString(util2, "module_from_spec");
        module_ = PyObject_CallFunction(from_spec, "O", spec);
        Py_DECREF(from_spec); Py_DECREF(util2);
        PyObject* loader  = PyObject_GetAttrString(spec, "loader");
        PyObject* exec_fn = PyObject_GetAttrString(loader, "exec_module");
        PyObject* result  = PyObject_CallFunction(exec_fn, "O", module_);
        Py_XDECREF(result); Py_DECREF(exec_fn); Py_DECREF(loader); Py_DECREF(spec);
        if (PyErr_Occurred()) { PyErr_Print(); throw std::runtime_error("Error executing: " + filepath); }
        on_trigger_ = PyObject_GetAttrString(module_, "on_trigger");
        if (!on_trigger_ || !PyCallable_Check(on_trigger_))
            throw std::runtime_error(filepath + " must define on_trigger()");
    }

    ~PythonScript() { GilGuard gil; Py_XDECREF(on_trigger_); Py_XDECREF(module_); }

    std::string      id()      const override { return id_; }
    std::string      runtime() const override { return "python"; }
    core::ScriptMode mode()    const override { return core::ScriptMode::Snapshot; }

    core::ScriptOutput invoke_snapshot(const core::SnapshotInput& input) override {
        GilGuard gil;
        PyObject* py_trigger = event_to_pydict(input.trigger_event);
        PyObject* py_windows = PyDict_New();
        for (auto& [sid, events] : input.windows) {
            PyObject* list = PyList_New(static_cast<Py_ssize_t>(events.size()));
            for (size_t i = 0; i < events.size(); ++i)
                PyList_SET_ITEM(list, i, event_to_pydict(events[i]));
            PyDict_SetItemString(py_windows, sid.c_str(), list);
            Py_DECREF(list);
        }
        PyObject* py_result = PyObject_CallFunction(on_trigger_, "OO", py_trigger, py_windows);
        Py_DECREF(py_trigger); Py_DECREF(py_windows);
        if (!py_result) { PyErr_Print(); return {}; }
        core::ScriptOutput out;
        if (PyList_Check(py_result)) {
            Py_ssize_t n = PyList_GET_SIZE(py_result);
            out.emitted.reserve(static_cast<size_t>(n));
            for (Py_ssize_t i = 0; i < n; ++i) {
                PyObject* item = PyList_GET_ITEM(py_result, i);
                if (PyDict_Check(item)) out.emitted.push_back(pydict_to_event(item));
            }
        }
        Py_DECREF(py_result);
        return out;
    }

private:
    std::string id_, filepath_;
    PyObject* module_     = nullptr;
    PyObject* on_trigger_ = nullptr;
};

} // namespace mawmaw::executor::python
