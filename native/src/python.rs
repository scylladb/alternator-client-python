// Copyright ScyllaDB, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::sync::OnceLock;

use pyo3::exceptions::{PyImportError, PyRuntimeError};
use pyo3::prelude::*;

use crate::{runtime, INTERFACE_VERSION, PYTHON_VERSION};

pub(crate) fn bridge_runtime() -> PyResult<&'static tokio::runtime::Runtime> {
    static BRIDGE: OnceLock<Result<(), String>> = OnceLock::new();
    let runtime = runtime::shared_runtime().map_err(PyRuntimeError::new_err)?;
    BRIDGE
        .get_or_init(|| {
            pyo3_async_runtimes::tokio::init_with_runtime(runtime)
                .map_err(|_| "Alternator async bridge was already initialized".to_owned())
        })
        .as_ref()
        .map_err(|message| PyRuntimeError::new_err(message.clone()))?;
    Ok(runtime)
}

/// Diagnostic only: initialize and execute on the runtime with the GIL released.
#[pyfunction]
fn _runtime_id(py: Python<'_>) -> PyResult<String> {
    py.detach(|| {
        let runtime = bridge_runtime()?;
        Ok(runtime.block_on(async { tokio::runtime::Handle::current().id().to_string() }))
    })
}

/// Diagnostic only: the async bridge executes on the same runtime as sync work.
#[pyfunction]
fn _runtime_id_async(py: Python<'_>) -> PyResult<Bound<'_, PyAny>> {
    bridge_runtime()?;
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        Ok(tokio::runtime::Handle::current().id().to_string())
    })
}

#[pyfunction]
fn _runtime_initialized() -> bool {
    runtime::initialized()
}

#[pymodule(gil_used = true)]
fn _alternator_rust(module: &Bound<'_, PyModule>) -> PyResult<()> {
    let sys = module.py().import("sys")?;
    let version: (u8, u8, u8, String, u8) = sys.getattr("version_info")?.extract()?;
    let implementation: String = sys.getattr("implementation")?.getattr("name")?.extract()?;
    let config = module.py().import("sysconfig")?;
    let free_threaded: bool = config
        .call_method1("get_config_var", ("Py_GIL_DISABLED",))?
        .is_truthy()?;
    if implementation != "cpython"
        || version.0 != 3
        || !(10..=14).contains(&version.1)
        || free_threaded
    {
        return Err(PyImportError::new_err(
            "alternator-client-rust-backend requires GIL-enabled CPython 3.10-3.14",
        ));
    }
    crate::python_client::register(module)?;
    crate::errors::register(module)?;
    module.add("INTERFACE_VERSION", INTERFACE_VERSION)?;
    module.add("__version__", PYTHON_VERSION)?;
    module.add_function(wrap_pyfunction!(_runtime_id, module)?)?;
    module.add_function(wrap_pyfunction!(_runtime_id_async, module)?)?;
    module.add_function(wrap_pyfunction!(_runtime_initialized, module)?)?;
    Ok(())
}
