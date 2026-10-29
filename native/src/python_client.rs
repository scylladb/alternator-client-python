// Copyright ScyllaDB, Inc.
// SPDX-License-Identifier: Apache-2.0

use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyDict};

use crate::{client::Client, config::NativeConfig, python::bridge_runtime};

#[pyclass(name = "NativeClient")]
pub(crate) struct NativeClient {
    pub(crate) client: Client,
    event_loop: Option<Py<PyAny>>,
}

impl NativeClient {
    pub(crate) fn check_loop(&self, py: Python<'_>) -> PyResult<()> {
        if let Some(expected) = &self.event_loop {
            let current = pyo3_async_runtimes::tokio::get_current_loop(py)?;
            if !expected.bind(py).is(&current) {
                return Err(PyRuntimeError::new_err(
                    "Alternator native async client belongs to a different event loop",
                ));
            }
        }
        Ok(())
    }
}

#[pymethods]
impl NativeClient {
    fn execute(
        &self,
        py: Python<'_>,
        operation: &str,
        parameters: &Bound<'_, PyDict>,
    ) -> PyResult<Py<PyAny>> {
        self.check_loop(py)?;
        if self.client.is_closed() {
            return Err(PyRuntimeError::new_err(
                "Alternator native client is closed",
            ));
        }
        let input = crate::operations::input(operation, parameters.as_any())?;
        let runtime = bridge_runtime()?;
        py.detach(|| runtime.block_on(crate::operations::execute(&self.client, input)))
    }

    fn execute_async<'py>(
        &self,
        py: Python<'py>,
        operation: &str,
        parameters: &Bound<'_, PyDict>,
    ) -> PyResult<Bound<'py, PyAny>> {
        self.check_loop(py)?;
        if self.client.is_closed() {
            return Err(PyRuntimeError::new_err(
                "Alternator native client is closed",
            ));
        }
        let input = crate::operations::input(operation, parameters.as_any())?;
        bridge_runtime()?;
        let client = self.client.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            crate::operations::execute(&client, input).await
        })
    }

    fn close(&self, py: Python<'_>) -> PyResult<()> {
        let runtime = bridge_runtime()?;
        self.client.start_close();
        py.detach(|| runtime.block_on(self.client.close()));
        Ok(())
    }

    fn close_async<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.check_loop(py)?;
        bridge_runtime()?;
        self.client.start_close();
        let client = self.client.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            client.close().await;
            Ok(())
        })
    }

    #[getter]
    fn closed(&self) -> bool {
        self.client.is_closed()
    }

    fn _live_drivers(&self) -> usize {
        self.client.live_drivers()
    }
}

fn extract_config(mapping: &Bound<'_, PyDict>) -> PyResult<NativeConfig> {
    // Strict, complete schema: unknown keys cannot be silently ignored. Config
    // extraction and all validation finish before the runtime can start I/O.
    const FIELDS: &[&str] = &[
        "schema_version",
        "seed_hosts",
        "port",
        "scheme",
        "scopes",
        "active_interval_ms",
        "idle_interval_ms",
        "key_affinity",
        "table_pk_attributes",
        "access_key_id",
        "secret_access_key",
        "session_token",
        "region",
        "request_gzip",
        "compression_min_bytes",
        "gzip_level",
        "response_compression",
        "optimize_headers",
        "user_agent",
        "max_attempts",
        "connect_seconds",
        "read_seconds",
        "discovery_seconds",
        "system_roots",
        "ca_pem",
        "client_cert_pem",
        "client_key_pem",
    ];
    for (key, _) in mapping.iter() {
        let key = key
            .extract::<String>()
            .map_err(|_| PyValueError::new_err("native config keys must be strings"))?;
        if !FIELDS.contains(&key.as_str()) {
            return Err(PyValueError::new_err(format!(
                "unknown native config key: {key}"
            )));
        }
    }
    for name in [
        "schema_version",
        "port",
        "active_interval_ms",
        "idle_interval_ms",
        "compression_min_bytes",
        "gzip_level",
        "max_attempts",
        "connect_seconds",
        "read_seconds",
        "discovery_seconds",
    ] {
        if mapping
            .get_item(name)?
            .is_some_and(|value| value.is_instance_of::<PyBool>())
        {
            return Err(PyValueError::new_err(format!(
                "native config {name} must be numeric, not bool"
            )));
        }
    }
    macro_rules! field {
        ($name:ident) => {
            mapping
                .get_item(stringify!($name))?
                .ok_or_else(|| {
                    PyValueError::new_err(concat!("missing native config key: ", stringify!($name)))
                })?
                .extract()
                .map_err(|_| {
                    PyValueError::new_err(concat!(
                        "invalid native config field: ",
                        stringify!($name)
                    ))
                })?
        };
    }
    let config = NativeConfig {
        schema_version: field!(schema_version),
        seed_hosts: field!(seed_hosts),
        port: field!(port),
        scheme: field!(scheme),
        scopes: field!(scopes),
        active_interval_ms: field!(active_interval_ms),
        idle_interval_ms: field!(idle_interval_ms),
        key_affinity: field!(key_affinity),
        table_pk_attributes: field!(table_pk_attributes),
        access_key_id: field!(access_key_id),
        secret_access_key: field!(secret_access_key),
        session_token: field!(session_token),
        region: field!(region),
        request_gzip: field!(request_gzip),
        compression_min_bytes: field!(compression_min_bytes),
        gzip_level: field!(gzip_level),
        response_compression: field!(response_compression),
        optimize_headers: field!(optimize_headers),
        user_agent: field!(user_agent),
        max_attempts: field!(max_attempts),
        connect_seconds: field!(connect_seconds),
        operation_timeout_seconds: field!(read_seconds),
        discovery_seconds: field!(discovery_seconds),
        system_roots: field!(system_roots),
        ca_pem: field!(ca_pem),
        client_cert_pem: field!(client_cert_pem),
        client_key_pem: field!(client_key_pem),
    };
    config.validate().map_err(PyValueError::new_err)?;
    Ok(config)
}

#[pyfunction]
fn create_client(py: Python<'_>, config: &Bound<'_, PyDict>) -> PyResult<NativeClient> {
    let config = extract_config(config)?;
    let runtime = bridge_runtime()?;
    let client = py
        .detach(|| runtime.block_on(Client::create(config)))
        .map_err(PyValueError::new_err)?;
    Ok(NativeClient {
        client,
        event_loop: None,
    })
}

#[pyfunction]
fn create_client_async<'py>(
    py: Python<'py>,
    config: &Bound<'_, PyDict>,
) -> PyResult<Bound<'py, PyAny>> {
    let config = extract_config(config)?;
    let event_loop = Some(pyo3_async_runtimes::tokio::get_current_loop(py)?.unbind());
    bridge_runtime()?;
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let client = Client::create(config)
            .await
            .map_err(PyValueError::new_err)?;
        Ok(NativeClient { client, event_loop })
    })
}

pub(crate) fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<NativeClient>()?;
    module.add_function(wrap_pyfunction!(create_client, module)?)?;
    module.add_function(wrap_pyfunction!(create_client_async, module)?)?;
    Ok(())
}
