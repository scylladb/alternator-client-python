// Copyright ScyllaDB, Inc.
// SPDX-License-Identifier: Apache-2.0
#![cfg(feature = "conversion-tests")]

use _alternator_rust::{
    conversion::{Input, Output},
    metadata::{AttemptResponse, OperationMetadata},
    response,
};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyModule};
use std::ffi::CString;

#[pyfunction]
fn request<'py>(
    py: Python<'py>,
    operation: &str,
    value: &Bound<'py, PyAny>,
) -> PyResult<Bound<'py, PyAny>> {
    let input = Input::from_python(operation, value)?;
    // A real detach verifies that the entire request is owned and Send.
    let input = py.detach(move || input);
    input.to_python(py, None)
}

#[pyfunction]
fn owned_request<'py>(
    py: Python<'py>,
    operation: &str,
    value: &Bound<'py, PyAny>,
) -> PyResult<Bound<'py, PyAny>> {
    let input = Input::from_python(operation, value)?;
    value.cast::<PyDict>()?.clear();
    let input = py.detach(move || input);
    input.to_python(py, None)
}

#[pyfunction]
fn output<'py>(
    py: Python<'py>,
    operation: &str,
    value: &Bound<'py, PyAny>,
    body: Vec<u8>,
    headers: Vec<(String, Vec<u8>)>,
    attempts: u32,
) -> PyResult<Bound<'py, PyDict>> {
    let output = Output::from_python(operation, value)?;
    let metadata = metadata(body, headers, attempts);
    response::success(py, &output, &metadata)
}

fn metadata(body: Vec<u8>, headers: Vec<(String, Vec<u8>)>, attempts: u32) -> OperationMetadata {
    OperationMetadata {
        endpoint_url: None,
        attempts,
        responses: vec![AttemptResponse {
            status: 200,
            attempt: attempts,
            headers,
            body,
        }],
    }
}

#[pyfunction]
#[pyo3(signature = (operation, status, body, headers, attempts, modeled=None))]
fn service(
    py: Python<'_>,
    operation: &str,
    status: u16,
    body: Vec<u8>,
    headers: Vec<(String, Vec<u8>)>,
    attempts: u32,
    modeled: Option<&str>,
) -> PyResult<Py<PyAny>> {
    let mut metadata = metadata(body, headers, attempts);
    metadata.responses[0].status = status;
    let modeled = match modeled {
        Some("ConditionalCheckFailedException") => {
            Some(response::ModeledError::ConditionalCheckFailed)
        }
        Some("TransactionCanceledException") => Some(response::ModeledError::TransactionCanceled),
        None => None,
        Some(_) => {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "unknown modeled shape",
            ))
        }
    };
    Ok(response::service_error(py, operation, &metadata, modeled)?
        .into_value(py)
        .into_any())
}

#[test]
fn shared_contract_and_native_edge_cases() {
    Python::attach(|py| {
        let module = PyModule::new(py, "conversion_test")?;
        module.add_function(wrap_pyfunction!(request, &module)?)?;
        module.add_function(wrap_pyfunction!(owned_request, &module)?)?;
        module.add_function(wrap_pyfunction!(output, &module)?)?;
        module.add_function(wrap_pyfunction!(service, &module)?)?;
        _alternator_rust::errors::register(&module)?;
        let sys = py.import("sys")?;
        sys.getattr("path")?
            .call_method1("insert", (0, concat!(env!("CARGO_MANIFEST_DIR"), "/..")))?;
        sys.getattr("modules")?
            .set_item("conversion_test", &module)?;
        let code = CString::new(include_str!("conversion_cases.py")).unwrap();
        let filename = CString::new(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/conversion_cases.py"
        ))
        .unwrap();
        PyModule::from_code(py, &code, &filename, c"conversion_cases")?;
        PyResult::Ok(())
    })
    .unwrap();
}

#[test]
fn final_attempt_and_defaulted_scalars() {
    use aws_sdk_dynamodb::operation::query::QueryOutput;
    Python::attach(|py| {
        let mut metadata = metadata(b"{}".to_vec(), vec![], 3);
        let value = Output::Query(Box::new(QueryOutput::builder().count(19).build()));
        let result = response::success(py, &value, &metadata)?;
        assert!(!result.contains("Count")?);
        metadata.responses[0].body = b"{\"Count\":0}".to_vec();
        let result = response::success(py, &value, &metadata)?;
        assert_eq!(result.get_item("Count")?.unwrap().extract::<i32>()?, 19);
        // Presence comes from the body; the actual value comes from the SDK.
        metadata.attempts = 4;
        assert!(response::success(py, &value, &metadata).is_err());
        assert!(response::service_error(py, "Query", &metadata, None).is_err());
        PyResult::Ok(())
    })
    .unwrap();
}

#[test]
fn compact_presence_borrows_body_and_omits_bulk_attributes() {
    use aws_sdk_dynamodb::operation::query::QueryOutput;
    use serde_json::json;
    let output = Output::Query(Box::new(QueryOutput::builder().count(19).build()));
    let body = json!({
        "Count": 0,
        "ScannedCount": null,
        "Items": [{"large": {"B": "AAAA".repeat(64 * 1024)}}],
        "future": ["ignored"],
    });
    let mut metadata = metadata(serde_json::to_vec(&body).unwrap(), vec![], 1);
    let attempt = &metadata.responses[0];
    assert!(matches!(
        response::decoded_body(attempt).unwrap(),
        std::borrow::Cow::Borrowed(_)
    ));
    assert_eq!(
        response::success_presence(&output, attempt).unwrap(),
        json!({"Count": true, "ScannedCount": null})
    );
    // Skipped branches still need valid JSON, and only object roots are valid.
    for body in [
        b"{} trailing".as_slice(),
        b"[]",
        b"null",
        br#"{"ignored":"\q"}"#,
    ] {
        metadata.responses[0].body = body.to_vec();
        assert!(response::success_presence(&output, &metadata.responses[0]).is_err());
    }
}

#[test]
fn sdk_transport_categories_and_service_projection() {
    use _alternator_rust::{
        errors,
        transport_failure::{Category, TransportFailure},
    };
    use aws_sdk_dynamodb::operation::get_item::GetItemError;
    use aws_smithy_runtime_api::client::{
        orchestrator::HttpResponse,
        result::{ConnectorError, SdkError},
    };
    use std::io;
    Python::attach(|py| {
        for category in [
            Category::EndpointConnection,
            Category::ConnectionClosed,
            Category::ConnectTimeout,
            Category::ReadTimeout,
            Category::Tls,
        ] {
            let failure = TransportFailure::new(
                category,
                "http://node:8000",
                Box::new(io::Error::other("scripted")),
            );
            let error: SdkError<GetItemError, HttpResponse> =
                SdkError::dispatch_failure(ConnectorError::io(Box::new(failure)));
            let mapped = errors::sdk_error(
                py,
                "GetItem",
                "unused",
                &error,
                &OperationMetadata::default(),
            )?;
            assert_eq!(
                mapped.value(py).getattr("category")?.extract::<String>()?,
                category.as_str()
            );
            assert_eq!(
                mapped
                    .value(py)
                    .getattr("endpoint_url")?
                    .extract::<String>()?,
                "http://node:8000"
            );
        }
        let timeout: SdkError<GetItemError, HttpResponse> =
            SdkError::timeout_error("operation deadline");
        let error = errors::sdk_error(
            py,
            "GetItem",
            "http://node",
            &timeout,
            &OperationMetadata::default(),
        )?;
        assert_eq!(
            error.value(py).getattr("category")?.extract::<String>()?,
            "read_timeout"
        );
        let error: SdkError<GetItemError, HttpResponse> =
            SdkError::construction_failure("runtime failure");
        assert!(errors::sdk_error(
            py,
            "GetItem",
            "unused",
            &error,
            &OperationMetadata::default()
        )?
        .is_instance_of::<pyo3::exceptions::PyRuntimeError>(py));
        PyResult::Ok(())
    })
    .unwrap();
}
