// Copyright ScyllaDB, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Reserved version-1 exceptions. The Python adapter supplies botocore classes.
use pyo3::create_exception;
use pyo3::exceptions::PyException;
use pyo3::prelude::*;

create_exception!(_alternator_rust, ValidationError, PyException);
create_exception!(_alternator_rust, ServiceError, PyException);
create_exception!(_alternator_rust, TransportError, PyException);

pub fn validation(path: &dyn std::fmt::Display, message: &str) -> PyErr {
    Python::attach(|py| {
        let report = format!("{path}: {message}");
        let error = ValidationError::new_err(report.clone());
        // Exception instances have a writable __dict__.
        error
            .value(py)
            .setattr("report", report)
            .expect("exception report");
        error
    })
}

pub fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add("ValidationError", module.py().get_type::<ValidationError>())?;
    module.add("ServiceError", module.py().get_type::<ServiceError>())?;
    module.add("TransportError", module.py().get_type::<TransportError>())?;
    Ok(())
}

pub fn transport_error(
    py: Python<'_>,
    category: crate::transport_failure::Category,
    endpoint_url: &str,
    message: &str,
) -> PyResult<PyErr> {
    let error = TransportError::new_err(message.to_owned());
    error.value(py).setattr("category", category.as_str())?;
    error.value(py).setattr("endpoint_url", endpoint_url)?;
    Ok(error)
}

/// Convert SDK errors after the complete operation/capture has finished. Service
/// errors must have a final HTTP response; never borrow a previous attempt's body.
pub fn sdk_error<E>(
    py: Python<'_>,
    operation: &str,
    endpoint_url: &str,
    error: &aws_smithy_runtime_api::client::result::SdkError<
        E,
        aws_smithy_runtime_api::client::orchestrator::HttpResponse,
    >,
    metadata: &crate::metadata::OperationMetadata,
) -> PyResult<PyErr>
where
    E: std::error::Error
        + aws_smithy_types::error::metadata::ProvideErrorMetadata
        + Send
        + Sync
        + 'static,
{
    use crate::transport_failure::{Category, TransportFailure};
    use aws_smithy_runtime_api::client::result::SdkError;
    if let SdkError::ServiceError(service) = error {
        // The pinned SDK also wraps successful-status deserialization failures
        // in ServiceError::unhandled. Preserve those as protocol failures.
        if !(200..300).contains(&service.raw().status().as_u16()) {
            // Parsed modeled/unknown errors retain their protocol code. Parsed
            // code-less HTTP errors instead wrap ErrorMetadata itself. The
            // pinned SDK's deserialization failures have neither signal; pass
            // that distinction to projection without matching error messages
            // or treating every Unhandled variant as a protocol failure.
            let protocol_failure = (service.err().code().is_none()
                && !service
                    .err()
                    .source()
                    .is_some_and(|source| source.is::<aws_smithy_types::error::ErrorMetadata>()))
            .then(|| {
                format!(
                    "{}",
                    aws_smithy_types::error::display::DisplayErrorContext(error)
                )
            });
            return crate::response::service_error_with_protocol_failure(
                py,
                operation,
                metadata,
                protocol_failure,
            );
        }
    }
    if let Some(failure) = TransportFailure::find(error) {
        return transport_error(
            py,
            failure.category,
            &failure.endpoint_url,
            &failure.to_string(),
        );
    }
    if matches!(error, SdkError::TimeoutError(_))
        || matches!(error, SdkError::DispatchFailure(e) if e.is_timeout())
    {
        return transport_error(py, Category::ReadTimeout, endpoint_url, &error.to_string());
    }
    // Construction, unparseable protocol responses, runtime and closed-client
    // failures have no service-error or network-category substitution.
    Ok(pyo3::exceptions::PyRuntimeError::new_err(error.to_string()))
}
