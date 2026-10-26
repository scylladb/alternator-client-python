// Copyright ScyllaDB, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Owned SDK conversion without Python SDK serialization, validation or transport.
//! Dictionary operations retain Python key comparison semantics, including for
//! accepted str subclasses. The generated module covers the frozen model.
use std::collections::HashMap;
use std::fmt::Display;

use aws_smithy_types::Blob;
use pyo3::exceptions::{PyNotImplementedError, PyRuntimeError};
use pyo3::prelude::*;
use pyo3::types::{
    PyBool, PyByteArray, PyBytes, PyDict, PyFloat, PyInt, PyList, PyString, PyTuple,
};
use serde_json::Value;

use crate::errors::validation;

mod generated;
pub use generated::{Input, Output};

// Bound traversal, including cyclic Python dictionaries/lists, before it can
// exhaust the Rust stack. This is a native representability limit.
const MAX_DEPTH: usize = 128;
// Child paths are borrowed format_args! nodes, not allocated prefixes. Only a
// validation failure renders the chain into its final owned report.
fn check_depth(path: &dyn Display, depth: usize) -> PyResult<()> {
    if depth > MAX_DEPTH {
        Err(validation(path, "nesting exceeds the native limit of 128"))
    } else {
        Ok(())
    }
}

fn dictionary<'py>(
    v: &Bound<'py, PyAny>,
    path: &dyn Display,
    depth: usize,
) -> PyResult<Bound<'py, PyDict>> {
    check_depth(path, depth)?;
    v.cast::<PyDict>()
        .cloned()
        .map_err(|_| validation(path, "expected dict"))
}

fn string(v: &Bound<'_, PyAny>, path: &dyn Display) -> PyResult<String> {
    v.cast::<PyString>()
        .map_err(|_| validation(path, "expected str"))?
        .extract()
        .map_err(|_| validation(path, "string is not representable as UTF-8"))
}

fn boolean(v: &Bound<'_, PyAny>, path: &dyn Display) -> PyResult<bool> {
    v.cast::<PyBool>()
        .map_err(|_| validation(path, "expected bool"))?
        .extract()
}

fn integer(v: &Bound<'_, PyAny>, path: &dyn Display) -> PyResult<i32> {
    if v.is_instance_of::<PyBool>() || !v.is_instance_of::<PyInt>() {
        return Err(validation(path, "expected int"));
    }
    v.extract()
        .map_err(|_| validation(path, "integer is outside the native i32 range"))
}

fn double(v: &Bound<'_, PyAny>, path: &dyn Display) -> PyResult<f64> {
    if v.is_instance_of::<PyBool>()
        || !(v.is_instance_of::<PyInt>() || v.is_instance_of::<PyFloat>())
    {
        return Err(validation(path, "expected float or int"));
    }
    let result: f64 = v
        .extract()
        .map_err(|_| validation(path, "number is outside the native f64 range"))?;
    if !result.is_finite() {
        return Err(validation(path, "number must be finite"));
    }
    Ok(result)
}

fn blob(v: &Bound<'_, PyAny>, path: &dyn Display) -> PyResult<Blob> {
    if let Ok(bytes) = v.cast::<PyBytes>() {
        return Ok(Blob::new(bytes.as_bytes().to_vec()));
    }
    if let Ok(bytes) = v.cast::<PyByteArray>() {
        // Copy while holding the GIL; no Python memory crosses the boundary.
        return Ok(Blob::new(bytes.to_vec()));
    }
    Err(validation(path, "expected bytes or bytearray"))
}

fn sequence<'py>(
    v: &Bound<'py, PyAny>,
    path: &dyn Display,
    depth: usize,
) -> PyResult<Box<dyn ExactSizeIterator<Item = Bound<'py, PyAny>> + 'py>> {
    check_depth(path, depth)?;
    // Keep one native iterator per active sequence, rather than cloning every
    // element reference before the first recursive visit. No Python callbacks.
    if let Ok(list) = v.cast::<PyList>() {
        return Ok(Box::new(list.iter()));
    }
    if let Ok(tuple) = v.cast::<PyTuple>() {
        return Ok(Box::new(tuple.iter()));
    }
    Err(validation(path, "expected list or tuple"))
}

fn members(
    v: &Bound<'_, PyDict>,
    allowed: &[&str],
    required: &[&str],
    path: &dyn Display,
) -> PyResult<()> {
    for (key, _) in v.iter() {
        let key = string(&key, path)?;
        if !allowed.contains(&key.as_str()) {
            return Err(validation(
                &format_args!("{path}.{key}"),
                "unknown parameter",
            ));
        }
    }
    for key in required {
        if !v.contains(key)? {
            return Err(validation(
                &format_args!("{path}.{key}"),
                "missing required parameter",
            ));
        }
    }
    Ok(())
}

fn length(path: &dyn Display, len: usize, min: Option<usize>, max: Option<usize>) -> PyResult<()> {
    if min.is_some_and(|min| len < min) || max.is_some_and(|max| len > max) {
        let expected = match (min, max) {
            (Some(min), Some(max)) => format!("between {min} and {max}"),
            (Some(min), None) => format!("at least {min}"),
            (None, Some(max)) => format!("at most {max}"),
            (None, None) => "any length".to_owned(),
        };
        return Err(validation(
            path,
            &format!("invalid length {len}; expected {expected}"),
        ));
    }
    Ok(())
}

// SDK build() is fallible for some structures and infallible for others.
trait Finish<T> {
    fn finish(self, path: &dyn Display) -> PyResult<T>;
}
impl<T> Finish<T> for T {
    fn finish(self, _: &dyn Display) -> PyResult<T> {
        Ok(self)
    }
}
impl<T> Finish<T> for Result<T, aws_smithy_types::error::operation::BuildError> {
    fn finish(self, path: &dyn Display) -> PyResult<T> {
        self.map_err(|e| validation(path, &e.to_string()))
    }
}

fn scalar_present(presence: Option<&Value>, field: &str) -> bool {
    presence.is_none_or(|v| v.get(field).is_some_and(|v| !v.is_null()))
}
