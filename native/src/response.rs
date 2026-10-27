// Copyright ScyllaDB, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Response and service error projection from the operation's own final attempt.
use std::borrow::Cow;
use std::collections::HashMap;
use std::io::Read;

use base64::Engine;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyBytes, PyDict, PyFloat, PyInt, PyList, PyString};
use serde_json::value::RawValue;
use serde_json::Value;

use crate::conversion::Output;
use crate::errors::ServiceError;
use crate::metadata::{AttemptResponse, OperationMetadata};

fn failure(message: impl ToString) -> PyErr {
    PyRuntimeError::new_err(message.to_string())
}

pub fn final_response(metadata: &OperationMetadata) -> PyResult<&AttemptResponse> {
    metadata
        .responses
        .last()
        .filter(|r| r.attempt == metadata.attempts)
        .ok_or_else(|| failure("no HTTP response for the final operation attempt"))
}

// Match the pinned SDK's default bound on every expanded encoding layer.
const MAX_DECOMPRESSED_BYTES: u64 = 32 * 1024 * 1024;

fn read_decoded(mut decoder: impl Read) -> Result<Vec<u8>, std::io::Error> {
    let mut decoded = Vec::new();
    decoder
        .by_ref()
        .take(MAX_DECOMPRESSED_BYTES)
        .read_to_end(&mut decoded)?;
    if decoder.read(&mut [0])? != 0 {
        return Err(std::io::Error::other(format!(
            "decompressed response exceeds {MAX_DECOMPRESSED_BYTES} byte limit"
        )));
    }
    Ok(decoded)
}

pub fn decoded_body(response: &AttemptResponse) -> Result<Cow<'_, [u8]>, std::io::Error> {
    let mut body = Cow::Borrowed(response.body.as_slice());
    // HTTP encodings apply in order; remove in reverse. The capture is immutable.
    let encoding = response
        .headers
        .iter()
        .filter(|(name, _)| name.eq_ignore_ascii_case("content-encoding"))
        .map(|(_, value)| String::from_utf8_lossy(value).into_owned())
        .collect::<Vec<_>>()
        .join(",");
    for encoding in encoding.split(',').rev() {
        // Match the pinned SDK's aliases, parameters and whitespace handling.
        let encoding = encoding.split(';').next().unwrap_or(encoding).trim();
        let decoded = match encoding.to_ascii_lowercase().as_str() {
            "" | "identity" => continue,
            "gzip" | "x-gzip" => {
                // The SDK accepts just the first gzip member and ignores trailers.
                read_decoded(flate2::read::GzDecoder::new(body.as_ref()))?
            }
            "deflate" => {
                // Both RFC zlib and the common raw-deflate form are accepted.
                read_decoded(flate2::read::ZlibDecoder::new(body.as_ref())).or_else(|error| {
                    // flate2 reports malformed zlib data as InvalidInput.
                    // Preserve our expansion-limit error rather than retrying it.
                    if error.kind() == std::io::ErrorKind::InvalidInput {
                        read_decoded(flate2::read::DeflateDecoder::new(body.as_ref()))
                    } else {
                        Err(error)
                    }
                })?
            }
            other => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("unsupported response encoding: {other}"),
                ))
            }
        };
        body = Cow::Owned(decoded);
    }
    Ok(body)
}

pub fn metadata_to_python<'py>(
    py: Python<'py>,
    metadata: &OperationMetadata,
) -> PyResult<Bound<'py, PyDict>> {
    let response = final_response(metadata)?;
    let result = PyDict::new(py);
    let headers = PyDict::new(py);
    for (name, value) in &response.headers {
        // HTTP header values are octets, decoded losslessly as Latin-1 like
        // Python's HTTP layer, without replacing high bytes or changing values.
        let value: String = value.iter().map(|v| char::from(*v)).collect();
        let name = name.to_ascii_lowercase();
        let value = match headers.get_item(&name)? {
            Some(previous) => format!("{}, {value}", previous.extract::<String>()?),
            None => value,
        };
        headers.set_item(name, value)?;
    }
    result.set_item("HTTPStatusCode", response.status)?;
    result.set_item("HTTPHeaders", headers)?;
    if let Some(id) = response.request_id() {
        result.set_item("RequestId", id)?;
    }
    result.set_item("RetryAttempts", metadata.attempts.saturating_sub(1))?;
    Ok(result)
}

/// Generated routes cover every output member guarded by scalar_present, plus
/// only the maps/lists/structures needed to reach those members.
pub(crate) enum Presence {
    Marker,
    Object(&'static [(&'static str, Presence)]),
    Map(&'static Presence),
    List(&'static Presence),
}

fn project_presence(body: &[u8], route: &Presence) -> Result<Value, serde_json::Error> {
    if body == b"null" {
        return Ok(Value::Null);
    }
    Ok(match route {
        Presence::Marker => Value::Bool(true),
        Presence::Object(fields) => {
            let values: HashMap<String, &RawValue> = serde_json::from_slice(body)?;
            let mut result = serde_json::Map::new();
            for (name, child) in *fields {
                if let Some(value) = values.get(*name) {
                    result.insert(
                        (*name).into(),
                        project_presence(value.get().as_bytes(), child)?,
                    );
                }
            }
            Value::Object(result)
        }
        Presence::Map(child) => {
            let values: HashMap<String, &RawValue> = serde_json::from_slice(body)?;
            Value::Object(
                values
                    .into_iter()
                    .map(|(key, value)| Ok((key, project_presence(value.get().as_bytes(), child)?)))
                    .collect::<Result<_, serde_json::Error>>()?,
            )
        }
        Presence::List(child) => {
            let values: Vec<&RawValue> = serde_json::from_slice(body)?;
            Value::Array(
                values
                    .into_iter()
                    .map(|value| project_presence(value.get().as_bytes(), child))
                    .collect::<Result<_, _>>()?,
            )
        }
    })
}

pub fn success_presence(output: &Output, response: &AttemptResponse) -> PyResult<Value> {
    let body = decoded_body(response).map_err(failure)?;
    let body = if body.is_empty() {
        b"{}"
    } else {
        body.as_ref()
    };
    // RawValue borrows skipped branches without allocating their attribute
    // strings or containers. Relevant branches are scanned again; compressed
    // responses still need this separate decoding pass after SDK parsing.
    let presence = project_presence(body, output.presence_route()).map_err(failure)?;
    if !presence.is_object() {
        return Err(failure("expected a JSON object response"));
    }
    Ok(presence)
}

pub fn success<'py>(
    py: Python<'py>,
    output: &Output,
    metadata: &OperationMetadata,
) -> PyResult<Bound<'py, PyDict>> {
    let presence = success_presence(output, final_response(metadata)?)?;
    let result = output
        .to_python(py, Some(&presence))?
        .cast_into::<PyDict>()?;
    result.set_item("ResponseMetadata", metadata_to_python(py, metadata)?)?;
    Ok(result)
}

fn scalar_to_python<'py>(py: Python<'py>, value: Value) -> PyResult<Bound<'py, PyAny>> {
    Ok(match value {
        Value::Null => py.None().into_bound(py),
        Value::Bool(v) => v.into_pyobject(py)?.to_owned().into_any(),
        Value::Number(v) => {
            if let Some(v) = v.as_i64() {
                v.into_pyobject(py)?.into_any()
            } else if let Some(v) = v.as_u64() {
                v.into_pyobject(py)?.into_any()
            } else if !v.to_string().contains(['.', 'e', 'E']) {
                // arbitrary_precision retains integer tokens beyond i64/u64.
                py.get_type::<PyInt>().call1((v.to_string(),))?
            } else {
                // Python JSON parsing permits float overflow to +/-infinity.
                py.get_type::<PyFloat>().call1((v.to_string(),))?
            }
        }
        Value::String(v) => v.into_pyobject(py)?.into_any(),
        Value::Array(_) | Value::Object(_) => unreachable!("scalar JSON token"),
    })
}

// Serde's byte-string visitor preserves escaped lone UTF-16 surrogates as
// WTF-8. Construct Python Unicode without a Rust String or Python JSON call.
fn json_string<'py>(py: Python<'py>, quoted: &[u8]) -> PyResult<Bound<'py, PyString>> {
    struct StringVisitor<'py>(Python<'py>);
    impl<'de, 'py> serde_core::de::Visitor<'de> for StringVisitor<'py> {
        type Value = PyResult<Bound<'py, PyString>>;
        fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("a JSON string")
        }
        fn visit_bytes<E: serde_core::de::Error>(self, value: &[u8]) -> Result<Self::Value, E> {
            Ok(PyString::from_encoded_object(
                PyBytes::new(self.0, value).as_any(),
                Some(c"utf-8"),
                Some(c"surrogatepass"),
            ))
        }
    }
    use serde_core::de::Deserializer;
    serde_json::Deserializer::from_slice(quoted)
        .deserialize_bytes(StringVisitor(py))
        .map_err(failure)?
}

fn json_to_python<'py, 'body>(
    py: Python<'py>,
    body: &'body [u8],
) -> PyResult<(Bound<'py, PyAny>, Option<&'body [u8]>)> {
    use aws_smithy_json::deserialize::{
        json_token_iter, token::expect_number_as_string_or_null, Token,
    };
    enum Frame<'py> {
        Object(Bound<'py, PyDict>, Option<Bound<'py, PyString>>),
        Array(Bound<'py, PyList>),
    }
    let mut frames = Vec::new();
    let mut root = None;
    let mut message_start = None;
    let mut message_spans = [None, None];
    // The caller validated the root RawValue. SDK tokens advance once through
    // its bytes; frames retain only active containers and one pending key.
    for token in json_token_iter(body) {
        let token = token.map_err(failure)?;
        let start = token.offset().0;
        let (value, frame, end) = match token {
            Token::ObjectKey { key, .. } => {
                let quoted = &body[start..start + key.as_escaped_str().len() + 2];
                if let Some(Frame::Object(_, pending)) = frames.last_mut() {
                    *pending = Some(json_string(py, quoted)?);
                }
                continue;
            }
            Token::EndObject { .. } | Token::EndArray { .. } => {
                if frames.len() == 2 {
                    if let Some((index, begin)) = message_start.take() {
                        message_spans[index] = Some((begin, start + 1));
                    }
                }
                frames
                    .pop()
                    .ok_or_else(|| failure("unmatched JSON container"))?;
                continue;
            }
            Token::StartObject { .. } => {
                let value = PyDict::new(py);
                (
                    value.clone().into_any(),
                    Some(Frame::Object(value, None)),
                    start + 1,
                )
            }
            Token::StartArray { .. } => {
                let value = PyList::empty(py);
                (
                    value.clone().into_any(),
                    Some(Frame::Array(value)),
                    start + 1,
                )
            }
            Token::ValueString { value, .. } => {
                let end = start + value.as_escaped_str().len() + 2;
                (json_string(py, &body[start..end])?.into_any(), None, end)
            }
            token @ Token::ValueNumber { .. } => {
                let raw = expect_number_as_string_or_null(Some(Ok(token)), body)
                    .map_err(failure)?
                    .ok_or_else(|| failure("expected a JSON number"))?;
                (
                    scalar_to_python(py, serde_json::from_str(raw).map_err(failure)?)?,
                    None,
                    start + raw.len(),
                )
            }
            Token::ValueBool { value, .. } => (
                value.into_pyobject(py)?.to_owned().into_any(),
                None,
                start + if value { 4 } else { 5 },
            ),
            Token::ValueNull { .. } => (py.None().into_bound(py), None, start + 4),
        };
        let top_level = frames.len() == 1;
        match frames.last_mut() {
            Some(Frame::Object(object, pending)) => {
                let key = pending
                    .take()
                    .ok_or_else(|| failure("missing JSON object key"))?;
                if top_level {
                    let index = if key.as_any().eq("message")? {
                        Some(0)
                    } else if key.as_any().eq("Message")? {
                        Some(1)
                    } else {
                        None
                    };
                    if let Some(index) = index {
                        if frame.is_some() {
                            message_start = Some((index, start));
                        } else {
                            message_spans[index] = Some((start, end));
                        }
                    }
                }
                object.set_item(key, &value)?;
            }
            Some(Frame::Array(array)) => array.append(&value)?,
            None => root = Some(value),
        }
        if let Some(frame) = frame {
            frames.push(frame);
        }
    }
    let message = message_spans[0]
        .or(message_spans[1])
        .map(|(start, end)| &body[start..end]);
    Ok((root.ok_or_else(|| failure("missing JSON value"))?, message))
}

fn decode_attribute(value: &Bound<'_, PyAny>, depth: usize) -> PyResult<()> {
    if depth > 128 {
        return Err(failure(
            "service AttributeValue nesting exceeds native limit",
        ));
    }
    let dict = value.cast::<PyDict>().map_err(failure)?;
    // The SDK skips null union members when another variant is present.
    let mut variants = 0;
    for (key, value) in dict.iter() {
        if !value.is_none() && key.extract::<String>()? != "__type" {
            variants += 1;
            if variants > 1 {
                return Err(failure(
                    "service AttributeValue contains multiple type members",
                ));
            }
        }
    }
    if variants == 0 {
        return Err(failure("service AttributeValue has no valid type member"));
    }
    for (key, value) in dict.iter() {
        if value.is_none() && variants > 0 {
            continue;
        }
        match key.extract::<String>()?.as_str() {
            "S" | "N" => {
                value.cast::<PyString>().map_err(failure)?;
            }
            "BOOL" | "NULL" => {
                value.cast::<PyBool>().map_err(failure)?;
            }
            "SS" | "NS" => {
                for entry in value.cast::<PyList>().map_err(failure)?.iter() {
                    entry.cast::<PyString>().map_err(failure)?;
                }
            }
            "B" => dict.set_item(key, decode_base64(&value)?)?,
            "BS" => {
                let result = PyList::empty(value.py());
                for entry in value.cast::<PyList>().map_err(failure)?.iter() {
                    result.append(decode_base64(&entry)?)?;
                }
                dict.set_item(key, result)?;
            }
            "M" => decode_item(&value, depth + 1)?,
            "L" => {
                for entry in value.cast::<PyList>().map_err(failure)?.iter() {
                    decode_attribute(&entry, depth + 1)?;
                }
            }
            _ => {}
        }
    }
    Ok(())
}

fn decode_base64<'py>(v: &Bound<'py, PyAny>) -> PyResult<Bound<'py, PyBytes>> {
    let encoded = v.cast::<PyString>().map_err(failure)?;
    let encoded = encoded.to_str()?;
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(encoded)
        .map_err(failure)?;
    Ok(PyBytes::new(v.py(), &bytes))
}
fn decode_item(value: &Bound<'_, PyAny>, depth: usize) -> PyResult<()> {
    for (_, value) in value.cast::<PyDict>().map_err(failure)?.iter() {
        decode_attribute(&value, depth + 1)?;
    }
    Ok(())
}

pub fn service_error(
    py: Python<'_>,
    operation: &str,
    metadata: &OperationMetadata,
) -> PyResult<PyErr> {
    service_error_with_protocol_failure(py, operation, metadata, None)
}

pub(crate) fn service_error_with_protocol_failure(
    py: Python<'_>,
    operation: &str,
    metadata: &OperationMetadata,
    protocol_failure: Option<String>,
) -> PyResult<PyErr> {
    let attempt = final_response(metadata)?;
    // HTTP service errors may contain empty, non-JSON or non-object proxy
    // bodies. Their status/header code and wire context still identify them.
    let decoded = decoded_body(attempt).map_err(failure)?;
    // Botocore identifies generic server/proxy failures before UTF-8 parsing.
    // Python bytes.strip() includes vertical tab as well as ASCII whitespace.
    let start = decoded
        .iter()
        .position(|byte| !matches!(byte, b' ' | b'\t'..=b'\r'))
        .unwrap_or(decoded.len());
    let generic = attempt.status >= 500
        && (start == decoded.len() || decoded[start..].starts_with(b"<html>"));
    let (result, body_message) = if generic {
        (PyDict::new(py), None)
    } else {
        match serde_json::from_slice::<&RawValue>(&decoded) {
            Ok(body) if body.get().starts_with('{') => {
                // A real SDK parsing failure must not acquire a service class
                // merely because its captured body is valid JSON.
                if let Some(message) = protocol_failure {
                    return Ok(failure(message));
                }
                let (result, message) = json_to_python(py, body.get().as_bytes())?;
                (result.cast_into::<PyDict>()?, message)
            }
            Err(_) if !decoded.is_empty() => {
                let result = PyDict::new(py);
                result.set_item("message", std::str::from_utf8(&decoded).map_err(failure)?)?;
                (result, None)
            }
            _ => (PyDict::new(py), None),
        }
    };
    let header_code = attempt
        .headers
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case("x-amzn-errortype"))
        .and_then(|(_, value)| std::str::from_utf8(value).ok());
    let body_code = if generic {
        None
    } else {
        result
            .get_item("__type")?
            .and_then(|value| value.extract::<String>().ok())
            .or(result
                .get_item("code")?
                .and_then(|value| value.extract::<String>().ok()))
    };
    let code = body_code
        .as_deref()
        .or(header_code.filter(|_| !generic))
        .map(|v| {
            let code = v.split(':').next().unwrap_or(v);
            code.rsplit('#').next().unwrap_or(code)
        })
        .map(str::to_owned)
        .unwrap_or_else(|| attempt.status.to_string());
    if result.contains("__type")? {
        result.del_item("__type")?;
    }
    // Retain unrelated fields in a dictionary payload; canonical identity wins.
    let error = result
        .get_item("Error")?
        .and_then(|value| value.cast_into::<PyDict>().ok())
        .unwrap_or_else(|| PyDict::new(py));
    error.set_item("Code", &code)?;
    error.set_item(
        "Message",
        if generic {
            http::StatusCode::from_u16(attempt.status)
                .ok()
                .and_then(|status| status.canonical_reason())
                .unwrap_or("")
                .into_pyobject(py)?
                .into_any()
        } else if let Some(message) = body_message {
            // Preserve the separate owned projection of the canonical message,
            // including container-valued messages on unknown service errors.
            json_to_python(py, message)?.0
        } else {
            match result.get_item("message")?.or(result.get_item("Message")?) {
                Some(message) => message,
                None => "".into_pyobject(py)?.into_any(),
            }
        },
    )?;
    result.set_item("Error", error)?;
    if code == "ConditionalCheckFailedException" {
        if let Some(item) = result.get_item("Item")?.filter(|item| !item.is_none()) {
            decode_item(&item, 0)?;
        }
    }
    if code == "TransactionCanceledException" {
        if let Some(reasons) = result
            .get_item("CancellationReasons")?
            .filter(|reasons| !reasons.is_none())
        {
            for reason in reasons.cast::<PyList>().map_err(failure)?.iter() {
                if let Some(item) = reason
                    .cast::<PyDict>()
                    .map_err(failure)?
                    .get_item("Item")?
                    .filter(|item| !item.is_none())
                {
                    decode_item(&item, 0)?;
                }
            }
        }
    }
    let response_metadata = metadata_to_python(py, metadata)?;
    if let Some(payload) = result
        .get_item("ResponseMetadata")?
        .and_then(|value| value.cast_into::<PyDict>().ok())
    {
        for (key, value) in payload.iter() {
            // Reserve optional genuine fields even when absent. HTTPHeaders
            // always comes from the transport and is never merged with the body.
            if !(key.eq("HTTPStatusCode")?
                || key.eq("HTTPHeaders")?
                || key.eq("RequestId")?
                || key.eq("RetryAttempts")?
                || key.eq("MaxAttemptsReached")?)
            {
                response_metadata.set_item(key, value)?;
            }
        }
    }
    result.set_item("ResponseMetadata", response_metadata)?;
    let error = ServiceError::new_err(format!("{operation}: {code}"));
    let value = error.value(py);
    value.setattr("response", result)?;
    value.setattr("operation_name", operation)?;
    value.setattr("body", PyBytes::new(py, &attempt.body))?;
    // Keep captured names (transport-normalized lowercase), byte-exact values
    // and duplicate entries in service context.
    value.setattr(
        "headers",
        attempt
            .headers
            .iter()
            .map(|(k, v)| (k.clone(), PyBytes::new(py, v)))
            .collect::<Vec<_>>(),
    )?;
    Ok(error)
}
