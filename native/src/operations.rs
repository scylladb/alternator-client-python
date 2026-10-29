// Copyright ScyllaDB, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Execute owned inputs through the routed client and its complete-operation registry.
use pyo3::exceptions::{PyNotImplementedError, PyRuntimeError};
use pyo3::prelude::*;

use crate::{
    client::Client,
    conversion::{Input, Output},
    errors, response,
};

pub(crate) fn input(operation: &str, parameters: &Bound<'_, PyAny>) -> PyResult<Input> {
    match operation {
        "GetItem" | "PutItem" | "UpdateItem" | "DeleteItem" | "Query" | "Scan" => {
            Input::from_python(operation, parameters)
        }
        _ => Err(PyNotImplementedError::new_err(format!(
            "native operation {operation} is not implemented"
        ))),
    }
}

pub(crate) async fn execute(client: &Client, input: Input) -> PyResult<Py<PyAny>> {
    // Each arm retains its concrete SDK error until Python projection. No Python
    // values are borrowed across await; success and failure use the same capture.
    macro_rules! run {
        ($name:literal, $variant:ident, $request:expr) => {{
            let (result, metadata) = client
                .run($request)
                .await
                .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
            Python::attach(|py| match result {
                Ok(output) => {
                    Ok(
                        response::success(py, &Output::$variant(Box::new(output)), &metadata)?
                            .into_any()
                            .unbind(),
                    )
                }
                Err(error) => Err(errors::sdk_error(
                    py,
                    $name,
                    metadata.endpoint_url.as_deref().unwrap_or(""),
                    &error,
                    &metadata,
                )?),
            })
        }};
    }
    match input {
        Input::GetItem(input) => run!("GetItem", GetItem, |sdk| async move {
            sdk.get_item()
                .set_table_name(input.table_name)
                .set_key(input.key)
                .set_attributes_to_get(input.attributes_to_get)
                .set_consistent_read(input.consistent_read)
                .set_return_consumed_capacity(input.return_consumed_capacity)
                .set_projection_expression(input.projection_expression)
                .set_expression_attribute_names(input.expression_attribute_names)
                .send()
                .await
        }),
        Input::PutItem(input) => run!("PutItem", PutItem, |sdk| async move {
            sdk.put_item()
                .set_table_name(input.table_name)
                .set_item(input.item)
                .set_expected(input.expected)
                .set_return_values(input.return_values)
                .set_return_consumed_capacity(input.return_consumed_capacity)
                .set_return_item_collection_metrics(input.return_item_collection_metrics)
                .set_conditional_operator(input.conditional_operator)
                .set_condition_expression(input.condition_expression)
                .set_expression_attribute_names(input.expression_attribute_names)
                .set_expression_attribute_values(input.expression_attribute_values)
                .set_return_values_on_condition_check_failure(
                    input.return_values_on_condition_check_failure,
                )
                .send()
                .await
        }),
        Input::UpdateItem(input) => run!("UpdateItem", UpdateItem, |sdk| async move {
            sdk.update_item()
                .set_table_name(input.table_name)
                .set_key(input.key)
                .set_attribute_updates(input.attribute_updates)
                .set_expected(input.expected)
                .set_conditional_operator(input.conditional_operator)
                .set_return_values(input.return_values)
                .set_return_consumed_capacity(input.return_consumed_capacity)
                .set_return_item_collection_metrics(input.return_item_collection_metrics)
                .set_update_expression(input.update_expression)
                .set_condition_expression(input.condition_expression)
                .set_expression_attribute_names(input.expression_attribute_names)
                .set_expression_attribute_values(input.expression_attribute_values)
                .set_return_values_on_condition_check_failure(
                    input.return_values_on_condition_check_failure,
                )
                .send()
                .await
        }),
        Input::DeleteItem(input) => run!("DeleteItem", DeleteItem, |sdk| async move {
            sdk.delete_item()
                .set_table_name(input.table_name)
                .set_key(input.key)
                .set_expected(input.expected)
                .set_conditional_operator(input.conditional_operator)
                .set_return_values(input.return_values)
                .set_return_consumed_capacity(input.return_consumed_capacity)
                .set_return_item_collection_metrics(input.return_item_collection_metrics)
                .set_condition_expression(input.condition_expression)
                .set_expression_attribute_names(input.expression_attribute_names)
                .set_expression_attribute_values(input.expression_attribute_values)
                .set_return_values_on_condition_check_failure(
                    input.return_values_on_condition_check_failure,
                )
                .send()
                .await
        }),
        Input::Query(input) => run!("Query", Query, |sdk| async move {
            sdk.query()
                .set_table_name(input.table_name)
                .set_index_name(input.index_name)
                .set_select(input.select)
                .set_attributes_to_get(input.attributes_to_get)
                .set_limit(input.limit)
                .set_consistent_read(input.consistent_read)
                .set_key_conditions(input.key_conditions)
                .set_query_filter(input.query_filter)
                .set_conditional_operator(input.conditional_operator)
                .set_scan_index_forward(input.scan_index_forward)
                .set_exclusive_start_key(input.exclusive_start_key)
                .set_return_consumed_capacity(input.return_consumed_capacity)
                .set_projection_expression(input.projection_expression)
                .set_filter_expression(input.filter_expression)
                .set_key_condition_expression(input.key_condition_expression)
                .set_expression_attribute_names(input.expression_attribute_names)
                .set_expression_attribute_values(input.expression_attribute_values)
                .send()
                .await
        }),
        Input::Scan(input) => run!("Scan", Scan, |sdk| async move {
            sdk.scan()
                .set_table_name(input.table_name)
                .set_index_name(input.index_name)
                .set_attributes_to_get(input.attributes_to_get)
                .set_limit(input.limit)
                .set_select(input.select)
                .set_scan_filter(input.scan_filter)
                .set_conditional_operator(input.conditional_operator)
                .set_exclusive_start_key(input.exclusive_start_key)
                .set_return_consumed_capacity(input.return_consumed_capacity)
                .set_total_segments(input.total_segments)
                .set_segment(input.segment)
                .set_projection_expression(input.projection_expression)
                .set_filter_expression(input.filter_expression)
                .set_expression_attribute_names(input.expression_attribute_names)
                .set_expression_attribute_values(input.expression_attribute_values)
                .set_consistent_read(input.consistent_read)
                .send()
                .await
        }),
        _ => Err(PyNotImplementedError::new_err(
            "native operation is not implemented",
        )),
    }
}
