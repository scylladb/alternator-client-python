// Copyright ScyllaDB, Inc.
// SPDX-License-Identifier: Apache-2.0

pub mod client;
pub mod config;
pub mod metadata;
pub mod registry;
pub mod runtime;
pub mod transport;
pub mod transport_failure;

pub const INTERFACE_VERSION: u32 = 1;
pub const PYTHON_VERSION: &str = env!("ALTERNATOR_PYTHON_VERSION");

#[cfg(feature = "python")]
mod python;
#[cfg(feature = "python")]
mod python_client;

#[cfg(feature = "python")]
pub mod conversion;
#[cfg(feature = "python")]
pub mod errors;
#[cfg(feature = "python")]
pub mod response;

#[cfg(feature = "python")]
mod operations;
