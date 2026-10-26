// Copyright ScyllaDB, Inc.
// SPDX-License-Identifier: Apache-2.0

pub mod client;
pub mod config;
pub mod metadata;
pub mod registry;
pub mod runtime;
pub mod transport;

pub const INTERFACE_VERSION: u32 = 1;
pub const PYTHON_VERSION: &str = env!("ALTERNATOR_PYTHON_VERSION");

#[cfg(feature = "extension")]
mod python;
#[cfg(feature = "extension")]
mod python_client;
