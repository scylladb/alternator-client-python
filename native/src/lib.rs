// Copyright ScyllaDB, Inc.
// SPDX-License-Identifier: Apache-2.0

pub mod runtime;

pub const INTERFACE_VERSION: u32 = 1;
pub const PYTHON_VERSION: &str = env!("ALTERNATOR_PYTHON_VERSION");

#[cfg(feature = "extension")]
mod python;
