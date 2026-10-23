// Copyright ScyllaDB, Inc.
// SPDX-License-Identifier: Apache-2.0

fn main() {
    // The Python release version belongs to static wheel metadata, not Cargo.
    println!("cargo:rerun-if-changed=pyproject.toml");
    let metadata = std::fs::read_to_string("pyproject.toml").expect("read Python metadata");
    let project = metadata
        .split("[project]")
        .nth(1)
        .expect("[project] metadata");
    let version = project
        .lines()
        .take_while(|line| !line.starts_with('['))
        .find_map(|line| line.strip_prefix(r#"version = ""#))
        .and_then(|line| line.strip_suffix('"'))
        .expect("static quoted Python project version");
    println!("cargo:rustc-env=ALTERNATOR_PYTHON_VERSION={version}");
}
