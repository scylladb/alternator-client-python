# Copyright ScyllaDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Detect newly modeled operations/parameters and unclassified Config settings."""

from __future__ import annotations

import json
from copy import deepcopy
from dataclasses import fields
from pathlib import Path
from typing import Any

import botocore.session

from alternator import TLS, Config
from tests.contract.cases import operation_cases


def test_manifest_covers_complete_model_graph() -> None:
    manifest = json.loads(
        (Path(__file__).parents[1] / "contract/manifest.json").read_text()
    )
    model = botocore.session.get_session().get_service_model("dynamodb")
    seen: set[str] = set()

    def check_shape(shape: Any) -> None:  # noqa: ANN401 -- botocore's recursive Shape model
        if shape.name in seen:
            return
        seen.add(shape.name)
        expected = manifest["shapes"][shape.name]
        assert expected["type"] == shape.type_name
        for key in ("min", "max", "pattern", "enum", "sensitive"):
            assert expected.get(key) == shape.metadata.get(key)
        if shape.type_name == "structure":
            assert expected["members"] == {k: v.name for k, v in shape.members.items()}
            assert expected["required"] == shape.required_members
            for member in shape.members.values():
                check_shape(member)
        elif shape.type_name == "list":
            assert expected["member"] == shape.member.name
            check_shape(shape.member)
        elif shape.type_name == "map":
            assert expected["key"] == shape.key.name
            assert expected["value"] == shape.value.name
            check_shape(shape.key)
            check_shape(shape.value)

    cases = operation_cases()
    assert len(manifest["operations"]) == 10
    assert {c.operation for c in cases} == set(manifest["operations"])
    for name, operation in manifest["operations"].items():
        operation_model = model.operation_model(name)
        assert operation_model.input_shape is not None
        assert operation_model.output_shape is not None
        assert operation["input"] == operation_model.input_shape.name
        assert operation["output"] == operation_model.output_shape.name
        assert operation["errors"] == [s.name for s in operation_model.error_shapes]
        exercised = set().union(
            *(case.request.keys() for case in cases if case.operation == name)
        )
        assert exercised == set(operation_model.input_shape.members)
        check_shape(operation_model.input_shape)
        check_shape(operation_model.output_shape)
        for error in operation_model.error_shapes:
            check_shape(error)
    assert seen == set(manifest["shapes"])
    assert set(manifest["config"]) == {f.name for f in fields(Config)}
    assert set(manifest["tls"]) == {f.name for f in fields(TLS)}


def test_operation_cases_isolate_inputs_from_expected_responses() -> None:
    cases = operation_cases()
    original = deepcopy(cases)
    put = next(
        case for case in cases if case.method == "put_item" and case.name == "modern"
    )

    put.request["Item"]["blob"]["B"] = b"mutated-by-client"
    put.request["Item"]["nested"]["M"]["new"] = {"S": "mutated-by-client"}
    del put.request["Item"]["number"]

    for case, before in zip(cases, original, strict=True):
        assert case.response == before.response
        if case is not put:
            assert case.request == before.request
    assert operation_cases() == original
