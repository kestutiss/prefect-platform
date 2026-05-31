"""Tests for all example flows.

Run with: pytest tests/ -v
No running server or Docker needed — conftest.py wires prefect_test_harness.
"""
import os

import pandas as pd
import pytest

from flows.examples.etl_example import ETLFlowInput, etl_example_flow
from flows.examples.hello_world import HelloFlowInput, hello_world_flow


class TestHelloWorldFlow:
    def test_default_parameters(self):
        result = hello_world_flow()
        assert "Hello, World!" in result
        assert "Hello, Prefect!" in result

    def test_custom_names_and_greeting(self):
        input_ = HelloFlowInput(names=["Alice", "Bob"], greeting="Hi")
        result = hello_world_flow(input_=input_)
        assert "Hi, Alice!" in result
        assert "Hi, Bob!" in result

    def test_single_name(self):
        input_ = HelloFlowInput(names=["Claude"])
        result = hello_world_flow(input_=input_)
        assert "Hello, Claude!" in result


class TestETLFlow:
    def test_record_count_matches_batch_size(self, tmp_path):
        input_ = ETLFlowInput(
            source="test",
            batch_size=10,
            output_path=str(tmp_path / "output.csv"),
        )
        count = etl_example_flow(input_=input_)
        assert count == 10

    def test_output_file_is_created(self, tmp_path):
        input_ = ETLFlowInput(
            batch_size=5,
            output_path=str(tmp_path / "output.csv"),
        )
        etl_example_flow(input_=input_)
        assert os.path.exists(input_.output_path)

    def test_output_has_expected_columns(self, tmp_path):
        input_ = ETLFlowInput(
            batch_size=5,
            output_path=str(tmp_path / "cols.csv"),
        )
        etl_example_flow(input_=input_)
        df = pd.read_csv(input_.output_path)
        for col in ("id", "value", "category", "value_normalized", "category_upper"):
            assert col in df.columns, f"Missing column: {col}"

    def test_large_batch(self, tmp_path):
        input_ = ETLFlowInput(
            batch_size=500,
            output_path=str(tmp_path / "large.csv"),
        )
        count = etl_example_flow(input_=input_)
        assert count == 500
