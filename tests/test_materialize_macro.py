import pytest
from jinja2 import Environment, FileSystemLoader, select_autoescape
from typing import Optional

EMPTY_LOOKUP_KEY = (
    "metrics=,dimensions=,group_by=,limit=,time_start=,time_end=,where=,order_by="
)


def jinja_env(execute: bool = False, test_local_vars: Optional[dict] = None):
    var_mock_macro = """
    {%- macro var(name, default_value) -%}
        {{ test_local_vars.get(name, default_value) }}
    {%- endmacro -%}
    """

    env = Environment(
        loader=FileSystemLoader("src/dbt_metric_utils"), autoescape=select_autoescape()
    )

    env.globals["execute"] = execute
    env.globals["test_local_vars"] = test_local_vars or {}
    # Macro should be added to the environment after all vars have been set.
    # Otherwise vars will not be available in the macro.
    env.globals["var"] = env.from_string(var_mock_macro).module.var
    env.globals["log"] = env.from_string(
        "{%- macro log(msg) -%}{%- endmacro -%}"
    ).module.log

    return env.get_template("dbt_metric_utils_materialize.sql")


@pytest.fixture
def dbt_metric_utils_encode_materialize_lookup_key_fn():
    return jinja_env().module.dbt_metric_utils_encode_materialize_lookup_key


def dbt_metric_utils_materialize_fn(execute: bool, test_local_vars: Optional[dict] = None):
    return jinja_env(execute, test_local_vars).module.dbt_metric_utils_materialize


def test_encode_dbt_metric_utils_materialize_lookup_key__empty(
    dbt_metric_utils_encode_materialize_lookup_key_fn,
):
    assert dbt_metric_utils_encode_materialize_lookup_key_fn() == EMPTY_LOOKUP_KEY


def test_encode_dbt_metric_utils_materialize_lookup_key(
    dbt_metric_utils_encode_materialize_lookup_key_fn,
):
    expected = "metrics=['m1'],dimensions=['dim1'],group_by=gb,limit=l,time_start=ts,time_end=te,where=w,order_by=ob"

    assert (
        dbt_metric_utils_encode_materialize_lookup_key_fn(
            ["m1"], ["dim1"], "gb", "l", "ts", "te", "w", "ob"
        )
        == expected
    )


def test_macro_dbt_metric_utils_materialize__non_execute():
    assert dbt_metric_utils_materialize_fn(execute=False)() == EMPTY_LOOKUP_KEY


def test_macro_dbt_metric_utils_materialize(
    dbt_metric_utils_encode_materialize_lookup_key_fn,
):
    macro_args = [["m1"], ["dim1"], "gb", "l", "ts", "te", "w", "ob"]
    expected_query = "SELECT a FROM some_table"

    lookup_key = dbt_metric_utils_encode_materialize_lookup_key_fn(*macro_args)

    assert (
        dbt_metric_utils_materialize_fn(
            execute=True, test_local_vars={lookup_key: expected_query}
        )(*macro_args)
        == f"({expected_query})"
    )
