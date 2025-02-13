import re
from typing import Dict, List, Tuple

import yaml
from dbt.cli.main import dbtRunner, dbtRunnerResult
from dbt_metricflow.cli.cli_context import CLIContext
from metricflow.engine.metricflow_engine import MetricFlowQueryRequest

import ast


def render_metric(mf, metric_name, metric_kwargs):
    var_key, var_val = render_dbt_metric_utils_materialize(mf=mf, node_id=metric_name, **metric_kwargs)

    return var_key, var_val


def render_dbt_metric_utils_materialize(
        mf,
        node_id: str,
        metrics: List[str],
        dimensions: List[str] | None = None,
        group_by: List[str] | None = None,
        limit: int | None = None,
        time_start: str | None = None,
        time_end: str | None = None,
        where: str | None = None,
        order_by: List[str] | None = None,
):
    mf_request = MetricFlowQueryRequest.create_with_random_request_id(
        metric_names=metrics,
        group_by_names=group_by,
        limit=limit,
        time_constraint_start=time_start,
        time_constraint_end=time_end,
        where_constraint=where,
        order_by_names=order_by,
    )

    mf_query = mf.explain(mf_request).rendered_sql_without_descriptions.sql_query

    # Python renders no value as 'None' and Jinja as empty string.
    var_key = f"metrics={metrics},dimensions={dimensions or ''},group_by={group_by or ''}," \
              + f"limit={limit or ''},time_start={time_start or ''}," \
              + f"time_end={time_end or ''},where={where or ''},order_by={order_by or ''}"
    var_val = mf_query

    return var_key, var_val


def _parse_kwargs(func_str: str) -> dict:
    """
    Parses a string of a function call like "f(a='blah', b=5, c=['a', 'c'])"
    and returns a dictionary of keyword arguments.
    """
    # Wrap the argument string in a dummy function call.
    # This allows us to use the AST parser on a function call node.

    # Parse the dummy function call.
    tree = ast.parse(func_str, mode='eval')

    if not isinstance(tree.body, ast.Call):
        raise ValueError("The provided string does not appear to be a function call.")

    kwargs = {}
    for kw in tree.body.keywords:
        key = kw.arg
        # Use literal_eval to safely evaluate the expression.
        value = ast.literal_eval(kw.value)
        kwargs[key] = value

    return kwargs


def _write_metric_queries(dbt_target: str | None = None) -> dict:
    dbt = dbtRunner()
    res: dbtRunnerResult = dbt.invoke(
        ["parse", "--quiet"]
        if dbt_target is None
        else ["parse", "--target", dbt_target, "--quiet"]
    )
    manifest = res.result
    mf = CLIContext().mf

    materialize_calls_raw_sql = [
        (n.unique_id, n.raw_code) for k, n in manifest.nodes.items() if "model." in k
    ]
    materialize_calls = []
    for m in materialize_calls_raw_sql:
        if matches := re.findall(r"\{\{(\s*dbt_metric_utils_materialize\(.*?\)\s*)\}\}", m[1], re.DOTALL):
            for match in matches:
                materialize_calls.append((m[0], match.replace(" ", "").replace("\n", "")))

    new_dbt_vars = {}
    materialized_metric_dependencies = {}
    for node_id, mc in materialize_calls:
        kw = _parse_kwargs(mc)
        var_key, var_val = render_metric(mf, node_id, kw)

        new_dbt_vars[var_key] = var_val

        # Update the dependencies
        metrics = kw["metrics"]
        if node_id in materialized_metric_dependencies:
            materialized_metric_dependencies[node_id] = list(
                set(materialized_metric_dependencies[node_id]).union(set(metrics)))
        else:
            materialized_metric_dependencies[node_id] = metrics

    metric_name_to_fqn = {m.name: m.unique_id for m in manifest.metrics.values()}

    # Fix dependency graph so that dbt knows which queries to run first
    for node_id, metrics in materialized_metric_dependencies.items():
        metric_fqns = [metric_name_to_fqn[m] for m in metrics]
        manifest.nodes[node_id].depends_on.nodes.extend(metric_fqns)

    return manifest, new_dbt_vars


def get_metric_queries_as_dbt_vars(dbt_target: str | None = None) -> Tuple[dict, str]:
    manifest, new_dbt_vars = _write_metric_queries(dbt_target)
    return manifest, yaml.dump(new_dbt_vars, default_flow_style=False)
