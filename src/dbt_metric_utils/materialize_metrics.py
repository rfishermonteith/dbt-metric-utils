import re
import ast
import yaml
from typing import Dict, List, Tuple, Optional

from dbt.cli.main import dbtRunner, dbtRunnerResult
from dbt_metricflow.cli.cli_context import CLIContext
from metricflow.engine.metricflow_engine import MetricFlowQueryRequest

# Constant for the regex pattern to extract materialize calls.
MATERIALIZE_CALL_PATTERN = r"\{\{(\s*dbt_metric_utils_materialize\(.*?\)\s*)\}\}"


def render_metric(metricflow_engine, metric_name: str, metric_kwargs: dict) -> Tuple[str, str]:
    """
    Renders a metric by delegating to the `render_dbt_metric_utils_materialize` function.

    Args:
        metricflow_engine: The MetricFlow engine instance.
        metric_name: The unique identifier for the metric.
        metric_kwargs: A dictionary of keyword arguments for the materialization.

    Returns:
        A tuple containing:
          - var_key: A string key representing the metric configuration.
          - var_val: The rendered SQL query for the metric.
    """
    var_key, var_val = render_dbt_metric_utils_materialize(
        mf=metricflow_engine, node_id=metric_name, **metric_kwargs
    )
    return var_key, var_val


def render_dbt_metric_utils_materialize(
    mf,
    node_id: str,
    metrics: List[str],
    dimensions: Optional[List[str]] = None,
    group_by: Optional[List[str]] = None,
    limit: Optional[int] = None,
    time_start: Optional[str] = None,
    time_end: Optional[str] = None,
    where: Optional[str] = None,
    order_by: Optional[List[str]] = None,
) -> Tuple[str, str]:
    """
    Creates a MetricFlow query request, renders the SQL query, and constructs a variable key.

    Args:
        mf: The MetricFlow engine instance.
        node_id: The identifier for the node/model.
        metrics: List of metric names.
        dimensions: Optional list of dimension names.
        group_by: Optional list of columns to group by.
        limit: Optional row limit for the query.
        time_start: Optional start time constraint.
        time_end: Optional end time constraint.
        where: Optional where clause for filtering.
        order_by: Optional list of columns to order by.

    Returns:
        A tuple containing:
          - var_key: A string that uniquely identifies the metric configuration.
          - var_val: The rendered SQL query.
    """
    # Create the MetricFlow query request.
    mf_request = MetricFlowQueryRequest.create_with_random_request_id(
        metric_names=metrics,
        group_by_names=group_by,
        limit=limit,
        time_constraint_start=time_start,
        time_constraint_end=time_end,
        where_constraint=where,
        order_by_names=order_by,
    )

    # Render the SQL query using the MetricFlow engine.
    mf_query = mf.explain(mf_request).rendered_sql_without_descriptions.sql_query

    # Construct the variable key.
    # If a parameter is None, an empty string is used in its place.
    var_key = (
        f"metrics={metrics},"
        f"dimensions={dimensions or ''},"
        f"group_by={group_by or ''},"
        f"limit={limit or ''},"
        f"time_start={time_start or ''},"
        f"time_end={time_end or ''},"
        f"where={where or ''},"
        f"order_by={order_by or ''}"
    )
    var_val = mf_query

    return var_key, var_val


def _parse_kwargs(func_str: str) -> dict:
    """
    Parses a string representing a function call and returns its keyword arguments as a dictionary.

    The function expects an input like:
      "f(a='blah', b=5, c=['a', 'c'])"

    Args:
        func_str: The string containing the function call.

    Returns:
        A dictionary containing the keyword arguments and their evaluated literal values.

    Raises:
        ValueError: If the provided string is not a valid function call.
    """
    try:
        tree = ast.parse(func_str, mode='eval')
    except SyntaxError as e:
        raise ValueError("Invalid function call syntax") from e

    if not isinstance(tree.body, ast.Call):
        raise ValueError("The provided string does not appear to be a function call.")

    kwargs = {}
    for kw in tree.body.keywords:
        key = kw.arg
        # Safely evaluate the literal expression.
        kwargs[key] = ast.literal_eval(kw.value)
    return kwargs


def _write_metric_queries(dbt_target: Optional[str] = None) -> Tuple[dict, dict]:
    """
    Extracts materialize calls from dbt nodes, renders metric queries using MetricFlow,
    and updates the dependency graph in the dbt manifest.

    Args:
        dbt_target: Optional target for the dbt invocation. If None, uses the default target.

    Returns:
        A tuple containing:
          - manifest: The updated dbt manifest with dependency graph modifications.
          - new_dbt_vars: A dictionary mapping metric configuration keys to their rendered SQL queries.
    """
    dbt = dbtRunner()
    # Invoke the dbt parse command.
    res: dbtRunnerResult = dbt.invoke(
        ["parse", "--quiet"]
        if dbt_target is None
        else ["parse", "--target", dbt_target, "--quiet"]
    )
    manifest = res.result
    mf = CLIContext().mf

    # Extract raw materialize calls from dbt nodes.
    materialize_calls_raw_sql = [
        (node.unique_id, node.raw_code)
        for key, node in manifest.nodes.items()
        if "model." in key
    ]
    materialize_calls = []
    for node_id, raw_code in materialize_calls_raw_sql:
        # Find all materialize calls in the raw code using the regex pattern.
        matches = re.findall(MATERIALIZE_CALL_PATTERN, raw_code, re.DOTALL)
        for match in matches:
            # Remove extra spaces and newlines.
            cleaned_call = match.replace(" ", "").replace("\n", "")
            materialize_calls.append((node_id, cleaned_call))

    new_dbt_vars = {}
    materialized_metric_dependencies = {}
    for node_id, materialize_call_str in materialize_calls:
        kwargs = _parse_kwargs(materialize_call_str)
        var_key, var_val = render_metric(mf, node_id, kwargs)
        new_dbt_vars[var_key] = var_val

        # Update the dependency mapping.
        metrics = kwargs.get("metrics", [])
        if node_id in materialized_metric_dependencies:
            materialized_metric_dependencies[node_id] = list(
                set(materialized_metric_dependencies[node_id]).union(set(metrics))
            )
        else:
            materialized_metric_dependencies[node_id] = metrics

    # Build a mapping from metric names to fully qualified node ids.
    metric_name_to_fqn = {metric.name: metric.unique_id for metric in manifest.metrics.values()}

    # Update the dependency graph so that dbt executes queries in the correct order.
    for node_id, metrics in materialized_metric_dependencies.items():
        metric_fqns = [metric_name_to_fqn[m] for m in metrics if m in metric_name_to_fqn]
        manifest.nodes[node_id].depends_on.nodes.extend(metric_fqns)

    return manifest, new_dbt_vars


def get_metric_queries_as_dbt_vars(dbt_target: Optional[str] = None) -> Tuple[dict, str]:
    """
    Retrieves metric queries as dbt variables.

    Args:
        dbt_target: Optional target for the dbt invocation. If None, uses the default target.

    Returns:
        A tuple containing:
          - manifest: The updated dbt manifest.
          - yaml_vars: A YAML-formatted string of the new dbt variables mapping metric configurations
                       to their rendered SQL queries.
    """
    manifest, new_dbt_vars = _write_metric_queries(dbt_target)
    yaml_vars = yaml.dump(new_dbt_vars, default_flow_style=False)
    return manifest, yaml_vars
