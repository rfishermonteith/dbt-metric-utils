from dbt_metric_utils.helpers import _extract_materialize_calls


def test_dimension_in_call():
    input_sql = """
    
    select * from {{ dbt_metric_utils_materialize(
      metrics=['metric']
      , where="{{Dimension('dim')}}=1"
    ) }}
    
    """

    expected_sql = [
        """dbt_metric_utils_materialize(
      metrics=['metric']
      , where="{{Dimension('dim')}}=1"
    )"""
    ]

    actual_sql = _extract_materialize_calls(input_sql)
    assert expected_sql == actual_sql


def test_multiple_calls():
    input_sql = """

    select * from {{ dbt_metric_utils_materialize(
      metrics=['metric_1']
    ) }}

    select * from {{ dbt_metric_utils_materialize(
      metrics=['metric_2']
    ) }}
    """

    expected_sql = [
        """dbt_metric_utils_materialize(
      metrics=['metric_1']
    )""",
        """dbt_metric_utils_materialize(
      metrics=['metric_2']
    )"""
    ]

    actual_sql = _extract_materialize_calls(input_sql)
    assert expected_sql == actual_sql
