import pytest
import pyspark_dataframe as psd


@pytest.mark.spark
@pytest.mark.parametrize("in_task_group_id,in_task_id",
                         [(1, 1), pytest.param(1, 3, marks=pytest.mark.xfail)])
def test_task_group(in_task_group_id, in_task_id):
    psd.fn_run_dataframe_task(in_task_group_id=in_task_group_id,
                              in_task_id=in_task_id)
