import pytest
import pyspark_dataframe as psd


@pytest.mark.spark
@pytest.mark.parametrize("in_task_group_id,in_task_id",
                         [
                             pytest.param(1, 3, marks=pytest.mark.xfail),
                             pytest.param(10, 3, marks=pytest.mark.xfail)
                         ])
def test_task_group_invalid_parameters(in_task_group_id, in_task_id):
    psd.fn_run_dataframe_task(in_task_group_id=in_task_group_id,
                              in_task_id=in_task_id)


@pytest.mark.spark
@pytest.mark.parametrize("in_task_group_id,in_task_id",
                         [
                              (1, 1), (1, 2),
                              (2, 1), (2, 2), (2, 3), (2, 4), (2, 5),
                              (3, 1), (3, 2), (3, 3), (3, 4),
                             # (3, 5),
                         ]
                         )
def test_task_data(in_task_group_id, in_task_id):
    psd.fn_run_test_task(in_task_group_id=in_task_group_id,
                         in_task_id=in_task_id,
                         in_task_type=psd.TASK_TYPE_DF)
