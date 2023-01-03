import pytest
import pyspark_dataframe as psd
from pyspark_sql import TASK_TYPES_LIST

l_task_types_param_tuple = "in_task_type", TASK_TYPES_LIST


@pytest.mark.spark
@pytest.mark.parametrize(*l_task_types_param_tuple)
@pytest.mark.parametrize("in_task_group_id,in_task_id",
                         [
                             pytest.param(1, 3, marks=pytest.mark.xfail),
                             pytest.param(10, 3, marks=pytest.mark.xfail)
                         ])
def test_task_group_invalid_parameters(in_task_group_id, in_task_id, in_task_type):
    """
    Testing invalid parameters input
    :param in_task_group_id:
    :param in_task_id:
    :param in_task_type:
    :return:
    """
    psd.fn_run_task_type(in_task_group_id=in_task_group_id,
                         in_task_id=in_task_id,
                         in_task_type=in_task_type)


@pytest.mark.spark
@pytest.mark.parametrize(*l_task_types_param_tuple)
@pytest.mark.parametrize("in_task_group_id,in_task_id",
                         [
                             (1, 1), (1, 2),
                             (2, 1), (2, 2), (2, 3), (2, 4),
                             (2, 5),
                             (3, 1), (3, 2), (3, 3), (3, 4), (3, 5),
                             (4, 1), (4, 2), (4, 3)
                         ]
                         )
def test_task_data(in_task_group_id, in_task_id, in_task_type):
    """
    Testing all tasks using SQL and dataframe code
    :param in_task_group_id:
    :param in_task_id:
    :param in_task_type:
    :return:
    """
    psd.fn_run_test_task(in_task_group_id=in_task_group_id,
                         in_task_id=in_task_id,
                         in_task_type=in_task_type)
