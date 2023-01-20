"""
Module for tests of pyspark task using sql and dataframe API
"""
from typing import Dict

import pytest
import pyspark_task as t
import pyspark_task_validator as tv

from pyspark_task_validator import Task, TASK_TYPES_LIST

DICT_ALL_GROUP_TASKS = {}

l_test_task_types_tuple = "in_task_type", TASK_TYPES_LIST
l_dict_tasks_tuple = "in_task_group_id,in_task_id", [
    task for task in t.LIST_ALL_TASKS if task.group_id <= 4
    # (1, 1),
    # (2, 5)
]


@pytest.fixture(scope='session', autouse=True)
def fn_init_and_cleanup_test_session():
    """
    Fixture that allows to do init and clean up actions during test session
    :return:
    """
    # Will be executed before the first test

    tv.fn_clean_up_all_folders()

    # Init Dataframes with src data
    for k, v in t.fn_get_dict_with_all_tasks().items():
        DICT_ALL_GROUP_TASKS.setdefault(k, v)

    yield tv.SPARK_SESSION

    # Will be executed after last
    tv.fn_clean_up_all_folders()
    tv.fn_close_session()


def fn_get_test_task_filter_dict() -> Dict[Task, str]:
    """
    Returns dictionary with filter on task output
    It is required to reduce size of file for expected output
    :return:
    """
    l_dict_test_filter = {
        Task(1, 2): "id <= 20",
        Task(2, 1): "id in (1,5,6,8,19,30,33,34,35,36,38,42,44,52,55,57,64,72,74,76)",
        Task(2, 2): "id <= 20",
        Task(2, 4): """
            id in (
                351901,64444,42093,456473,372636,457272,170685,153318,288955,452806,
                435985,248093,111744,392651,180469,204816,263364,230316,56785,109722
            )
            """,
        Task(2, 5): "id <= 20",
        Task(3, 1): """
            first_last_concat in (
                'Darcy Phillips','Amelia Wright','Haris Ellis',
                'Tony Hall','Rubie Stewart','Miley Perry','Marcus Carter','Charlie Harris','Honey Rogers','Luke Harris',
                'Spike Murphy','Vincent Adams','James Barnes','George Bailey','Sienna Holmes','Isabella Elliott',
                'Freddie Martin','Kate Wright','Albert Myers','Connie Wells'
            )
         """,
        Task(3, 2): "id in ( 1,2,4,6,7,11,12,13,15,17,19,22,23,24,27,28,30,31,32,33 )",
        Task(3, 5): """
            first_name in ('Wilson') and 
            last_name  in (
                'Mitchell','Anderson','Cameron','Gray','Barnes',
                'Williams','Stewart','Elliott','Cole',
                'Tucker','Stewart','Ferguson','Davis','Higgins','Perry','Riley',
                'Edwards','Richards','Myers','Johnson'
            )
        """,
        Task(4, 1): """
            country_full_name in (
                'Bulgaria','Surinam','Mauritius','Chile','Ethiopia','Peru','Mali',
                'Malawi','Senegal','Spain','Cuba','Belgium','Yemen','Denmark','Belgium','Ecuador',
                'Honduras','Peru','El Salvador','China'
            )
            """,
        Task(4, 2): "account_type = 'Professional' and account_id in (7253) ",
        (4, 3): "id in (1,6,12,13,16,22,26) "}

    return l_dict_test_filter


DICT_TEST_TASK_FILTERS = fn_get_test_task_filter_dict()


@pytest.mark.spark
@pytest.mark.parametrize("in_task_group_id,in_task_id",
                         [
                             pytest.param(1, 3, marks=pytest.mark.xfail),
                             pytest.param(10, 3, marks=pytest.mark.xfail),
                             pytest.param(5, 1, marks=pytest.mark.xfail)
                         ])
@pytest.mark.parametrize(*l_test_task_types_tuple)
def test_task_group_invalid_parameters(in_task_group_id, in_task_id, in_task_type):
    """
    Testing invalid parameters input and test failure if difference found
    :param in_task_group_id:
    :param in_task_id:
    :param in_task_type:
    :return:
    """
    tv.fn_run_task_type(in_task_group_id=in_task_group_id,
                        in_task_id=in_task_id,
                        in_task_type=in_task_type,
                        in_dict_all_group_tasks=DICT_ALL_GROUP_TASKS)


@pytest.mark.spark
@pytest.mark.parametrize(*l_dict_tasks_tuple)
@pytest.mark.parametrize(*l_test_task_types_tuple)
def test_task_data(in_task_group_id, in_task_id, in_task_type):
    """
    Testing all tasks using SQL and dataframe code
    :param in_task_group_id:
    :param in_task_id:
    :param in_task_type:
    :return:
    """
    tv.fn_run_test_task(in_task_group_id=in_task_group_id,
                        in_task_id=in_task_id,
                        in_task_type=in_task_type,
                        in_dict_all_group_tasks=DICT_ALL_GROUP_TASKS,
                        in_test_task_filter=DICT_TEST_TASK_FILTERS)


@pytest.mark.spark
@pytest.mark.parametrize("in_task_group_id", [1])
def test_fn_run_task_group_sql(in_task_group_id):
    """
    Testing run of sqls from task folder for 1 sql to make sure that scripts works
    :param in_task_group_id:
    :return:
    """
    tv.fn_run_task_group_sql(in_task_group_id)

# # # # # # # # # # # # # # # # # # #
# alternative way of managing tests:
# # # # # # # # # # # # # # # # # # #
# class PySparkTestCase(pytest.unittest.TestCase):
#     """Set-up of global test SparkSession"""
#
#     @classmethod
#     def setUpClass(cls):
#         cls.spark = (SparkSession
#                      .builder
#                      .master("local[1]")
#                      .appName("PySpark unit test")
#                      .getOrCreate())
#
#     @classmethod
#     def tearDownClass(cls):
#         cls.spark.stop()
#
#
# def test_schema(df1: DataFrame, df2: DataFrame, check_nullable=True):
#     field_list = lambda fields: (fields.name, fields.dataType, fields.nullable)
#     fields1 = [*map(field_list, df1.schema.fields)]
#     fields2 = [*map(field_list, df2.schema.fields)]
#     if check_nullable:
#         res = set(fields1) == set(fields2)
#     else:
#         res = set([field[:-1] for field in fields1]) == set([field[:-1] for field in fields2])
#     return res
#
#
# def test_data(df1: DataFrame, df2: DataFrame):
#     data1 = df1.collect()
#     data2 = df2.collect()
#     return set(data1) == set(data2)
