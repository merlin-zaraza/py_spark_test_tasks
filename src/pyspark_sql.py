"""
Package for running direct sql on files
"""
import pathlib
import argparse
import os
import shutil
from typing import List, Dict

from pyspark.sql import SparkSession, DataFrame

ACCOUNTS = 'accounts'
TRANSACTIONS = 'transactions'
COUNTRY_ABBREVIATION = 'country_abbreviation'

TASK_TYPE_SQL = "sql"
TASK_TYPE_DF = "df"
TASK_TYPES_LIST = [TASK_TYPE_DF, TASK_TYPE_SQL]

_APP_NAME = "py_spark_tasks"

STR_TRUE: str = "true"
FOLDER_DATA: str = os.environ.get("SPARK_DATA", "/opt/spark-data")
FOLDER_APPS: str = os.environ.get("SPARK_APPS", "/opt/spark-apps")
FOLDER_TEST: str = os.environ.get("SPARK_TEST", "/opt/spark-test")
FOLDER_TABLES: str = f"{FOLDER_DATA}/tables"
SPARK_SESSION: SparkSession = SparkSession.builder.appName(_APP_NAME).getOrCreate()
ROUND_DIGITS: int = 2

FILE_TYPE_CSV: str = "csv"
FILE_TYPE_PARQUET: str = "parquet"

_DF_PARTITIONS_COUNT: int = 20
_SEPARATOR: str = ";"


def fn_get_sql_task_folder_path(in_task_group_id: int) -> str:
    """
    Path to the SQLs
    :param in_task_group_id:
    :return: path to the folder with SQLs
    """
    return f"{FOLDER_APPS}/sql/task{in_task_group_id}"


class TaskDf:
    """
    Class for task dataframe definition
    """
    task_group_id: int
    tgt_folder: str
    data_frame: DataFrame
    test_filter_value: str = STR_TRUE
    sql: str = ""

    def __str__(self):
        l_nl = "\n"
        l_str = "**************************" + l_nl
        l_str += f" task_group_id : {self.task_group_id} " + l_nl
        l_str += f" tgt_folder : {self.tgt_folder} " + l_nl
        l_str += f" test_filter_value : {self.test_filter_value} " + l_nl
        l_str += "**************************" + l_nl
        l_str += f" data_frame : {self.data_frame} " + l_nl
        l_str += f" sql : {self.sql} " + l_nl
        l_str += "**************************" + l_nl

        return l_str

    def __init__(self, in_task_group_id, in_tgt_folder, in_data_frame, in_test_filter_value=STR_TRUE):
        self.task_group_id = in_task_group_id
        self.tgt_folder = in_tgt_folder
        self.data_frame = in_data_frame
        self.test_filter_value = in_test_filter_value

        l_path = fn_get_sql_task_folder_path(self.task_group_id)

        with open(f"{l_path}/{self.tgt_folder}.sql", "r", encoding="UTF-8") as file:
            self.sql = file.read()


def fn_init_argparse(in_def_task_type) -> argparse.ArgumentParser:
    """
    Argument parser
    """
    parser = argparse.ArgumentParser(
        usage="%(prog)s [Group ID]",
        description="Executing user defined SQLs groups"
    )
    parser.add_argument(
        "-g", "--group_id", default=None, type=int
    )

    parser.add_argument(
        "-t", "--task_id", default=None, type=int
    )

    parser.add_argument(
        "-tt", "--task_type", default=in_def_task_type, type=str
    )

    return parser


def fn_get_default_view_name(in_file_name, in_view_name):
    """
    Function for getting default view name
    """

    return in_view_name if in_view_name else in_file_name


def fn_create_df_from_parquet(in_file_name: str = "*",
                              in_view_name: str = None,
                              in_repartition: int = _DF_PARTITIONS_COUNT,
                              in_folder_path: str = FOLDER_TABLES,
                              in_sub_folder: str = None):
    """
    Function for registering file in SQL engine as temp view
    """

    if in_sub_folder:
        l_file_name = f"{in_sub_folder}/{in_file_name}"
    else:
        l_file_name = in_file_name

    l_df = SPARK_SESSION.read \
        .parquet(f"file://{in_folder_path}/{l_file_name}.{FILE_TYPE_PARQUET}") \
        .repartition(in_repartition) \
        .cache()

    l_view_name = fn_get_default_view_name(in_file_name, in_view_name if in_view_name else in_sub_folder)
    l_df.createTempView(l_view_name)

    return l_df


def fn_create_df_from_csv_file(in_file_name: str = "*",
                               in_separator: str = _SEPARATOR,
                               in_view_name: str = None,
                               in_repartition: int = _DF_PARTITIONS_COUNT,
                               in_folder_path: str = FOLDER_DATA):
    """
    Function for registering file in SQL engine as temp view
    """

    l_df = SPARK_SESSION.read \
        .option("header", STR_TRUE) \
        .option("inferSchema", STR_TRUE) \
        .option("sep", in_separator) \
        .csv(f"file://{in_folder_path}/{in_file_name}.{FILE_TYPE_CSV}") \
        .repartition(in_repartition) \
        .cache()

    l_view_name = fn_get_default_view_name(in_file_name, in_view_name)
    l_df.createTempView(l_view_name)

    return l_df


def fn_init_tables(*args):
    """
    Function for tables initialisation, register existing csv files as SQL views
    """

    for l_one_file in args:
        fn_create_df_from_csv_file(in_file_name=l_one_file)


def fn_get_task_target_folder(in_task_type: str,
                              in_task_group_id: int,
                              in_tgt_folder: str = None):
    """
    Gives path to target folder (for task or group)
    """
    l_task_group_folder = f"{FOLDER_DATA}/{in_task_type}/task{in_task_group_id}"

    if in_tgt_folder is None:
        l_target_folder = f"{l_task_group_folder}"
    else:
        l_target_folder = f"{l_task_group_folder}/{in_tgt_folder}"

    return l_target_folder


def fn_run_task(in_tgt_folder: str,
                in_data_frame: DataFrame = None,
                in_sql: str = None,
                in_output_file_type: str = FILE_TYPE_CSV,
                in_tgt_path: str = None,
                in_task_group_id: int = 1,
                in_repartition_tgt: int = None,
                in_mode: str = "overwrite"):
    """
    Function to write Data Frame Output to file using DF or Spark SQL
    """

    l_type = TASK_TYPE_DF
    l_df = in_data_frame

    if in_sql:
        l_type = TASK_TYPE_SQL
        l_df = SPARK_SESSION.sql(in_sql)
        l_df.explain(extended=False)

    if in_tgt_path is not None:
        l_path = f"{in_tgt_path}/{in_tgt_folder}"
    else:
        l_path = fn_get_task_target_folder(in_task_group_id=in_task_group_id,
                                           in_task_type=l_type,
                                           in_tgt_folder=in_tgt_folder)

    if (in_repartition_tgt is not None) and in_repartition_tgt > 0:
        if in_output_file_type == FILE_TYPE_PARQUET:
            l_df.repartition(in_repartition_tgt) \
                .write.mode(in_mode) \
                .parquet(l_path)
        else:
            l_df.repartition(in_repartition_tgt) \
                .write.mode(in_mode) \
                .option("header", STR_TRUE) \
                .option("inferSchema", STR_TRUE) \
                .option("sep", _SEPARATOR) \
                .csv(l_path)
    else:
        if in_output_file_type == FILE_TYPE_PARQUET:
            l_df.write.mode(in_mode) \
                .parquet(l_path)
        else:
            l_df.write.mode(in_mode) \
                .option("header", STR_TRUE) \
                .option("inferSchema", STR_TRUE) \
                .option("sep", _SEPARATOR) \
                .csv(l_path)

    # l_df.show()


def fn_run_tasks_by_definition_list(in_task_group_id: int,
                                    in_task_definition_list: List[TaskDf],
                                    in_task_type: str,
                                    in_repartition_tgt: int = 1):
    """
    Function to execute DF functions based on DF list
    """

    for l_one_task_df in in_task_definition_list:

        if in_task_type == TASK_TYPE_DF:
            fn_run_task_df(in_task_group_id=in_task_group_id,
                           in_tgt_folder=l_one_task_df.tgt_folder,
                           in_data_frame=l_one_task_df.data_frame,
                           in_repartition_tgt=in_repartition_tgt)
        elif in_task_type == TASK_TYPE_SQL:
            fn_run_task_sql(in_task_group_id=in_task_group_id,
                            in_tgt_folder=l_one_task_df.tgt_folder,
                            in_sql=l_one_task_df.sql,
                            in_repartition_tgt=in_repartition_tgt)


def fn_run_task_sql(in_tgt_folder: str,
                    in_sql: str,
                    in_task_group_id: int = 1,
                    in_repartition_tgt: int = None):
    """
    Function to run 1 SQL
    """

    fn_run_task(in_tgt_folder=in_tgt_folder,
                in_sql=in_sql,
                in_task_group_id=in_task_group_id,
                in_repartition_tgt=in_repartition_tgt)


def fn_run_task_df(in_tgt_folder: str,
                   in_data_frame: DataFrame,
                   in_task_group_id: int = 1,
                   in_repartition_tgt: int = None):
    """
    Function to explain and run one DF
    """
    fn_run_task(in_tgt_folder=in_tgt_folder,
                in_data_frame=in_data_frame,
                in_task_group_id=in_task_group_id,
                in_repartition_tgt=in_repartition_tgt)


def fn_get_task_group_range(in_task_group_id: int = None):
    """
    Function to get list of tasks to run
    By default (in_task_group_id is none) it is all tasks : 1,2,3,4
    Otherwise only one task
    """
    l_default_range = range(1, 5)

    if in_task_group_id is None:
        l_range = l_default_range
    else:
        if in_task_group_id not in l_default_range:
            raise ValueError(f"in_task_group_id is out of range : {in_task_group_id} not in {l_default_range}")

        l_range = range(in_task_group_id, in_task_group_id + 1)

    return l_range


def fn_init(in_init_all_tables: bool = False):
    """
    Function to init spark tables and spark session
    """

    global SPARK_SESSION
    SPARK_SESSION = SparkSession.builder.appName(_APP_NAME).getOrCreate()

    if in_init_all_tables:
        fn_init_tables(ACCOUNTS,
                       TRANSACTIONS,
                       COUNTRY_ABBREVIATION)


def fn_close_session():
    """
    Function to close spark session
    """
    SPARK_SESSION.stop()


def fn_clean_up_data_folder(in_task_group_id: int,
                            in_task_type: str):
    """
    Function to clean up data folder before task execution
    """

    l_group_path = fn_get_task_target_folder(in_task_group_id=in_task_group_id,
                                             in_task_type=in_task_type)

    shutil.rmtree(l_group_path, onerror=FileNotFoundError)


def fn_run_task_group_sql(in_task_group_id: int):
    """
    Function to run SQLs in task folder
    """

    fn_init(in_init_all_tables=True)

    l_range = fn_get_task_group_range(in_task_group_id)

    fn_clean_up_data_folder(in_task_group_id=in_task_group_id,
                            in_task_type=TASK_TYPE_SQL)

    for l_one_task_group_id in l_range:
        l_task_sql_folder = fn_get_sql_task_folder_path(l_one_task_group_id)

        # constructing the path object
        l_dir = pathlib.Path(l_task_sql_folder)

        # iterating the directory
        for l_items in l_dir.iterdir():
            # checking if it's a file
            if l_items.is_file():
                l_sql_file_name = l_items.name.replace(".sql", "")

                with l_items.open(mode="r") as l_sql_file:
                    l_sql_file_content = l_sql_file.read()

                    fn_run_task_sql(in_tgt_folder=l_sql_file_name,
                                    in_sql=l_sql_file_content,
                                    in_task_group_id=l_one_task_group_id,
                                    in_repartition_tgt=1)

    fn_close_session()


def fn_get_one_task_definition(in_task_group_id: int,
                               in_task_id: int,
                               in_dict_all_group_tasks: Dict[int, List[TaskDf]]) -> TaskDf:
    """
    Get task definition by id (starting from 1 not 0)
    :param in_task_group_id:
    :param in_task_id:
    :param in_dict_all_group_tasks dictionary with list of tasks definition
    :return Task definition class by task group and task id:
    """
    return in_dict_all_group_tasks[in_task_group_id][in_task_id - 1]


def fn_run_task_type(in_dict_all_group_tasks: Dict[int, List[TaskDf]],
                     in_task_group_id: int = None,
                     in_task_id: int = None,
                     in_task_type: str = TASK_TYPE_DF):
    """
    Function to execute all DF from task group list
    and put them to the /opt/spark-data/df folder
    """

    l_range = fn_get_task_group_range(in_task_group_id)

    if in_task_id is None:

        fn_clean_up_data_folder(in_task_group_id=in_task_group_id,
                                in_task_type=in_task_type)

    else:
        l_tasks_count = len(in_dict_all_group_tasks[in_task_group_id])

        l_list_default_task_ids = list(range(1, l_tasks_count + 1))

        if in_task_id not in l_list_default_task_ids:
            raise ValueError(f"in_task_id is not in {l_list_default_task_ids} for in_task_group_id={in_task_group_id}")

    for l_one_task_group in l_range:

        l_one_task_definition_list = []

        if in_task_id is None:
            l_one_task_definition_list = in_dict_all_group_tasks[l_one_task_group]
        else:
            l_one_task_definition = fn_get_one_task_definition(in_task_group_id=in_task_group_id,
                                                               in_task_id=in_task_id,
                                                               in_dict_all_group_tasks=in_dict_all_group_tasks)
            l_one_task_definition_list.append(l_one_task_definition)

        # print(l_one_task_group, l_one_task_definition_list)

        fn_run_tasks_by_definition_list(in_task_group_id=l_one_task_group,
                                        in_task_definition_list=l_one_task_definition_list,
                                        in_task_type=in_task_type)


def fn_run_test_task(in_task_group_id: int,
                     in_task_id: int,
                     in_dict_all_group_tasks: Dict[int, List[TaskDf]],
                     in_task_type: str = TASK_TYPE_DF):
    """Function for test execution, compares input and output
    files In case of difference raise error and shows rows with diff values
    :param in_task_group_id:
    :param in_task_id:
    :param in_dict_all_group_tasks: Dict[int, List[TaskDf]],
    :param in_task_type:
    :return: None

    """

    def fn_validate_col_list(in_df: DataFrame):
        l_validated_cols_list = []

        for l_one_col in in_df.columns:
            try:
                int(l_one_col)
                l_valid_col = f"`{l_one_col}`"
            except ValueError:
                l_valid_col = l_one_col

            l_validated_cols_list.append(l_valid_col)

        return l_validated_cols_list

    fn_run_task_type(in_task_group_id=in_task_group_id,
                     in_task_id=in_task_id,
                     in_task_type=in_task_type,
                     in_dict_all_group_tasks=in_dict_all_group_tasks)

    l_task_def = fn_get_one_task_definition(in_task_group_id=in_task_group_id,
                                            in_task_id=in_task_id,
                                            in_dict_all_group_tasks=in_dict_all_group_tasks)

    l_folder_name = l_task_def.tgt_folder
    l_src_filter = l_task_def.test_filter_value

    l_folder_path = fn_get_task_target_folder(in_task_type=in_task_type,
                                              in_task_group_id=in_task_group_id,
                                              in_tgt_folder=l_folder_name)

    l_task_group_folder_name = f'task{in_task_group_id}'

    l_view_name = f"{l_task_group_folder_name}_{in_task_id}_{in_task_type}_"

    l_path = f"{FOLDER_TEST}/{l_task_group_folder_name}/expected_output"

    l_actual_view_name = l_view_name + "actual"
    l_expected_view_name = l_view_name + "expected"

    # ACT
    fn_create_df_from_csv_file(in_folder_path=l_folder_path,
                               in_view_name=l_actual_view_name)
    # expect
    l_df_expect = fn_create_df_from_csv_file(in_file_name=l_folder_name,
                                             in_folder_path=l_path,
                                             in_view_name=l_expected_view_name)

    l_cols = ",".join(fn_validate_col_list(l_df_expect))

    l_sql = f"""
            SELECT 
                {l_cols},
                 sum(actual) as total_actual, 
                 sum(expected) as total_expected 
            FROM
            (
                Select {l_cols}, 1 as actual, 0 as expected from {l_actual_view_name} where {l_src_filter}   
                UNION ALL
                Select {l_cols}, 0 as actual, 1 as expected from {l_expected_view_name}
            )
            GROUP BY 
                {l_cols}
            HAVING
                sum(actual) != sum(expected)                        
        """

    print(f"Running sql : {l_sql}")

    l_df_diff = SPARK_SESSION.sql(l_sql)
    l_diff_cnt = l_df_diff.count()

    if l_diff_cnt == 0:
        print(f'Test succeeded for {l_folder_name}')
    else:
        l_df_diff.orderBy(l_df_expect.columns).show()
        raise AssertionError(f"Test has failed for '{l_folder_name}'. Difference found")


def fn_df_to_parquet_file(in_df_dict: Dict[str, DataFrame]):
    """
    Write dataframe as a parquet file
    :param in_df_dict:
    :return:
    """

    for l_one_df_name, l_one_df in in_df_dict.items():
        fn_run_task(in_tgt_folder=l_one_df_name + "/*",
                    in_data_frame=l_one_df,
                    in_tgt_path=FOLDER_TABLES,
                    in_output_file_type=FILE_TYPE_PARQUET)


if __name__ == "__main__":
    l_args = fn_init_argparse(TASK_TYPE_SQL)
    l_args = l_args.parse_args()
    l_group_id = l_args.group_id

    fn_run_task_group_sql(in_task_group_id=l_group_id)
