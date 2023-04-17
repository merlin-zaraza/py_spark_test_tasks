"""
Package for execution and validation of the test tasks
"""
import pathlib
import argparse
import os
import re
import shutil

from typing import List, Dict, Tuple
from collections import namedtuple

from pyspark.sql import SparkSession, DataFrame, DataFrameWriter
from project_logs import add_logging, DefaultLogger

Task = namedtuple("Task", "group_id task_id")
_APP_NAME = "py_spark_task_validator"

STR_TRUE: str = "true"

SPARK_MASTER: str = os.getenv("SPARK_MASTER", "spark://spark-master:7077")
FOLDER_DATA: str = os.environ.get("SPARK_DATA", "/opt/spark-data")
FOLDER_APPS: str = os.environ.get("SPARK_APPS", "/opt/spark-apps/main")
FOLDER_APPS_RESOURCES: str = FOLDER_APPS + "/resources"
FOLDER_TEST: str = os.environ.get("SPARK_TEST", "/opt/spark-apps/test")
FOLDER_LOG: str = os.environ.get("SPARK_LOG", "/opt/spark-log")
FOLDER_TABLES: str = f"{FOLDER_DATA}/tables"
SPARK_SESSION: SparkSession = None
ROUND_DIGITS: int = 2

CURRENT_LOGGER = DefaultLogger(in_loger_name=_APP_NAME,
                               in_log_folder=FOLDER_LOG,
                               in_file=__file__)

FILE_TYPE_CSV: str = "csv"
FILE_TYPE_PARQUET: str = "parquet"

ACCOUNTS = 'accounts'
TRANSACTIONS = 'transactions'
COUNTRY_ABBREVIATION = 'country_abbreviation'

LIST_OF_ALL_INPUT_TABLES = [ACCOUNTS, TRANSACTIONS, COUNTRY_ABBREVIATION]
DICT_OF_INIT_DATAFRAMES: Dict[str, DataFrame] = {}

TASK_TYPE_SQL = "sql"
TASK_TYPE_DF = "df"
TASK_TYPES_LIST = [TASK_TYPE_DF, TASK_TYPE_SQL]

_DF_PARTITIONS_COUNT: int = 20
_SEPARATOR: str = ";"


def fn_get_sql_task_folder_path(in_task_group_id: int) -> str:
    """
    Path to the SQLs
    :param in_task_group_id:
    :return: path to the folder with SQLs
    """
    return f"{FOLDER_APPS_RESOURCES}/sql/task{in_task_group_id}"


class TaskDef:
    """
    Class for task dataframe definition
    """

    sql: str

    _sql_ext: str = ".sql"
    _sql_name: str
    _sql_name_no_ext: str

    @property
    def sql_name_no_ext(self):
        """
        sql name without extension, required for target output folder
        """
        return self._sql_name_no_ext

    @property
    def sql_name(self):
        """
        Sql file name with .sql
        """
        return self._sql_name

    @sql_name.setter
    def sql_name(self, val: str):
        """
        Setter for sql_name, set _sql_name_no_ext at the same time
        """
        self._sql_name = val
        self._sql_name_no_ext = val.replace(self._sql_ext, "")

    @property
    def sql_path(self):
        """
        Getter for sql file path
        """
        return self._sql_path

    @sql_path.setter
    def sql_path(self, val):
        """
        Setter for sql file path
        """
        if not val:
            self.sql = ""
            self.sql_name = ""
            self._sql_path = ""
        else:
            if self._sql_ext not in val:
                val += self._sql_ext

            self._sql_path = val
            self.sql_name = os.path.basename(val)

            with open(f"{val}", "r", encoding="UTF-8") as file:
                self.sql = file.read()

    def __str__(self):
        l_str = ""

        for l_line in ["**************************",
                       f" task_group_id : {self.test_task.group_id} ",
                       f" task_id : {self.test_task.task_id} ",
                       "**************************",
                       f" data_frame : {self.data_frame} ",
                       f" sql_path : {self.sql_path} ",
                       f" sql_name : {self.sql_name} ",
                       f" sql : {self.sql} ",
                       "**************************"]:
            l_str += l_line + "\n"

        return l_str

    def __init__(self, in_data_frame: DataFrame,
                 in_sql_path: str = None,
                 in_test_task: Task = None):

        self.data_frame = in_data_frame
        self.test_task = in_test_task
        self.sql_path = in_sql_path


@add_logging(in_default_logger=CURRENT_LOGGER)
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


@add_logging(in_default_logger=CURRENT_LOGGER)
def fn_create_df_from_parquet(in_file_name: str = "*",
                              in_view_name: str = None,
                              in_repartition: int = _DF_PARTITIONS_COUNT,
                              in_folder_path: str = FOLDER_TABLES,
                              in_sub_folder: str = None,
                              in_cache_df : bool = False):
    """
    Function for registering file in SQL engine as temp view
    """

    if in_sub_folder:
        l_file_name = f"{in_sub_folder}/{in_file_name}"
    else:
        l_file_name = in_file_name

    l_df = SPARK_SESSION.read \
        .parquet(f"file://{in_folder_path}/{l_file_name}.{FILE_TYPE_PARQUET}") \
        .repartition(in_repartition)

    if in_cache_df:
        l_df.cache()

    l_view_name = fn_get_default_view_name(in_file_name, in_view_name if in_view_name else in_sub_folder)
    l_df.createTempView(l_view_name)

    return l_df


@add_logging(in_default_logger=CURRENT_LOGGER)
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
        .repartition(in_repartition)

    l_view_name = fn_get_default_view_name(in_file_name, in_view_name)
    l_df.createTempView(l_view_name)

    return l_df


@add_logging(in_default_logger=CURRENT_LOGGER)
def fn_init_tables(*args):
    """
    Function for tables initialisation, register existing csv files as SQL views
    """
    if not args:
        args = LIST_OF_ALL_INPUT_TABLES

    for l_one_table in args:
        if l_one_table not in DICT_OF_INIT_DATAFRAMES:
            l_one_df = fn_create_df_from_parquet(in_sub_folder=l_one_table,
                                                 in_cache_df=True)
            DICT_OF_INIT_DATAFRAMES.setdefault(l_one_table, l_one_df)

    return DICT_OF_INIT_DATAFRAMES


@add_logging(in_default_logger=CURRENT_LOGGER)
def fn_get_task_target_folder(in_task_type: str,
                              in_task_group_id: int = None,
                              in_tgt_folder: str = None):
    """
    Gives path to target folder (for task or group)
    """
    l_task_group_folder = f"{FOLDER_DATA}/{in_task_type}"

    if in_task_group_id:
        l_task_group_folder += f"/task{in_task_group_id}"

    if in_tgt_folder is None:
        l_target_folder = f"{l_task_group_folder}"
    else:
        l_target_folder = f"{l_task_group_folder}/{in_tgt_folder}"

    return l_target_folder


@add_logging(in_default_logger=CURRENT_LOGGER)
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

    def fn_add_csv_options(in_df: DataFrameWriter) -> DataFrameWriter:
        return in_df \
            .option("header", STR_TRUE) \
            .option("inferSchema", STR_TRUE) \
            .option("sep", _SEPARATOR)

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
            fn_add_csv_options(l_df.repartition(in_repartition_tgt).write.mode(in_mode)) \
                .csv(l_path)
    else:
        if in_output_file_type == FILE_TYPE_PARQUET:
            l_df.write.mode(in_mode) \
                .parquet(l_path)
        else:
            fn_add_csv_options(l_df.write.mode(in_mode)) \
                .csv(l_path)

    # l_df.show()


@add_logging(in_default_logger=CURRENT_LOGGER)
def fn_run_tasks_by_definition_list(in_task_group_id: int,
                                    in_task_definition_list: List[TaskDef],
                                    in_task_type: str,
                                    in_repartition_tgt: int = 1):
    """
    Function to execute DF functions based on DF list
    """

    for l_one_task_df in in_task_definition_list:

        if in_task_type == TASK_TYPE_DF:
            fn_run_task_df(in_task_group_id=in_task_group_id,
                           in_tgt_folder=l_one_task_df.sql_name_no_ext,
                           in_data_frame=l_one_task_df.data_frame,
                           in_repartition_tgt=in_repartition_tgt)
        elif in_task_type == TASK_TYPE_SQL:
            fn_run_task_sql(in_task_group_id=in_task_group_id,
                            in_tgt_folder=l_one_task_df.sql_name_no_ext,
                            in_sql=l_one_task_df.sql,
                            in_repartition_tgt=in_repartition_tgt)


@add_logging(in_default_logger=CURRENT_LOGGER)
def fn_run_task_sql(in_tgt_folder: str,
                    in_sql: str,
                    in_task_group_id: int = 1,
                    in_repartition_tgt: int = None):
    """
    Function to run 1 SQL
    """

    CURRENT_LOGGER.info(f"Executing sql : \n"
                        f"{in_sql}")

    fn_run_task(in_tgt_folder=in_tgt_folder,
                in_sql=in_sql,
                in_task_group_id=in_task_group_id,
                in_repartition_tgt=in_repartition_tgt)


@add_logging(in_default_logger=CURRENT_LOGGER)
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


@add_logging(in_default_logger=CURRENT_LOGGER)
def fn_get_task_group_range(in_task_group_id: int = None,
                            in_dict_all_group_tasks: Dict = None, ):
    """
    Function to get list of tasks to run
    By default (in_task_group_id is none) it is all tasks : 1,2,3,4
    Otherwise only one task
    """
    if in_dict_all_group_tasks is None:
        l_default_range = range(1, 5)
    else:
        l_default_range = range(1, len(in_dict_all_group_tasks) + 1)

    if in_task_group_id is None:
        l_range = l_default_range
    else:
        if in_task_group_id not in l_default_range:
            raise ValueError(f"in_task_group_id is out of range : {in_task_group_id} not in {l_default_range}")

        l_range = range(in_task_group_id, in_task_group_id + 1)

    return l_range


@add_logging(in_default_logger=CURRENT_LOGGER, in_major_step=True)
def fn_get_or_create_spark_session():
    """
    Function to init spark session
    """
    global SPARK_SESSION
    SPARK_SESSION = SparkSession.builder  \
        .master(SPARK_MASTER) \
        .appName(_APP_NAME).getOrCreate()

    fn_init_tables()

    return SPARK_SESSION


@add_logging(in_default_logger=CURRENT_LOGGER)
def fn_close_session():
    """
    Function close session and invalidate init tables list
    """
    global DICT_OF_INIT_DATAFRAMES

    SPARK_SESSION.stop()
    DICT_OF_INIT_DATAFRAMES = {}


@add_logging(in_default_logger=CURRENT_LOGGER)
def fn_clean_up_data_folder(in_task_type: str,
                            in_task_group_id: int = None):
    """
    Function to clean up data folder before task execution
    """

    l_group_path = fn_get_task_target_folder(in_task_group_id=in_task_group_id,
                                             in_task_type=in_task_type)

    CURRENT_LOGGER.info(f'l_group_path : {l_group_path}')

    if os.path.exists(l_group_path):
        l_dir = pathlib.Path(l_group_path)

        # iterating the directory
        for l_item in l_dir.iterdir():
            # checking if it's a file
            if l_item.is_dir():
                CURRENT_LOGGER.info(f'fn_clean_up_data_folder: Deleting {l_item}')
                shutil.rmtree(l_item, onerror=FileNotFoundError)


@add_logging(in_default_logger=CURRENT_LOGGER)
def fn_clean_up_all_folders():
    """
    Function for clean up target folders after getting sql/dataframe results
    :return:
    """
    for l_one_tt in TASK_TYPES_LIST:
        fn_clean_up_data_folder(in_task_type=l_one_tt)


@add_logging(in_default_logger=CURRENT_LOGGER, in_major_step=True)
def fn_run_task_group_sql(in_task_group_id: int):
    """
    Function to run SQLs in task folder
    """

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


def fn_get_one_task_definition(in_task_group_id: int,
                               in_task_id: int,
                               in_dict_all_group_tasks: Dict[int, List[TaskDef]]) -> TaskDef:
    """
    Get task definition by id (starting from 1 not 0)
    :param in_task_group_id:
    :param in_task_id:
    :param in_dict_all_group_tasks dictionary with list of tasks definition
    :return Task definition class by task group and task id:
    """
    return in_dict_all_group_tasks[in_task_group_id][in_task_id - 1]


@add_logging(in_default_logger=CURRENT_LOGGER, in_major_step=True)
def fn_run_task_type(in_dict_all_group_tasks: Dict[int, List[TaskDef]],
                     in_task_group_id: int = None,
                     in_task_id: int = None,
                     in_task_type: str = TASK_TYPE_DF):
    """
    Function to execute all DF from task group list
    and put them to the /opt/spark-data/df folder
    """

    l_range = fn_get_task_group_range(in_task_group_id, in_dict_all_group_tasks)

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

        fn_run_tasks_by_definition_list(in_task_group_id=l_one_task_group,
                                        in_task_definition_list=l_one_task_definition_list,
                                        in_task_type=in_task_type)


@add_logging(in_default_logger=CURRENT_LOGGER, in_major_step=True)
def fn_run_test_task(in_task_group_id: int,
                     in_task_id: int,
                     in_dict_all_group_tasks: Dict[int, List[TaskDef]],
                     in_task_type: str = TASK_TYPE_DF,
                     in_test_task_filter: Dict[Tuple[int, int], str] = None):
    """Function for test execution, compares input and output
    files In case of difference raise error and shows rows with diff values
    :param in_task_group_id:
    :param in_task_id:
    :param in_dict_all_group_tasks: Dict[int, List[TaskDef]],
    :param in_task_type:
    :param in_test_task_filter dictionary with filters
    :return: None

    """

    def fn_validate_col_list(in_df: DataFrame):
        l_validated_cols_list = []

        # valid column name should start from letter and consists from numbers, letters and underscore
        # from the beginning to the end
        l_valid_column_name_reg_exp = '^[a-z]+([0-9_a-z]+)?$'

        for l_one_col in in_df.columns:
            if re.match(l_valid_column_name_reg_exp, l_one_col, flags=re.IGNORECASE):
                l_valid_col = l_one_col
            else:
                l_valid_col = f"`{l_one_col}`"

            l_validated_cols_list.append(l_valid_col)

        return l_validated_cols_list

    fn_run_task_type(in_task_group_id=in_task_group_id,
                     in_task_id=in_task_id,
                     in_task_type=in_task_type,
                     in_dict_all_group_tasks=in_dict_all_group_tasks)

    l_task_def = fn_get_one_task_definition(in_task_group_id=in_task_group_id,
                                            in_task_id=in_task_id,
                                            in_dict_all_group_tasks=in_dict_all_group_tasks)

    l_folder_name = l_task_def.sql_name_no_ext
    l_src_filter = STR_TRUE

    if in_test_task_filter:
        l_key = Task(in_task_group_id, in_task_id)
        in_test_task_filter.setdefault(l_key, STR_TRUE)
        l_src_filter = in_test_task_filter[l_key]

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

    try:
        fn_get_or_create_spark_session()
        fn_run_task_group_sql(in_task_group_id=l_group_id)
    finally:
        fn_close_session()
