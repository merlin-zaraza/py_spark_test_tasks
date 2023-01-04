"""
Package for running direct sql on files
"""
import sys
from typing import Dict, List
from pyspark.sql import functions as f, DataFrame

import pyspark_sql as t
from pyspark_sql import TaskDf

DICT_CORE_DF: Dict = {}


def fn_get_task1_def_list():
    """
    Task 1 Data Frames List
    """

    l_df_account_types_count = DF_TRANSACTIONS \
        .groupBy("account_type") \
        .agg(f.countDistinct("id").alias("cnt"))

    l_df_account_balance = DF_TRANSACTIONS \
        .groupBy(f.col("id")) \
        .agg(f.round(f.sum("amount"), t.ROUND_DIGITS).alias("balance"),
             f.max("transaction_date").alias("latest_date"))

    l_task_group_id = 1

    return [
        TaskDf(l_task_group_id, "1.1_account_types_count", l_df_account_types_count),
        TaskDf(l_task_group_id, "1.2_account_balance", l_df_account_balance, "id <= 20 "),
    ]


def fn_inner_join_acc_names_to_df(in_dataframe: t.DataFrame) -> t.DataFrame:
    """
    Inner join of "first_name", "last_name" by id
    """

    return in_dataframe.join(
        f.broadcast(DF_ACCOUNTS.select("id", "first_name", "last_name")),
        "id",
        "inner")


def fn_get_task2_def_list():
    """
    Task 2 Data Frames List
    """

    l_df_accounts_btw_18_30 = DF_ACCOUNTS.selectExpr(
        "id",
        "first_name",
        "last_name",
        "age",
        "country"
    ).where("age between 18 and 30")

    l_df_accounts_non_pro = DF_TRANSACTIONS.selectExpr(
        "id",
        "account_type"
    ).where(f.col("account_type") != 'Professional') \
        .groupBy("id") \
        .agg(f.count("id").alias("cnt"))

    l_l_df_accounts_non_pro_with_user_info = fn_inner_join_acc_names_to_df(l_df_accounts_non_pro)

    l_df_accounts_top5 = DF_ACCOUNTS \
        .groupBy("first_name") \
        .agg(f.count("first_name").alias("cnt")) \
        .orderBy(f.col("cnt").desc()) \
        .limit(5)

    l_df_total_expenses = DF_TRANSACTIONS.selectExpr(
        "id",
        " case when amount < 0 then amount else 0 end as expenses",
        " case when amount > 0 then amount else 0 end as earnings "
    ).groupBy("id") \
        .agg(f.round(f.abs(f.sum("expenses")), t.ROUND_DIGITS).alias("expenses"),
             f.round(f.sum("earnings"), t.ROUND_DIGITS).alias("earnings"))

    l_df_total_expenses_with_user_info = fn_inner_join_acc_names_to_df(l_df_total_expenses)

    l_df_total_expenses_pivot = DF_TRANSACTIONS.selectExpr(
        "id",
        "case when amount > 0 then amount else 0 end as earnings",
        "int(substring(transaction_date, 1, 4)) as tr_year",
    ).groupBy("id") \
        .pivot("tr_year") \
        .agg(f.round(f.sum("earnings").alias("earnings"), t.ROUND_DIGITS)) \
        .fillna(value=0)

    l_task_group_id = 2

    return [
        TaskDf(l_task_group_id, "2.1_accounts_btw_18_30", l_df_accounts_btw_18_30,
               "id in (1,5,6,8,19,30,33,34,35,36,38,42,44,52,55,57,64,72,74,76)"),
        TaskDf(l_task_group_id, "2.2_accounts_non_pro", l_l_df_accounts_non_pro_with_user_info, "id <= 20"),
        TaskDf(l_task_group_id, "2.3_accounts_top_5", l_df_accounts_top5),
        TaskDf(l_task_group_id, "2.4_total_per_year", l_df_total_expenses_with_user_info,
               "id in (351901,64444,42093,456473,372636,457272,170685,153318,288955,452806,"
               "435985,248093,111744,392651,180469,204816,263364,230316,56785,109722)"),
        TaskDf(l_task_group_id, "2.5_total_earnings_pivot", l_df_total_expenses_pivot, "id <= 20"),
    ]


def fn_get_task3_def_list():
    """
    Task 3 Data Frames List
    """

    l_df_first_last_concatenated = DF_ACCOUNTS \
        .selectExpr("concat(first_name,  ' ', last_name) as first_last_concat") \
        .where("age between 18 and 30").distinct()

    l_df_avg_transaction_amount_2021_per_client = DF_TRANSACTIONS.selectExpr(
        "id",
        "amount"
    ).where("transaction_date like '2021%'") \
        .groupBy("id") \
        .agg(f.round(f.avg("amount"), t.ROUND_DIGITS).alias("avg_amount"))

    l_df_account_types_count = DF_TRANSACTIONS \
        .groupBy(f.col("account_type")) \
        .agg(f.countDistinct("id").alias("cnt"))

    l_df_top_10_positive = DF_TRANSACTIONS.where("amount > 0") \
        .groupBy("id") \
        .agg(f.round(f.sum("amount"), t.ROUND_DIGITS).alias("total_amount")) \
        .orderBy(f.col("total_amount").desc()) \
        .limit(10)

    l_df_clients_sorted_by_first_name_descending = DF_ACCOUNTS \
        .select("first_name", "last_name").distinct() \
        .orderBy(f.col("first_name").desc())

    l_task_group_id = 3

    return [
        TaskDf(l_task_group_id, "3.1_first_last_concatenated", l_df_first_last_concatenated, """
           first_last_concat in ('Darcy Phillips','Amelia Wright','Haris Ellis',
           'Tony Hall','Rubie Stewart','Miley Perry','Marcus Carter','Charlie Harris','Honey Rogers','Luke Harris',
           'Spike Murphy','Vincent Adams','James Barnes','George Bailey','Sienna Holmes','Isabella Elliott',
           'Freddie Martin','Kate Wright','Albert Myers','Connie Wells')
         """),
        TaskDf(l_task_group_id, "3.2_avg_transaction_amount_2021_per_client",
               l_df_avg_transaction_amount_2021_per_client,
               "id in ( 1,2,4,6,7,11,12,13,15,17,19,22,23,24,27,28,30,31,32,33 )"),
        TaskDf(l_task_group_id, "3.3_account_types_count", l_df_account_types_count),
        TaskDf(l_task_group_id, "3.4_top_10_positive", l_df_top_10_positive),
        TaskDf(l_task_group_id, "3.5_clients_sorted_by_first_name_descending",
               l_df_clients_sorted_by_first_name_descending,
               "first_name in ('Wilson') and "
               "last_name  in ('Mitchell','Anderson','Cameron','Gray','Barnes',"
               "'Williams','Stewart','Elliott','Cole',"
               "'Tucker','Stewart','Ferguson','Davis','Higgins','Perry','Riley',"
               "'Edwards','Richards','Myers','Johnson')"
               ),
    ]


def fn_get_richest_person_in_country_broadcast():
    """
    DF for richest person using broadcast
    """

    l_richest_person_transactions = DF_TRANSACTIONS.selectExpr(
        "id",
        "amount"
    ).groupBy("id") \
        .agg(
        f.round(f.sum("amount"), t.ROUND_DIGITS).alias("total_amount")
    )

    l_df_richest_person_account_info = l_richest_person_transactions.join(
        f.broadcast(DF_ACCOUNTS),
        "id",
        "inner"
    ).withColumn(colName="rn", col=f.expr("row_number() over (partition by country order by total_amount desc)")) \
        .selectExpr(
        "id",
        "first_name",
        "last_name",
        "country",
        "total_amount",
    ).where("rn  == 1 ")

    l_df_richest_person_all_info = l_df_richest_person_account_info.join(
        f.broadcast(DF_COUNTRY_ABBR),
        DF_COUNTRY_ABBR.abbreviation == l_df_richest_person_account_info.country,
        "inner"
    ).drop("abbreviation", "country", "id")

    return l_df_richest_person_all_info


def fn_get_invalid_accounts():
    """
    DF for invalid accounts using broadcast
    """

    l_df_tr_filtered = DF_TRANSACTIONS \
        .where(" account_type = 'Professional' ") \
        .drop("country")

    l_df_acc_filtered = DF_ACCOUNTS.where(" age < 26 ") \
        .withColumnRenamed("id", "account_id")

    l_df_tr_invalid_acc = l_df_tr_filtered.join(f.broadcast(l_df_acc_filtered),
                                                l_df_acc_filtered.account_id == l_df_tr_filtered.id,
                                                "inner")

    return l_df_tr_invalid_acc


def fn_get_all_info_broadcast():
    """
    DF for all data in one place using broadcast
    """

    l_df_trans_info = DF_TRANSACTIONS \
        .select("id",
                "amount",
                "account_type") \
        .groupBy("id", "account_type") \
        .agg(f.round(f.sum("amount"), t.ROUND_DIGITS).alias("total_amount"))
    # f.concat_ws(",", f.collect_list("account_type")).alias("account_types")

    l_df_trans_and_acc_info = l_df_trans_info \
        .join(f.broadcast(DF_ACCOUNTS),
              "id",
              "inner") \
        .withColumnRenamed("country", "abbreviation")

    l_df_all_info = l_df_trans_and_acc_info \
        .join(f.broadcast(DF_COUNTRY_ABBR),
              "abbreviation",
              "inner").drop("abbreviation")

    return l_df_all_info


def fn_get_task4_def_list():
    """
    Task 4 Data Frames List
    """
    l_task_group_id = 4

    return [
        TaskDf(l_task_group_id, "4.1_person_with_biggest_balance_in_country",
               fn_get_richest_person_in_country_broadcast(),
               "country_full_name in ('Bulgaria','Surinam','Mauritius','Chile','Ethiopia','Peru','Mali',"
               "'Malawi','Senegal','Spain','Cuba','Belgium','Yemen','Denmark','Belgium','Ecuador',"
               "'Honduras','Peru','El Salvador','China')"),
        TaskDf(l_task_group_id, "4.2_invalid_accounts", fn_get_invalid_accounts(),
               "account_type = 'Professional' and account_id in (7253) "),
        TaskDf(l_task_group_id, "4.3_single_dataset", fn_get_all_info_broadcast(),
               "id in (1,6,12,13,16,22,26)")
    ]


def fn_get_dict_with_all_tasks() -> Dict[int, List[TaskDf]]:
    """
    Returns Dictionary with all task groups inside
    """
    l_result = {}

    for l_one_task_id in t.fn_get_task_group_range():
        handler = getattr(sys.modules[__name__], f'fn_get_task{l_one_task_id}_def_list')
        l_result.setdefault(l_one_task_id, handler())

    return l_result


def fn_get_one_task_definition(in_task_group_id: int, in_task_id: int) -> TaskDf:
    """
    :param in_task_group_id:
    :param in_task_id:
    :return Task definition class by task group and task id:
    """
    l_one_task_index = in_task_id - 1
    return DICT_ALL_GROUP_TASKS[in_task_group_id][l_one_task_index]


def fn_run_task_type(in_task_group_id: int = None,
                     in_task_id: int = None,
                     in_task_type: str = t.TASK_TYPE_DF):
    """
    Function to execute all DF from task group list
    and put them to the /opt/spark-data/df folder
    """

    l_range = t.fn_get_task_group_range(in_task_group_id)

    if in_task_id is None:

        t.fn_clean_up_data_folder(in_task_group_id=in_task_group_id,
                                  in_task_type=in_task_type)

    else:
        l_tasks_count = len(DICT_ALL_GROUP_TASKS[in_task_group_id])

        l_list_default_task_ids = list(range(1, l_tasks_count + 1))

        if in_task_id not in l_list_default_task_ids:
            raise ValueError(f"in_task_id is not in {l_list_default_task_ids} for in_task_group_id={in_task_group_id}")

    for l_one_task_group in l_range:

        l_one_task_definition_list = []

        if in_task_id is None:
            l_one_task_definition_list = DICT_ALL_GROUP_TASKS[l_one_task_group]
        else:
            l_one_task_definition = fn_get_one_task_definition(in_task_group_id=in_task_group_id,
                                                               in_task_id=in_task_id)
            l_one_task_definition_list.append(l_one_task_definition)

        print(l_one_task_group, l_one_task_definition_list)

        t.fn_run_tasks_by_definition_list(in_task_group_id=l_one_task_group,
                                          in_task_definition_list=l_one_task_definition_list,
                                          in_task_type=in_task_type)


def fn_run_test_task(in_task_group_id: int,
                     in_task_id: int,
                     in_task_type: str = t.TASK_TYPE_DF):
    """
    Function for test execution, compares input and output files
    In case of difference raise error and shows rows with diff values.

    :param in_task_group_id:
    :param in_task_id:
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

    fn_run_task_type(in_task_group_id=in_task_group_id, in_task_id=in_task_id, in_task_type=in_task_type)

    l_task_def = fn_get_one_task_definition(in_task_group_id=in_task_group_id, in_task_id=in_task_id)

    l_folder_name = l_task_def.tgt_folder
    l_src_filter = l_task_def.test_filter_value

    l_folder_path = t.fn_get_task_target_folder(in_task_type=in_task_type,
                                                in_task_group_id=in_task_group_id,
                                                in_tgt_folder=l_folder_name)

    l_task_group_folder_name = f'task{in_task_group_id}'

    l_view_name = f"{l_task_group_folder_name}_{in_task_id}_{in_task_type}_"

    l_path = f"{t.FOLDER_TEST}/{l_task_group_folder_name}/expected_output"

    l_actual_view_name = l_view_name + "actual"
    l_expected_view_name = l_view_name + "expected"

    # ACT
    t.fn_create_df_from_csv_file(in_folder_path=l_folder_path,
                                 in_view_name=l_actual_view_name)
    # expect
    l_df_expect = t.fn_create_df_from_csv_file(in_file_name=l_folder_name,
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

    l_df_diff = t.SPARK_SESSION.sql(l_sql)
    l_diff_cnt = l_df_diff.count()

    if l_diff_cnt == 0:
        print(f'Test succeeded for {l_folder_name}')
    else:
        l_df_diff.orderBy(l_df_expect.columns).show()
        raise AssertionError(f"Test has failed for '{l_folder_name}'. Difference found")


def fn_df_to_parquet_file(in_df_dict: Dict[str, DataFrame]):
    for l_one_df_name, l_one_df in in_df_dict.items():
        t.fn_run_task(in_tgt_folder=l_one_df_name + "/*",
                      in_data_frame=l_one_df,
                      in_tgt_path=t.FOLDER_TABLES,
                      in_output_file_type=t.FILE_TYPE_PARQUET)


DF_ACCOUNTS: DataFrame = t.fn_create_df_from_parquet(in_sub_folder=t.ACCOUNTS)
DF_TRANSACTIONS: DataFrame = t.fn_create_df_from_parquet(in_sub_folder=t.TRANSACTIONS)
DF_COUNTRY_ABBR: DataFrame = t.fn_create_df_from_parquet(in_sub_folder=t.COUNTRY_ABBREVIATION)

DICT_CORE_DF = {t.ACCOUNTS: DF_ACCOUNTS,
                t.TRANSACTIONS: DF_TRANSACTIONS,
                t.COUNTRY_ABBREVIATION: DF_COUNTRY_ABBR}

DICT_ALL_GROUP_TASKS = fn_get_dict_with_all_tasks()

if __name__ == "__main__":
    l_args = t.fn_init_argparse(t.TASK_TYPE_DF)
    l_args = l_args.parse_args()
    l_group_id = l_args.group_id
    l_task_id = l_args.task_id
    l_task_type = l_args.task_type

    # fn_df_to_parquet_file(DICT_CORE_DF)

    fn_run_task_type(in_task_group_id=l_group_id,
                     in_task_id=l_task_id,
                     in_task_type=l_task_type)
