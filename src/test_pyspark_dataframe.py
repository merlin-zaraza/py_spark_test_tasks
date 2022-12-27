"""
Package for running direct sql on files
"""
from pyspark.sql import functions as f

import test_pyspark_sql as t
from test_pyspark_sql import TaskDf, \
    fn_get_tasks_range, fn_clean_up_data_folder, fn_run_tasks_by_definition_list, \
    fn_create_df_from_csv_file

l_accounts_df = fn_create_df_from_csv_file(in_file_name=t.ACCOUNTS)
l_transactions_df = fn_create_df_from_csv_file(in_file_name=t.TRANSACTIONS)
l_country_abbreviation_df = fn_create_df_from_csv_file(in_file_name=t.COUNTRY_ABBREVIATION)


def fn_get_task1_def_list():
    """
    Task 1 Data Frames List
    """

    l_df_account_types_count_1_1 = l_transactions_df \
        .groupBy(f.col("account_type")) \
        .agg(f.count("account_type").alias("cnt"))

    l_df_account_types_count_1_2 = l_transactions_df \
        .groupBy(f.col("id")) \
        .agg(f.sum("amount").alias("balance"), f.max("transaction_date").alias("latest_date"))

    return [
        TaskDf("1.1_account_types_count", l_df_account_types_count_1_1),
        TaskDf("1.2_account_balance", l_df_account_types_count_1_2),
    ]


def fn_get_task2_def_list():
    """
    Task 2 Data Frames List
    """

    accounts_btw_18_30 = l_accounts_df.selectExpr(
        "id",
        "first_name",
        "last_name",
        "age",
        "country"
    ).where("age between 18 and 30")

    accounts_non_pro = l_transactions_df.selectExpr(
        "id",
        "account_type"
    ).where(f.col("account_type") != 'Professional') \
        .groupBy("id") \
        .agg(f.count("id").alias("cnt"))

    accounts_top5 = l_accounts_df \
        .groupBy("first_name") \
        .agg(f.count("first_name").alias("cnt")) \
        .orderBy(f.col("cnt").desc()) \
        .limit(5)

    total_expenses = l_transactions_df.selectExpr(
        "id",
        " case when amount < 0 then amount else 0 end as expenses",
        " case when amount > 0 then amount else 0 end as earnings "
    ).groupBy("id") \
        .agg(f.sum("expenses").alias("expenses"),
             f.sum("earnings").alias("earnings"))

    total_expenses_pivot = l_transactions_df.selectExpr(
        "id",
        "amount",
        " case when amount < 0 then 'expenses' else 'earnings' end as expenses_type",
    ).groupBy("id") \
        .pivot("expenses_type") \
        .agg(f.sum("amount").alias("amount"))

    return [
        TaskDf("2.1_accounts_btw_18_30", accounts_btw_18_30),
        TaskDf("2.2_accounts_non_pro", accounts_non_pro),
        TaskDf("2.3_accounts_top_5", accounts_top5),
        TaskDf("2.4_total_expenses", total_expenses),
        TaskDf("2.5_total_expenses_pivot", total_expenses_pivot),
    ]


def fn_run_dataframe_task(in_task_group_id: int):
    """
    Function to execute all DF from task group list
    and put them to the /opt/spark-data/df folder
    """

    l_task_params_dict = {1: fn_get_task1_def_list(),
                          2: fn_get_task2_def_list()}

    l_range = fn_get_tasks_range(in_task_group_id)

    fn_clean_up_data_folder(in_task_group_id=in_task_group_id,
                            in_task_type=t.TAKS_TYPE_DF)

    for l_one_task_group in l_range:
        l_one_task_definition_list = l_task_params_dict.get(l_one_task_group)
        fn_run_tasks_by_definition_list(in_task_group_id=l_range,
                                        in_task_definition_list=l_one_task_definition_list)


if __name__ == "__main__":
    l_args = t.fn_init_argparse()
    l_args = l_args.parse_args()
    l_group_id = l_args.group_id

    fn_run_dataframe_task(in_task_group_id=l_group_id)
