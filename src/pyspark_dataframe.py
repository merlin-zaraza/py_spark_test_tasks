"""
Package for running direct sql on files
"""
from pyspark.sql import functions as f

import pyspark_sql as t
from pyspark_sql import TaskDf, \
    fn_get_tasks_range, fn_clean_up_data_folder, fn_run_tasks_by_definition_list, \
    fn_create_df_from_csv_file

l_df_accounts = fn_create_df_from_csv_file(in_file_name=t.ACCOUNTS)
l_df_transactions = fn_create_df_from_csv_file(in_file_name=t.TRANSACTIONS)
l_df_country_abbreviation = fn_create_df_from_csv_file(in_file_name=t.COUNTRY_ABBREVIATION)


def fn_get_task1_def_list():
    """
    Task 1 Data Frames List
    """

    l_df_account_types_count = l_df_transactions \
        .groupBy(f.col("account_type")) \
        .agg(f.count("account_type").alias("cnt"))

    l_df_account_balance = l_df_transactions \
        .groupBy(f.col("id")) \
        .agg(f.sum("amount").alias("balance"), f.max("transaction_date").alias("latest_date"))

    return [
        TaskDf("1.1_account_types_count", l_df_account_types_count),
        TaskDf("1.2_account_balance", l_df_account_balance),
    ]


def fn_get_task2_def_list():
    """
    Task 2 Data Frames List
    """

    def fn_inner_join_acc_names_to_df(in_dataframe: t.DataFrame) -> t.DataFrame:
        """
        Inner join of "first_name", "last_name" by id
        """

        return in_dataframe.join(
            f.broadcast(l_df_accounts.select("id", "first_name", "last_name")),
            "id",
            "inner")

    l_df_accounts_btw_18_30 = l_df_accounts.selectExpr(
        "id",
        "first_name",
        "last_name",
        "age",
        "country"
    ).where("age between 18 and 30")

    l_df_accounts_non_pro = l_df_transactions.selectExpr(
        "id",
        "account_type"
    ).where(f.col("account_type") != 'Professional') \
        .groupBy("id") \
        .agg(f.count("id").alias("cnt"))

    l_df_accounts_non_pro_with_user_info = fn_inner_join_acc_names_to_df(l_df_accounts_non_pro)

    l_df_accounts_top5 = l_df_accounts \
        .groupBy("first_name") \
        .agg(f.count("first_name").alias("cnt")) \
        .orderBy(f.col("cnt").desc()) \
        .limit(5)

    l_df_total_expenses = l_df_transactions.selectExpr(
        "id",
        " case when amount < 0 then amount else 0 end as expenses",
        " case when amount > 0 then amount else 0 end as earnings "
    ).groupBy("id") \
        .agg(f.sum("expenses").alias("expenses"),
             f.sum("earnings").alias("earnings"))

    l_df_total_expenses_with_user_info = fn_inner_join_acc_names_to_df(l_df_total_expenses)

    l_df_total_expenses_pivot = l_df_transactions.selectExpr(
        "id",
        "amount",
        "int(substring(transaction_date, 1, 4)) as tr_year",
    ).groupBy("id") \
        .pivot("tr_year") \
        .agg(f.sum("amount").alias("amount")) \
        .fillna(value=0)

    return [
        TaskDf("2.1_accounts_btw_18_30", l_df_accounts_btw_18_30),
        TaskDf("2.2_accounts_non_pro", l_df_accounts_non_pro_with_user_info),
        TaskDf("2.3_accounts_top_5", l_df_accounts_top5),
        TaskDf("2.4_total_per_year", l_df_total_expenses_with_user_info),
        TaskDf("2.5_total_expenses_pivot", l_df_total_expenses_pivot),
    ]


def fn_get_task3_def_list():
    """
    Task 3 Data Frames List
    """

    l_df_first_last_concatenated = l_df_accounts.selectExpr(
        "Concat(first_name,' ',last_name) as first_last_concat"
    ).where("age between 18 and 30")

    l_df_avg_transaction_amount_2021_per_client = l_df_transactions.selectExpr(
        "id",
        "amount"
    ).where("transaction_date like '2021%'") \
        .groupBy("id") \
        .agg(f.round(f.avg("amount"),
                     2).alias("avg_amount"))

    l_df_account_types_count = l_df_transactions \
        .groupBy(f.col("account_type")) \
        .agg(f.count("account_type").alias("cnt"))

    l_df_top_10_positive = l_df_transactions.where("amount > 0") \
        .groupBy("id") \
        .agg(f.round(f.sum("amount"),
                     2).alias("total_amount")) \
        .orderBy(f.col("total_amount").desc()) \
        .limit(10)

    l_df_clients_sorted_by_first_name_descending = l_df_accounts \
        .select("first_name", "last_name") \
        .orderBy(f.col("first_name").desc())

    return [
        TaskDf("3.1_first_last_concatenated", l_df_first_last_concatenated),
        TaskDf("3.2_avg_transaction_amount_2021_per_client", l_df_avg_transaction_amount_2021_per_client),
        TaskDf("3.3_account_types_count", l_df_account_types_count),
        TaskDf("3.4_top_10_positive", l_df_top_10_positive),
        TaskDf("3.5_clients_sorted_by_first_name_descending", l_df_clients_sorted_by_first_name_descending),
    ]


def fn_get_richest_person_broadcast():
    """
    DF for richest person using broadcast
    """

    l_richest_person_transactions = l_df_transactions.select(
        "id",
        "amount",
    ).groupBy("id") \
        .agg(f.sum("amount").alias("total_amount")) \
        .orderBy(f.col("total_amount").desc()) \
        .limit(1) \
        .first()

    l_id = l_richest_person_transactions['id']
    l_total_amount = l_richest_person_transactions['total_amount']

    l_df_l_richest_person_account = l_df_accounts.select(
        f.col("id"),
        f.col("first_name"),
        f.col("last_name"),
        f.col("country").alias("abbreviation"),
        f.lit(l_total_amount).alias("total_amount"),
    ).where(f"id = '{l_id}'")

    l_df_richest_person_account_all_info = l_df_country_abbreviation.join(
        f.broadcast(l_df_l_richest_person_account),
        "abbreviation",
        "inner"
    ).drop("country", "abbreviation", "id")

    return l_df_richest_person_account_all_info


def fn_get_invalid_accounts():
    """
    DF for invalid accounts using broadcast
    """

    l_df_tr_filtered = l_df_transactions \
        .where(" account_type = 'Professional' ") \
        .drop("country")

    l_df_acc_filtered = l_df_accounts.where(" age < 26 ") \
        .withColumnRenamed("id", "account_id")

    l_df_tr_invalid_acc = l_df_tr_filtered.join(f.broadcast(l_df_acc_filtered),
                                                l_df_acc_filtered.account_id == l_df_tr_filtered.id,
                                                "inner")

    l_df_tr_invalid_acc.show()

    return l_df_tr_invalid_acc


def fn_get_all_info_broadcast():
    """
    DF for all data in one plase using broadcast
    """

    l_df_trans_info = l_df_transactions \
        .select(
        "id",
        "amount",
        "account_type") \
        .groupBy("id", "account_type") \
        .agg(f.sum("amount").alias("total_amount"))
    # f.concat_ws(",", f.collect_list("account_type")).alias("account_types")

    l_df_trans_and_acc_info = l_df_trans_info \
        .join(f.broadcast(l_df_accounts),
              "id",
              "inner") \
        .withColumnRenamed("country", "abbreviation")

    l_df_all_info = l_df_trans_and_acc_info \
        .join(f.broadcast(l_df_country_abbreviation),
              "abbreviation",
              "inner").drop("abbreviation")

    return l_df_all_info


def fn_get_task4_def_list():
    """
    Task 4 Data Frames List
    """

    return [
        TaskDf("4.1_person_with_biggest_balance", fn_get_richest_person_broadcast()),
        TaskDf("4.2_invalid_accounts", fn_get_invalid_accounts()),
        TaskDf("4.3_single_dataset", fn_get_all_info_broadcast()),
    ]


def fn_run_dataframe_task(in_task_group_id: int):
    """
    Function to execute all DF from task group list
    and put them to the /opt/spark-data/df folder
    """

    l_range = fn_get_tasks_range(in_task_group_id)

    fn_clean_up_data_folder(in_task_group_id=in_task_group_id,
                            in_task_type=t.TASK_TYPE_DF)

    for l_one_task_group in l_range:

        if l_one_task_group == 1:
            l_one_task_definition_list = fn_get_task1_def_list()
        elif l_one_task_group == 2:
            l_one_task_definition_list = fn_get_task2_def_list()
        elif l_one_task_group == 3:
            l_one_task_definition_list = fn_get_task3_def_list()
        elif l_one_task_group == 4:
            l_one_task_definition_list = fn_get_task4_def_list()
        else:
            raise ValueError(f"Invalid Value for l_one_task_group : {l_one_task_group}")

        fn_run_tasks_by_definition_list(in_task_group_id=l_one_task_group,
                                        in_task_definition_list=l_one_task_definition_list)


if __name__ == "__main__":
    l_args = t.fn_init_argparse()
    l_args = l_args.parse_args()
    l_group_id = l_args.group_id

    fn_run_dataframe_task(in_task_group_id=l_group_id)
