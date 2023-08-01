import argparse
import logging
import pyspark
import os.path
from datetime import datetime
from logging.handlers import TimedRotatingFileHandler
from pyspark.sql import SparkSession
from typing import List, Dict


def read_files(paths: List[str]):
    """
        Function loads csv files provided in parameter.
    :param paths: List of files to be loaded.
    :return: none
    """
    dataset_one = spark.read.option("header", "true").csv(paths[0])
    dataset_two = spark.read.option("header", "true").csv(paths[1])
    return dataset_one, dataset_two


def logger_config():
    """
        Function creates rotating log based on time and returns it.
        5 log files are kept, new file generated every hour.
    :param: none
    :return: logger
    """
    logger = logging.getLogger("Rotating Log")
    logger.setLevel(logging.INFO)
    handler = TimedRotatingFileHandler('CODAC.log', when="h", interval=1, backupCount=5)
    fileformatter = logging.Formatter('%(asctime)s %(levelname)-6s %(message)s')
    handler.setFormatter(fileformatter)
    logger.addHandler(handler)
    return logger


log = logger_config()


def file_check(paths: List[str]):
    """
         Function which checks if files provided as a parameter to program exist.
    :param paths: List of paths to files with input data.
    :return: Bool
    """
    output = True
    for file in paths:
        if os.path.isfile(file):
            log.info(f"File {file} exist.")
        else:
            log.error(f"File {file} does not exist.")
            output = False
    return output


def get_arguments() -> List:
    """
        Function uses argparse lib to get three parameters from user:
            1. path for dataset_one (clients info)
            2. path for dataset_two (financial details)
            3. List of countries for filtering purposes
    """
    log.info(f"Start of parameters gathering.")
    arguments_list = []
    parser = argparse.ArgumentParser()
    parser.add_argument('--dataset_one', type=str, required=True, help='Please provide path for dataset_one')
    parser.add_argument('--dataset_two', type=str, required=True, help='Please provide path for dataset_two')
    parser.add_argument('--countries', action='store', type=str, nargs="+", help='Please provide list of countries')
    arguments = parser.parse_args()
    arguments_list.append(arguments.dataset_one)
    arguments_list.append(arguments.dataset_two)
    arguments_list.append(arguments.countries)
    log.info(f"End of parameters gathering.")
    log.info(f"Arguments from argparse: {arguments_list}")
    return arguments_list


def drop_columns(df: pyspark.sql.dataframe.DataFrame, columns: List) -> pyspark.sql.dataframe.DataFrame:
    """
        Function drops column(s) from provided DataFrame. Column names are stored in second parameter.
    :param df: Input DataFrame
    :param columns: List of columns to be dropped
    :return: DataFrame
    """
    for column in columns:
        if column in df.columns:
            log.info(f"Column {column} successfully dropped.")
            df = df.drop(column)
        else:
            log.warning(
                f"Column {column} which is requested to be deleted does not exist in the dataframe hence will not be deleted!")
    return df


def rename_columns(df: pyspark.sql.dataframe.DataFrame, dict: Dict[str, str]) -> pyspark.sql.dataframe.DataFrame:
    """
        Function renames columns of provided dataframe according to key,value pairs from dict parameter.
    :param df: Input Dataframe
    :param dict: Dictionary with new/old names for columns
    :return: DataFrame
    """
    for key, value in dict.items():
        if key in df.columns:
            df = df.withColumnRenamed(key, value)
            log.info(f"Column {key} renamed to {value}.")
        else:
            log.warning(f"Column {key} does not exist!")
    return df


def join_dataframes(df1: pyspark.sql.dataframe.DataFrame, df2: pyspark.sql.dataframe.DataFrame,
                    key_columns: List) -> pyspark.sql.dataframe.DataFrame:
    """
        Function joins two dataframes based on fields from key_columns list.
        It also checks if such fields exists in each dataframe.
    :param df1: Input dataframe
    :param df2: Input dataframe
    :param key_columns: List of fields used in join condition
    :return: joined DataFrame
    """
    log.info(f"Check for join condition validity.")
    for join_column in key_columns:
        if join_column in df1.columns and join_column in df2.columns:
            True
        else:
            log.error(f"Column {join_column} does not exist in at least one dataframes hence join operation fails.")
            raise Exception("Cannot join dataframes.")
    df = df1.join(df2, on=key_columns, how="inner")
    log.info(f"Datasets joined successfully.")
    return df


def filter_dataframe(df: pyspark.sql.dataframe.DataFrame, filters: Dict) -> pyspark.sql.dataframe.DataFrame:
    """
        Function filters provided dataframe according to key, value pairs from dict parameter.
    :param df:Input Dataframe
    :param filters: Dictionary of filters
    :return: DataFrame
    """
    for key, value in filters.items():
        if key in df.columns:
            log.info(f"Field {key} will be filtered with following values: {value}")
            df = df.filter(df[key].isin(value))
    log.info(f"Filtering finished. Returning final dataframe.")
    return df


def save_files(df: pyspark.sql.dataframe.DataFrame):
    """
        Functions stores input DataFrame in csv file.
    :param df: Input DataFrame
    :return: none
    """
    current_time = datetime.now()
    folder_structure = str(current_time.year) + "\\" \
                       + str(current_time.month) + "\\" \
                       + str(current_time.day) + "\\" \
                       + str(current_time.hour) + "\\" \
                       + str(current_time.minute) + "\\"
    df.write.option("header", True).csv(
        path=fr'C:\Users\Bartek\PycharmProjects\CODAC\client_data\{folder_structure}\client_data.csv')
    log.info(
        fr"Data saved in C:\Users\Bartek\PycharmProjects\CODAC\src\client_data\{folder_structure}\client_data.csv.")


if __name__ == '__main__':
    drop_set_one = ['first_name', 'last_name']
    drop_set_two = ['cc_n']
    rename_set_one = {"id": "Client_identifier"}
    rename_set_two = {"id": "Client_identifier", "btc_a": "bitcoin_address", "cc_t": "credit_card_type"}
    join_columns = ["Client_identifier"]
    filter_set = {}
    log.info("Program start.")
    spark = SparkSession \
        .builder \
        .master('local') \
        .appName("CODAC") \
        .getOrCreate()
    log.info("Spark initialized.")
    # get arguments from argparse
    args = get_arguments()
    # check if files provided in argparse exists
    file_check(args[0:2])
    # create a list with two file paths for dataset_one and dataset_two
    file_paths = args[0:2]
    # read dataset_one to dataframe
    df1 = read_files(file_paths)[0]
    # read dataset_two to dataframe
    df2 = read_files(file_paths)[1]
    # dropping columns from dataframes
    df1 = drop_columns(df1, drop_set_one)
    df2 = drop_columns(df2, drop_set_two)
    # renaming columns in dataframes
    df1 = rename_columns(df1, rename_set_one)
    df2 = rename_columns(df2, rename_set_two)
    # joining dataframes
    df = join_dataframes(df1, df2, join_columns)
    # filter dataframes
    filter_set["country"] = args[2]
    df = filter_dataframe(df, filter_set)
    # save df as file
    save_files(df)
    log.info("Program end.")
