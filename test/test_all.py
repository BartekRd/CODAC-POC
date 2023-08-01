import findspark
findspark.init()
import pytest
from chispa import assert_df_equality
from src.main import (
    drop_columns,
    rename_columns,
    join_dataframes,
    filter_dataframe,
    file_check
)

from test.spark_config import spark

@pytest.fixture()
def input(spark):
    dataset_one = spark.createDataFrame(
        data=[
            ("1", "Feliza", "Eusden", "feusden0@ameblo.jp", "Netherlands"),
            ("2", "Priscilla", "Le Pine", "plepine1@biglobe.ne.jp", "Netherlands"),
            ("3", "Jaimie", "Sandes", "jsandes2@reuters.com", "Netherlands"),
            ("4", "Nari", "Dolphin", "ndolphin3@cbslocal.com", "France"),
            ("5", "Garik", "Farre", "gfarre4@economist.com", "France"),
            ("6", "Kordula", "Broodes", "kbroodes5@amazon.de", "France"),
            ("7", "Rakel", "Ingliby", "ringliby6@ft.com", "United States"),
        ],
        schema=["id", "first_name", "last_name", "email", "country"]
    )

    dataset_two = spark.createDataFrame(
        data=[
            ("1", "1wjtPamAZeGhRnZfhBAHHHjNvnHefd2V2", "visa - electron", "4175006996999270"),
            ("2", "1Js9BA1rV31hJFmN25rh8HWfrrYLXAyw9T", "jcb", "3587679584356527"),
            ("3", "1CoG9ciLQVQCnia5oXfXPSag4DQ4iYeSpd", "diners - club - enroute", "201876885481838"),
            ("4", "1GNvinVKGzPBVNZScNA2jKnDSBs4R7Y3rY", "switch", "564182038040530730"),
            ("5", "1DHTzZ7ypu3EzWtLBFiWoRo9svd1STMyrg", "jcb", "3555559025151828"),
            ("6", "1LWktvit3XBCJNrsji7rWj2qEa5XAmyJiC", "jcb", "3580083825272493"),
            ("7", "1J71SRGqUjhqPuHaZaG8wEtKdNRaKUiuzm", "switch", "491193585665108260"),
            ("8", "1Q5FAwgXbhRxP1uYpgwXMY67zFzUDkwutW", "mastercard", "5100174550682620"),
            ("9", "1QKy8RoeWR48nrwknBDZKfmetv1SGoAHX4", "diners - club - carte - blanche", "30343863921001"),
            ("10", "1NRDQBCtuDqm8QomrbSUyh2tGDRqoj3DU8", "diners - club - carte - blanche", "30559612937267"),
            ("11", "1HcqQ5Ys77sJm3ZJvxEAPLjCzB8LXVBTHU", "visa", "4937793997478"),
            ("12", "1EncEr6Vd5ywk96une6ZuWZN1yn3gBGZX9", "jcb", "3569513122126013"),
            ("13", "14bMXV3h1S6KxGHdeNB9Bz4mmzCZJdRvk5", "jcb", "3537645802098952"),
        ],
        schema=["id", "btc_a", "cc_t", "cc_n"]
    )
    return dataset_one, dataset_two


@pytest.fixture()
def drop_output(spark):
    drop_columns_dataset_one = spark.createDataFrame(
        data=[
            ("1", "feusden0@ameblo.jp", "Netherlands"),
            ("2", "plepine1@biglobe.ne.jp", "Netherlands"),
            ("3", "jsandes2@reuters.com", "Netherlands"),
            ("4", "ndolphin3@cbslocal.com", "France"),
            ("5", "gfarre4@economist.com", "France"),
            ("6", "kbroodes5@amazon.de", "France"),
            ("7", "ringliby6@ft.com", "United States"),
        ],
        schema=["id", "email", "country"]
    )

    drop_columns_dataset_two = spark.createDataFrame(
        data=[
            ("1", "1wjtPamAZeGhRnZfhBAHHHjNvnHefd2V2", "visa - electron"),
            ("2", "1Js9BA1rV31hJFmN25rh8HWfrrYLXAyw9T", "jcb"),
            ("3", "1CoG9ciLQVQCnia5oXfXPSag4DQ4iYeSpd", "diners - club - enroute"),
            ("4", "1GNvinVKGzPBVNZScNA2jKnDSBs4R7Y3rY", "switch"),
            ("5", "1DHTzZ7ypu3EzWtLBFiWoRo9svd1STMyrg", "jcb"),
            ("6", "1LWktvit3XBCJNrsji7rWj2qEa5XAmyJiC", "jcb"),
            ("7", "1J71SRGqUjhqPuHaZaG8wEtKdNRaKUiuzm", "switch"),
            ("8", "1Q5FAwgXbhRxP1uYpgwXMY67zFzUDkwutW", "mastercard"),
            ("9", "1QKy8RoeWR48nrwknBDZKfmetv1SGoAHX4", "diners - club - carte - blanche"),
            ("10", "1NRDQBCtuDqm8QomrbSUyh2tGDRqoj3DU8", "diners - club - carte - blanche"),
            ("11", "1HcqQ5Ys77sJm3ZJvxEAPLjCzB8LXVBTHU", "visa"),
            ("12", "1EncEr6Vd5ywk96une6ZuWZN1yn3gBGZX9", "jcb"),
            ("13", "14bMXV3h1S6KxGHdeNB9Bz4mmzCZJdRvk5", "jcb"),
        ],
        schema=["id", "btc_a", "cc_t"]
    )
    return drop_columns_dataset_one, drop_columns_dataset_two


@pytest.fixture()
def rename_output(spark):
    rename_columns_dataset_one = spark.createDataFrame(
        data=[
            ("1", "feusden0@ameblo.jp", "Netherlands"),
            ("2", "plepine1@biglobe.ne.jp", "Netherlands"),
            ("3", "jsandes2@reuters.com", "Netherlands"),
            ("4", "ndolphin3@cbslocal.com", "France"),
            ("5", "gfarre4@economist.com", "France"),
            ("6", "kbroodes5@amazon.de", "France"),
            ("7", "ringliby6@ft.com", "United States"),
        ],
        schema=["Client_identifier", "email", "country"]
    )

    rename_columns_dataset_two = spark.createDataFrame(
        data=[
            ("1", "1wjtPamAZeGhRnZfhBAHHHjNvnHefd2V2", "visa - electron"),
            ("2", "1Js9BA1rV31hJFmN25rh8HWfrrYLXAyw9T", "jcb"),
            ("3", "1CoG9ciLQVQCnia5oXfXPSag4DQ4iYeSpd", "diners - club - enroute"),
            ("4", "1GNvinVKGzPBVNZScNA2jKnDSBs4R7Y3rY", "switch"),
            ("5", "1DHTzZ7ypu3EzWtLBFiWoRo9svd1STMyrg", "jcb"),
            ("6", "1LWktvit3XBCJNrsji7rWj2qEa5XAmyJiC", "jcb"),
            ("7", "1J71SRGqUjhqPuHaZaG8wEtKdNRaKUiuzm", "switch"),
            ("8", "1Q5FAwgXbhRxP1uYpgwXMY67zFzUDkwutW", "mastercard"),
            ("9", "1QKy8RoeWR48nrwknBDZKfmetv1SGoAHX4", "diners - club - carte - blanche"),
            ("10", "1NRDQBCtuDqm8QomrbSUyh2tGDRqoj3DU8", "diners - club - carte - blanche"),
            ("11", "1HcqQ5Ys77sJm3ZJvxEAPLjCzB8LXVBTHU", "visa"),
            ("12", "1EncEr6Vd5ywk96une6ZuWZN1yn3gBGZX9", "jcb"),
            ("13", "14bMXV3h1S6KxGHdeNB9Bz4mmzCZJdRvk5", "jcb"),
        ],
        schema=["Client_identifier", "bitcoin_address", "credit_card_type"]
    )
    return rename_columns_dataset_one, rename_columns_dataset_two


@pytest.fixture()
def join_output(spark):
    joined_columns_dataset = spark.createDataFrame(
        data=[
            ("1", "feusden0@ameblo.jp", "Netherlands", "1wjtPamAZeGhRnZfhBAHHHjNvnHefd2V2", "visa - electron"),
            ("2", "plepine1@biglobe.ne.jp", "Netherlands", "1Js9BA1rV31hJFmN25rh8HWfrrYLXAyw9T", "jcb"),
            ("3", "jsandes2@reuters.com", "Netherlands", "1CoG9ciLQVQCnia5oXfXPSag4DQ4iYeSpd", "diners - club - enroute"),
            ("4", "ndolphin3@cbslocal.com", "France", "1GNvinVKGzPBVNZScNA2jKnDSBs4R7Y3rY", "switch"),
            ("5", "gfarre4@economist.com", "France", "1DHTzZ7ypu3EzWtLBFiWoRo9svd1STMyrg", "jcb"),
            ("6", "kbroodes5@amazon.de", "France", "1LWktvit3XBCJNrsji7rWj2qEa5XAmyJiC", "jcb"),
            ("7", "ringliby6@ft.com", "United States", "1J71SRGqUjhqPuHaZaG8wEtKdNRaKUiuzm", "switch"),
        ],
        schema=["Client_identifier", "email", "country", "bitcoin_address", "credit_card_type"]
    )
    return joined_columns_dataset


@pytest.fixture()
def filter_output(spark):
    filtered_columns_dataset = spark.createDataFrame(
        data=[
            ("4", "ndolphin3@cbslocal.com", "France", "1GNvinVKGzPBVNZScNA2jKnDSBs4R7Y3rY", "switch"),
            ("5", "gfarre4@economist.com", "France", "1DHTzZ7ypu3EzWtLBFiWoRo9svd1STMyrg", "jcb"),
            ("6", "kbroodes5@amazon.de", "France", "1LWktvit3XBCJNrsji7rWj2qEa5XAmyJiC", "jcb"),
        ],
        schema=["Client_identifier", "email", "country", "bitcoin_address", "credit_card_type"]
    )
    return filtered_columns_dataset


class TestCodac:

    def test_file_check(self):
        paths = [r"C:\Users\Bartek\PycharmProjects\CODAC\dataset_one.csv", r"C:\Users\Bartek\PycharmProjects\CODAC\dataset_one.csv", r"C:\Users\Bartek\PycharmProjects\CODAC\XYZ\dataset_one.csv"]
        assert file_check(paths) == False

    def test_drop_columns(self, spark, input, drop_output):
        drop_set_one = ['first_name', 'last_name']
        drop_set_two = ['cc_n']
        df1 = drop_columns(input[0], drop_set_one)
        df2 = drop_columns(input[1], drop_set_two)
        assert_df_equality(df1, drop_output[0])
        assert_df_equality(df2, drop_output[1])

    def test_rename_columns(self, spark, drop_output, rename_output):
        rename_set_one = {"id": "Client_identifier"}
        rename_set_two = {"id": "Client_identifier", "btc_a": "bitcoin_address", "cc_t": "credit_card_type"}
        df1 = rename_columns(drop_output[0], rename_set_one)
        df2 = rename_columns(drop_output[1], rename_set_two)
        assert_df_equality(df1, rename_output[0])
        assert_df_equality(df2, rename_output[1])

    def test_df_join(self, spark, rename_output, join_output):
        join_columns = ["Client_identifier"]
        df1 = join_dataframes(rename_output[0], rename_output[1], join_columns)
        assert_df_equality(df1, join_output)

    def test_df_filter(self, spark, join_output, filter_output):
        filter_set = {}
        filter_set["country"] = {"France"}
        join_columns = ["Client_identifier"]
        df1 = filter_dataframe(join_output, filter_set)
        assert_df_equality(df1, filter_output)
