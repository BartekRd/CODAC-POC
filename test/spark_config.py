import pytest
from pyspark.sql import SparkSession

def get_spark():
    spark = SparkSession \
        .builder \
        .master('local') \
        .appName("CODAC") \
        .getOrCreate()

    return spark

@pytest.fixture()
def spark():
    print("spark setup")
    spark_session = get_spark()
    yield spark_session
    print("teardown")