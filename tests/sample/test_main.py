from pyspark.sql import SparkSession


def test_sample(spark_session: SparkSession) -> None:
    """Sample test case."""
    df = spark_session.createDataFrame(data=[[1, "a"], [2, "b"]], schema=["c1", "c2"])
    assert df.count() == 2
