import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark_session() -> SparkSession:
    """
    Get or create SparkSession object for testing purposes.
    :return: SparkSession object
    """
    spark = (
        SparkSession.builder.config("spark.sql.shuffle.partitions", 1)
        .config("spark.default.parallelism", 1)
        .config("spark.rdd.compress", False)
        .config("spark.shuffle.compress", False)
        .config(
            "spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.1.2,"
            "com.amazonaws:aws-java-sdk-bundle:1.11.375,"
            "org.apache.spark:spark-avro_2.12:3.1.2",
        )
        .config("spark.driver.extraJavaOptions", "-Duser.timezone=GMT")
        .config("spark.executor.extraJavaOptions", "-Duser.timezone=GMT")
        .enableHiveSupport()
        .getOrCreate()
    )
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()  # type: ignore
    hadoop_conf.set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hadoop_conf.set("fs.s3a.access.key", "mock")
    hadoop_conf.set("fs.s3a.secret.key", "mock")
    hadoop_conf.set("fs.s3a.endpoint", "http://127.0.0.1:5000")
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    return spark
