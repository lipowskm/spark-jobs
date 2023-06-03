import os
import sys
from typing import Dict, Optional

from utils import logger

try:
    import pyspark
except ImportError:
    import findspark

    findspark.init()
    import pyspark


def get_spark_session(
    app_name: str, env_vars: Optional[Dict[str, str]] = None, use_boto3: bool = False
) -> pyspark.sql.SparkSession:  # pragma: no cover
    """
    Get existing or create new SparkSession object.

    :param app_name: name of your Spark application
    :param env_vars: environmental variables to pass to Spark cluster
    :param use_boto3: whether to use boto3 library to gain access to AWS
    :return: SparkSession object
    """
    if use_boto3:
        try:
            from boto3.session import Session
        except ImportError:
            logger.error(
                "To use SparkSession with AWS connection, install boto3 package"
            )
            sys.exit(1)

        credentials = Session().get_credentials()

        spark = (
            pyspark.sql.SparkSession.builder.config(
                "spark.jars.packages",
                "org.apache.hadoop:hadoop-aws:3.3.0,"
                "com.amazonaws:aws-java-sdk-bundle:1.11.375,"
                "org.apache.spark:spark-avro_2.12:3.3.0",
            )
            .appName(app_name)
            .config("spark.driver.extraJavaOptions", "-Duser.timezone=GMT")
            .config("spark.executor.extraJavaOptions", "-Duser.timezone=GMT")
            .getOrCreate()
        )
        sc = spark.sparkContext
        hadoop_conf = sc._jsc.hadoopConfiguration()  # type: ignore

        sc.setSystemProperty("com.amazonaws.services.s3.enableV4", "true")
        hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        hadoop_conf.set("fs.s3a.access.key", credentials.access_key)
        hadoop_conf.set("fs.s3a.secret.key", credentials.secret_key)
        hadoop_conf.set("fs.s3a.session.token", credentials.token)
        hadoop_conf.set("fs.s3a.endpoint", "s3.amazonaws.com")
        hadoop_conf.set(
            "fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider",
        )
        hadoop_conf.set("mapreduce.fileoutputcommitter.algorithm.version", "2")
        hadoop_conf.set("fs.s3a.committer.name", "partitioned")
        hadoop_conf.set("fs.s3a.committer.magic.enabled", "false")
        hadoop_conf.set("fs.s3a.committer.staging.conflict-mode", "replace")
        hadoop_conf.set("fs.s3a.fast.upload.buffer", "bytebuffer")
    else:
        spark = (
            pyspark.sql.SparkSession.builder.appName(app_name)
            .config("spark.driver.extraJavaOptions", "-Duser.timezone=GMT")
            .config("spark.executor.extraJavaOptions", "-Duser.timezone=GMT")
            .getOrCreate()
        )
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    if env_vars:
        for key, value in env_vars.items():
            os.environ[key] = value
            spark.conf.set(f"spark.appMasterEnv.{key}", value)
            spark.conf.set(f"spark.executorEnv.{key}", value)
    return spark
