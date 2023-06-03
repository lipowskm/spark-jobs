from pyspark.sql import SparkSession


class Log4j:  # pragma: no cover
    """
    Wrapper class for Log4j JVM object.
    Use it to create logger object using
    existing Log4j logger from Spark session.
    >>> spark = SparkSession.builder.getOrCreate()
    >>> logger = Log4j(spark)
    >>> logger.info("Hello Spark")
    :param spark: SparkSession object.
    """

    def __init__(self, spark: SparkSession, log_level: str = "WARN") -> None:
        """
        Initialize logger class.

        :param spark: SparkSession object.
        :param log_level: valid values: ALL, DEBUG, ERROR, FATAL,
            INFO, OFF, TRACE, WARN
        """
        conf = spark.sparkContext.getConf()
        app_id = conf.get("spark.app.id")
        app_name = conf.get("spark.app.name")

        self.__log4j = spark._jvm.org.apache.log4j  # type: ignore
        self.__log_level_mapping = {
            "all": self.__log4j.Level.ALL,
            "debug": self.__log4j.Level.DEBUG,
            "error": self.__log4j.Level.ERROR,
            "fatal": self.__log4j.Level.FATAL,
            "info": self.__log4j.Level.INFO,
            "off": self.__log4j.Level.OFF,
            "trace": self.__log4j.Level.TRACE,
            "warn": self.__log4j.Level.WARN,
        }

        message_prefix = f"<{app_name} {app_id}>"
        self.logger = self.__log4j.LogManager.getLogger(message_prefix)
        self.set_log_level(log_level)

    def error(self, message: str) -> None:
        """
        Log an error.
        :param: Error message to log
        :return: None
        """
        self.logger.error(message)

    def warning(self, message: str) -> None:
        """
        Log a warning.
        :param: Warning message to log
        :return: None
        """
        self.logger.warning(message)

    def info(self, message: str) -> None:
        """
        Log an info.
        :param: Information message to log
        :return: None
        """
        self.logger.info(message)

    def set_log_level(self, log_level: str) -> None:
        """
        Set global log level.
        :param log_level: valid values: ALL, DEBUG, ERROR, FATAL,
            INFO, OFF, TRACE, WARN
        """
        self.logger.setLevel(self.__log_level_mapping[log_level.lower()])
