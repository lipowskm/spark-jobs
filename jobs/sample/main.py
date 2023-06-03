from pyspark.sql import SparkSession


def perform(spark: SparkSession) -> None:  # pragma: no cover
    """Sample job printing DataFrame to console."""
    data = [
        ("James", "Smith", "1991-04-01", "M", 3000),
        ("Michael", "Rose", "2000-05-19", "M", 4000),
        ("Robert", "Williams", "1978-09-05", "M", 4000),
        ("Maria", "Jones", "1967-12-01", "F", 4000),
        ("Jen", "Brown", "1980-02-17", "F", -1),
    ]
    columns = ["firstname", "lastname", "dob", "gender", "salary"]
    df = spark.createDataFrame(data, columns)
    df.show()
