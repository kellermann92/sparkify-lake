from pyspark.sql import SparkSession

def create_spark_session() -> SparkSession:
    """Method to create an SparkSession to connect to AWS EMR Cluster.

    Returns:
        SparkSession: SparkSession object that connects to AWS EMR Cluster.
    """
    session = SparkSession \
        .builder \
        .master('local') \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return session