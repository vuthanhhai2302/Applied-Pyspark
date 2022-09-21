from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

def init_spark_session(AppName) -> SparkSession:
    master = "local"
    conf = (
        SparkConf()\
        .setMaster(master)\
        .setAppName(AppName)\
        .set("spark.sql.session.timeZone", "UTC+7")\
        .set("spark.driver.memory", "4g")
    )
    spark = (
        SparkSession.builder.config(conf=conf).getOrCreate()
    )

    return spark