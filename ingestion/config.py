from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.appName('e-commerce')
    .master('local[*]')
    .getOrCreate()
)
