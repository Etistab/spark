from pyspark.sql import SparkSession

FILENAME = "full.csv"

def DataFrame() :
    spark = SparkSession.builder.appName("myproject").master("local").getOrCreate()
    spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

    return spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load(FILENAME)