from pyspark.sql import SparkSession


FILENAME = "full.csv"


spark = SparkSession.builder.appName('Spark Project').getOrCreate()
df = spark.read.csv(FILENAME)

# REPONSE 1
#df.groupBy("_c4").count().orderBy("count", ascending=False).show(10)

# REPONSE 2
#df.filter(df._c4 == "apache/spark").groupBy("_c1").count().orderBy("count", ascending=False).show(1)
