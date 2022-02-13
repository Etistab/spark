from pyspark.sql.functions import *
import Factory

df = Factory.DataFrame()

df.select("repo", explode(split(df.message, '[ ]')).alias('words')) \
    .groupby("words") \
    .count() \
    .orderBy("count", ascending=False) \
    .show(100)
