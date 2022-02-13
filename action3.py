from pyspark.sql.functions import *
import Factory

df = Factory.DataFrame()

df.filter(df.repo == "apache/spark")\
    .where(months_between(current_date(), to_date(df.date, "EEE MMM dd HH:mm:ss yyyy Z")) < 24) \
    .groupBy("author") \
    .count() \
    .orderBy("count", ascending=False) \
    .show()
