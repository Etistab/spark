import Factory

df = Factory.DataFrame()

df.filter(df.repo == "apache/spark")\
    .groupby("author")\
    .count() \
    .orderBy("count", ascending=False) \
    .show(1)
