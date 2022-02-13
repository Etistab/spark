import Factory

df = Factory.DataFrame()

df.groupby("repo") \
    .count() \
    .orderBy("count", ascending=False) \
    .show(10)
