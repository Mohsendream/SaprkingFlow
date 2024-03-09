from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("PythonWordCountJob").getOrCreate()

text = "Mohsen Mohsen Mohsen Mohsen Hello World data data Engineer Data"
words = spark.sparkContext.parallelize(text.split(" "))
counts = words.map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)
for wc in counts.collect():
    print(wc[0], wc[1])
spark.stop()
