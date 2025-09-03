from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("spark://spark-master:7077") \
    .appName("test") \
    .getOrCreate()

data = [
    (1, "John", 25),
    (2, "Jane", 30),
    (3, "Jim", 35),
]
columns = ["id", "name", "age"]

df = spark.createDataFrame(data, columns)
df.show()

spark.stop()
