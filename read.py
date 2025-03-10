import os
os.environ["SPARK_LOCAL_IP"] = "192.168.211.129"
from pyspark.sql import SparkSession


spark = SparkSession.builder \
    .appName("Top10Businesses") \
    .getOrCreate()

hdfs_file_path = 'hdfs://localhost:9000/user/ibrahim/yelp/business/business.json'
spark_df = spark.read.json(hdfs_file_path)
top_10_businesses = spark_df.orderBy('stars', ascending=False).limit(10)

print("Top 10 businesses with the highest star rating:")
top_10_businesses.show()


spark.stop()