import pyspark
from pyspark.sql import SparkSession
from pyspark import SparkConf
conf=SparkConf()
conf.set("spark.executor.memory", "2G")
conf.set("spark.executor.instances", "4")

spark = SparkSession.builder \
                        .appName('template-application') \
                        .config(conf=conf) \
                        .getOrCreate()


# I choose not to include the review dataset, because the user datasets already included the average star counts based on all their reviews

# Read data from the user dataset
us = spark.read.json("/datasets/yelp/user.json")

# Select the user_id and the average_stars from the table
df = us.select("user_id", "average_stars")

# Filter the results ascending based on average_stars
filteredDF = df.sort("average_stars", ascending=False)

# An ordered list of users based on the average star counts they have given in all their reviews
filteredDF.show()