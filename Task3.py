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


# Read data from the user dataset
us = spark.read.json("/datasets/yelp/user.json")

# Select the columns user_id and review_count
df = us.select("user_id", "review_count")

# Filter based on which users have a review_count which is greater than 1000.
filteredDF = df.filter(df.review_count > 1000)

# Select the user_id of the filtered users
influencers = filteredDF.select("user_id")

# All users who have made more than 1000 reviews.
influencers.show()