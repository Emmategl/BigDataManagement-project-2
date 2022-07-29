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

# Read data from the business dataset
bs = spark.read.json("/datasets/yelp/business.json")

# Select the review_count column
numReviews = bs.select("review_count")

# Group the number of reviews by sum
sumReviews = numReviews.groupBy().sum()

#The totalt number of reviews for all businesses
sumReviews.show()