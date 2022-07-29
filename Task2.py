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

# Select the columns: name, stars and review_count
df = bs.select("name", "stars", "review_count")

# Filter the dataframe based on number of reviews and which restaurants have 5 stars
filteredDF = df.filter((df.review_count >= 1000) & (df.stars == 5))

# All the businesses that have been reviewed by 1000 or more users, and where the "stars" in the business dataset is equal to 5.

filteredDF.show()