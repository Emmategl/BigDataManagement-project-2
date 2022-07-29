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


# Read data from the business and review datasets
bs = spark.read.json("/datasets/yelp/business.json")
rs = spark.read.json("/datasets/yelp/review.json")
# Join the two datasets with an inner join
combined = rs.join(bs, on="business_id", how="inner")

# Use the answer from Q3 and create a view
us = spark.read.json("/datasets/yelp/user.json")
df = us.select("user_id", "review_count")
filteredDF = df.filter(df.review_count > 1000)
influencers = filteredDF.select("user_id")
influencers.createTempView("influ")

# Read the view
allInfluencers = spark.read.table("influ")

# Combine the business and review datasets with the influencers with an inner join.
combinedInfluencers = combined.join(allInfluencers, on="user_id", how="inner")

# Group by business_id and use count to see how many times a business have been reviewed
countReviews = combinedInfluencers.groupBy("business_id").count()

# Filter the datasets based on which businesses have been reviewed more than 5 times
final = countReviews.filter(countReviews['count'] > 5)

# All businesses that have beeen reviewed by more than 5 influencers
final.show()
