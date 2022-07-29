import pyspark
from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql import functions as func
from pyspark.sql.functions import sum as _sum
from pyspark.sql.functions import split
from pyspark.sql.functions import explode
from pyspark.sql.functions import size
from pyspark.sql.functions import col, asc,desc

conf=SparkConf()
conf.set("spark.executor.memory", "2G")
conf.set("spark.executor.instances", "4")

spark = SparkSession.builder \
                        .appName('template-application') \
                        .config(conf=conf) \
                        .getOrCreate()

# Read data from the review and business datasets
rs = spark.read.json("/datasets/yelp/review.json")
bs = spark.read.json("/datasets/yelp/business.json")

# Select usefull columns and combine the two datasets
review = rs.select("business_id", "funny", "text", "stars")
business = bs.select("attributes", "categories", "city", "name", "state", "business_id")
combined = review.join(business, on="business_id", how="inner")

# Filter based on the amount of stars being less than 3
underTwoStars = combined.filter(combined.stars < 3)

# Group by business_id and use aggregation to find the amount of "Funny" comments for each business (still having less than 3 stars)
funnyReviwes = underTwoStars \
 .groupBy("business_id") \
 .agg(func.sum("funny")) \

# Filter based on the sum of "Funny" being more than 50.
bsBadButFunny = funnyReviwes.filter(funnyReviwes["sum(funny)"] > 50)

# Make a datasets only including resturants
tableColumns = bs.select("business_id", "categories", "attributes", "city")
restaurants = tableColumns.filter(tableColumns.categories.contains("Restaurant"))

# Join the two tables, so we end up with a datasets which only include the resturants which have more than 50 "funny" comments that have less than 3 stars.
resturantsBadButFunny = bsBadButFunny.join(restaurants, on="business_id", how="inner")

# Find all the different categories related to an business and count the number of times it appears.
df = resturantsBadButFunny.select(explode(split(resturantsBadButFunny.categories,",")).alias("word")).groupBy("word").count()

# Order the dataset by descending to display the categories which appear the most in restaurants with funny but bad reviews.
df.orderBy(desc("count")).show()