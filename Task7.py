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

# Select the usefull columns from the dataset
business = bs.select("attributes.BusinessAcceptsBitcoin", "business_id", "categories", "state")

# Select only restaurants
restaurants = business.filter(business.categories.contains("Restaurant"))

# Only include the restaurants which accepts Bitcoin
acceptsBC = restaurants.filter(restaurants.BusinessAcceptsBitcoin == "True")

# Count which categories are most often used within restaurants which accepts Bitcoins
df = acceptsBC.select(explode(split(acceptsBC.categories,",")).alias("word")).groupBy("word").count()

# Display the total amount of restaurants which accepts Bitcoin
acceptsBC.count()

# Order the dataset descending based on categories
df.orderBy(desc("count")).show()

# Order the datasets descending based on which states the restaurants is located in
dfStates = acceptsBC.groupBy('state').count()
dfStates.orderBy(desc("count")).show()