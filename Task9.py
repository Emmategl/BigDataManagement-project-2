import pyspark
from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql import functions as F
from pyspark.sql.functions import sum as _sum
from pyspark.sql.functions import split
from pyspark.sql.functions import explode
from pyspark.sql.functions import size
from pyspark.sql.functions import col, asc,desc
from pyspark.ml.feature import Tokenizer, RegexTokenizer
from pyspark.sql.functions import col, udf
from pyspark.sql.types import IntegerType

conf=SparkConf()
conf.set("spark.executor.memory", "2G")
conf.set("spark.executor.instances", "4")

spark = SparkSession.builder \
                        .appName('template-application') \
                        .config(conf=conf) \
                        .getOrCreate()

rs = spark.read.json("/datasets/yelp/review.json")
bs = spark.read.json("/datasets/yelp/business.json")

# Select important columns and combine datasets
review = rs.select("business_id", "text")
business = bs.select("name", "business_id", "categories")
combined = review.join(business, on="business_id", how="inner")
20
# Select only restaurants
restaurants = combined.filter(combined.categories.contains("Restaurant"))

# Create RegexTokenizer to find reviews, which contains the words racist, race and racism
regexTokenizer = RegexTokenizer(inputCol="text", outputCol="words", gaps=False, pattern="\W*((?i)racist|race|racism(?-i))\W*")

# Use the regex tokenzier on our combined datasets
regexTokenized = regexTokenizer.transform(restaurants)

# All the empty arrays in the column "words" represents reviews, which does not contain any of the chosen words (racist, race, racism). These empty arrays are converted to null values.
emptyArrayToNull = regexTokenized.withColumn("racist_words", F.when((F.size(F.col("words")) ==
      0), F.lit(None)).otherwise(F.col("words")))

# Now the null values are removed, so we are left with all the reviews, which contains the chosen words.
removeNulls =emptyArrayToNull.na.drop("any")

# Now I select the relevant columns
bsRacism = removeNulls.select("business_id", "name", "racist_words")

# Then i group by the restaurant names and count how many times they appear on the list
reviewsRacism = bsRacism.groupBy("name").count()

# Here we show the restaurants who have received the most reviews, where race, racism or racist is mentioned.
reviewsRacism.orderBy(desc("count")).show()