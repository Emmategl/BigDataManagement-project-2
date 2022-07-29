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
# Read data from the business dataset and filter based on which businesses are restaurants
bs = spark.read.json("/datasets/yelp/business.json")
business = bs.select("categories", "business_id", "stars")
restaurants = business.filter(business.categories.contains("Restaurant"))

# Only include the resturants which have more than 2 stars
df = restaurants.filter(restaurants.stars > 2)

# All the next queries do the same thing. They look at a specific cuisine and finds the average stars based on all the resturants which have this specific cusinie mentioned in the category

s = df.withColumn('contain_spanish', col('categories').rlike('(?i)^*spanish$'))
s.groupby('contain_spanish').agg({'Stars': 'mean'}).show(1)
m = df.withColumn('contain_mexican', col('categories').rlike('(?i)^*mexican$'))
m.groupby('contain_mexican').agg({'Stars': 'mean'}).show(1)
c = df.withColumn('contain_chinese', col('categories').rlike('(?i)^*chinese$'))
c.groupby('contain_chinese').agg({'Stars': 'mean'}).show(1)
f = df.withColumn('contain_french', col('categories').rlike('(?i)^*french$'))
f.groupby('contain_french').agg({'Stars': 'mean'}).show(1)
m = df.withColumn('contain_middleEastern', col('categories').rlike('(?i)^*Middle Eastern$'))
m.groupby('contain_middleEastern').agg({'Stars': 'mean'}).show(1)
i = df.withColumn('contain_italian', col('categories').rlike('(?i)^*italian$'))
i.groupby('contain_italian').agg({'Stars': 'mean'}).show(1)
p = df.withColumn('contain_portuguese', col('categories').rlike('(?i)^*portuguese$'))
p.groupby('contain_portuguese').agg({'Stars': 'mean'}).show(1)
a = df.withColumn('contain_american', col('categories').rlike('(?i)^*american$'))
a.groupby('contain_american').agg({'Stars': 'mean'}).show(1)