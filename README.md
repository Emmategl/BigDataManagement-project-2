# Big Data Management , MSc (Fall 2021)

In this assignment you will be working with semi-structured data (JSON) on a Spark cluster. The main task is to perform queries and basic analytics on a (somewhat) big data. The dataset is not huge, but big enough that working with it on a normal laptop would quickly become tiresome. The machines you will be running your code on is a Hadoop/Spark cluster, comprised of 5 desktop machines.

Once you’re on ambari0, where you can run pyspark to access a Python REPL shell, where the Spark context is available. The Spark context is the entry point for the Spark API.

The assignment consists of two sections: In the first you will create specific queries using Spark DataFrames, while the second is more freeform where you will answer high-level questions by querying the data and reasoning the results.


Formulate the following queries using Spark DataFrames, not using the spark.sql-method:
1. Analyze business.json to find the total number of reviews for all businesses. The out- put should be in the form of a Spark DataFrame with one value representing the count.
2. Analyze business.json to find all businesses that have received 5 stars and that have been reviewed by 1000 or more users. The output should be in the form of DataFrame of (name, stars, review count).
3. Analyzeuser.jsontofindtheinfluencerswhohavewrittenmorethan1000reviews.The output should be in the form of DataFrame of user id.
4. Analyze review.json, business.json, and a view created from your answer to Q3 to find the businesses that have been reviewed by more than 5 influencer users.
5. Analyze review.json and users.json to find an ordered list of users based on the av- erage star counts they have given in all their reviews.
