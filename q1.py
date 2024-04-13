import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, length, desc, count, udf, lit
from pyspark.sql.types import BooleanType
# you may add more import if you need to


# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 1").getOrCreate()
# YOUR CODE GOES BELOW

df = spark.read.option("header",True).csv("hdfs://%s:9000/assignment2/part1/input/" % (hdfs_nn))

@udf(returnType=BooleanType()) # using a user defined function to check for no reviews.
def no_reviews(col1):
    if col1:
        return bool(eval(col1))
    else:
        return bool(col1)


df = spark.read.option("header", True).option("inferSchema", True).option("delimiter", ",").option("quotes", '"').csv("hdfs://%s:9000/assignment2/part1/input/" % (hdfs_nn))


df = df.filter(no_reviews(col("Reviews"))).filter((col("Rating") >= 1.0) & (col("Rating").isNotNull())) #removing rows with no reviews or rating < 1.0.

df.show()

df.write.mode("overwrite").csv("/assignment2/output/question1/", header=True)