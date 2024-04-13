import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace, explode, count, split, col, trim

# you may add more import if you need to


# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 4").getOrCreate()
# YOUR CODE GOES BELOW

cuisinesDF = (
    spark.read.option("header", True)
    .option("inferSchema", True)
    .option("delimiter", ",")
    .option("quotes", '"')
    .csv("assignment2/part1/input/TA_restaurants_curated_cleaned.csv")
)

cuisinesDF = cuisinesDF.withColumn("Cuisine Style", regexp_replace("Cuisine Style", "\\[", "")) # remove the [ replace with empty string ""
cuisinesDF = cuisinesDF.withColumn("Cuisine Style", regexp_replace("Cuisine Style", "\\]", "")) # remove the ] replace with empty string ""
cuisinesDF = cuisinesDF.withColumn("Cuisine Style", split(col("Cuisine Style"), ", ")) # format into array for explode function

#takes column "Cuisine Style", exploding it to multiple rows, removing single quotes from each element, and trimming any leading or trailing whitespace from each element
cuisinesDF_expand_rows =  cuisinesDF.withColumn("Cuisine Style", explode("Cuisine Style")).withColumn("Cuisine Style", regexp_replace("Cuisine Style", "'", "")).withColumn("Cuisine Style", trim(col("Cuisine Style")))

filtered_cuisinesDF = cuisinesDF_expand_rows.select("City", "Cuisine Style")
filtered_cuisinesDF = filtered_cuisinesDF.groupBy("City", "Cuisine Style").agg(count("*").alias("count"))
filtered_cuisinesDF = filtered_cuisinesDF.select(col("City").alias("City"), col("Cuisine Style").alias("Cuisine"), col("count").alias("count"))


filtered_cuisinesDF.show()

filtered_cuisinesDF.write.mode("overwrite").csv("assignment2/output/question4/", header=True)