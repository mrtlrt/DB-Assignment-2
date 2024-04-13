import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, lit, dense_rank
from pyspark.sql.window import Window

# you may add more import if you need to


# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 3").getOrCreate()
# YOUR CODE GOES BELOW

restaurantsDF = (
    spark.read.option("header", True)
    .option("inferSchema", True)
    .option("delimiter", ",")
    .option("quotes", '"')
    .csv("assignment2/part1/input/TA_restaurants_curated_cleaned.csv")
)

# Calculate average rating per city
cityAvgRatingDF = restaurantsDF.groupBy("City").agg(avg("Rating").alias("AvgRating"))

# Rank cities based on average rating
windowSpec = Window.orderBy(cityAvgRatingDF["AvgRating"].desc())
rankedCitiesDF = cityAvgRatingDF.withColumn("RatingRank", dense_rank().over(windowSpec))

# Get top 3 and bottom 3 cities
top3CitiesDF = rankedCitiesDF.filter(rankedCitiesDF["RatingRank"] <= 3)
bottom3CitiesDF = rankedCitiesDF.filter(rankedCitiesDF["RatingRank"] > rankedCitiesDF.count() - 3)

# Add RatingGroup column
top3CitiesDF = top3CitiesDF.withColumn("RatingGroup", lit("Top"))
bottom3CitiesDF = bottom3CitiesDF.withColumn("RatingGroup", lit("Bottom"))

# Combine and sort top and bottom cities
combinedDF = top3CitiesDF.union(bottom3CitiesDF).select("City", "AvgRating", "RatingGroup")

combinedDF.show()

combinedDF.write.mode("overwrite").csv("assignment2/output/question3/", header=True)