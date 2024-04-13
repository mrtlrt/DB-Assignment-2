from typing_extensions import final

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, min, max

# you may add more import if you need to


# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 2").getOrCreate()
# YOUR CODE GOES BELOW

df = (
    spark.read.option("header", True)
    .option("inferSchema", True)
    .option("delimiter", ",")
    .option("quotes", '"')
    .csv("hdfs://%s:9000/assignment2/part1/input/" % (hdfs_nn))
)

df = df.filter(df["Price Range"].isNotNull())

# finding the best restaurant s for each city for each price range (in terms of rating).

best_restaurants_df = (df.groupBy("City", "Price Range").agg(max(col("Rating")).alias("Rating")))


# finding the worst restaurant s for each city for each price range (in terms of rating).

worst_restaurants_df = df.groupBy("City", "Price Range").agg(min(col("Rating")).alias("Rating"))


merged_df = best_restaurants_df.union(worst_restaurants_df)

final_df = merged_df.join(df, on=["Price Range", "City", "Rating"], how="inner")

final_df = final_df.dropDuplicates(["Price Range", "City", "Rating"]).orderBy(col("City").asc(),col("Rating").desc())

final_df = final_df.select("_c0",
            "Name",
            "City",
            "Cuisine Style",
            "Ranking",
            "Rating",
            "Price Range",
            "Number of Reviews",
            "Reviews",
            "URL_TA",
            "ID_TA"
)

final_df.show()

final_df.write.mode("overwrite").csv("/assignment2/output/question2/", header=True)