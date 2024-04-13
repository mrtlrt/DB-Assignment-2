import sys 
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col, concat_ws, from_json, count, array, array_sort
from pyspark.sql.window import Window
from pyspark.sql.types import ArrayType, StructType, StructField, StringType
# you may add more import if you need to

# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 5").getOrCreate()
# YOUR CODE GOES BELOW

df = (
    spark.read.option("header", True)
    .option("inferSchema", True)
    .option("delimiter", ",")
    .option("quotes", '"')
    .parquet("/content/assignment2/part2/input/tmdb_5000_credits.parquet")
)

json = ArrayType(StructType([StructField("name", StringType(), False)])) # array of objects, where each object has a single field named "name" containing string values

df = df.withColumn("actor1", explode(from_json(col("cast"), json).getField("name"))) # explode "cast" column and extract "name" field from each JSON object within array

df = df.withColumn("actor2", explode(from_json(col("cast"), json).getField("name"))) # do the same for actor 2

df = df.select("movie_id", "title", "actor1", "actor2") 

df = df.filter(col("actor1") != col("actor2"))

df = df.withColumn("cast_pair", array(col("actor1"), col("actor2")).cast("string")) # create array column containing pairs of actors and convert it to string

df = df.dropDuplicates(["movie_id", "title", "cast_pair"]) # drop duplicate rows based on movie_id, title, and cast_pair column

totalDF = df.groupBy("cast_pair").agg(count("*").alias("count")).filter(col("count") >= 2)

actorPairsDF = totalDF.join(df, ["cast_pair"], "inner").drop("cast_pair", "count")

actorPairsDF.show()