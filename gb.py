from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Create a SparkSession
spark = SparkSession.builder.appName("WineQualityAnalysis").getOrCreate()

# Read the wine data from CSV
wine = spark.read.format("csv") \
    .option("header", True) \
    .option("inferSchema", True) \
    .option("path", "hdfs:///lab_test/wine.csv") \
    .load()

# Group the data by quality and count the occurrences
quality_counts = wine.groupBy("quality").count().orderBy("quality")

# Show the frequency of each quality rating
quality_counts.show()

# Stop the SparkSession
spark.stop()




