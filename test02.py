#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions

if __name__ == "__main__":
    # Create a SparkSession
    spark = SparkSession.builder.appName("MongoDBIntegration").getOrCreate()

    # Read the "quality" data back from MongoDB into a new DataFrame
    readQuality = spark.read \
        .format("com.mongodb.spark.sql.DefaultSource") \
        .option("uri", "mongodb://127.0.0.1/wine_db.quality") \
        .load()

    # Create a temporary view of the DataFrame
    readQuality.createOrReplaceTempView("quality")

    # Print the schema of the DataFrame
    readQuality.printSchema()

    # Perform a SQL query on the DataFrame
    sqlDF = spark.sql("""
        SELECT quality, COUNT(*) as cnt
        FROM quality
        GROUP BY quality
        ORDER BY cnt DESC
    """)

    # Show the result of the SQL query
    sqlDF.show()

