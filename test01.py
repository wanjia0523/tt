from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import col

def parseInput(line):
    fields = line.split(',')
    return Row(
        fixed_acidity=float(fields[0]),
        volatile_acidity=float(fields[1]),
        citric_acid=float(fields[2]),
        residual_sugar=float(fields[3]),
        chlorides=float(fields[4]),
        free_sulfur_dioxide=int(fields[5]),
        total_sulfur_dioxide=int(fields[6]),
        density=float(fields[7]),
        pH=float(fields[8]),
        sulphates=float(fields[9]),
        alcohol=float(fields[10]),
        quality=int(fields[11])
    )

if __name__ == "__main__":
    # Create a SparkSession
    spark = SparkSession.builder.appName("WineQuality").getOrCreate()

    # Read the dataset into a DataFrame
    lines = spark.sparkContext.textFile("hdfs:///lab_test/wine")
    data = lines.map(parseInput).toDF()

    # Select the "quality" column
    quality_df = data.select(col("quality"))

    # Write the "quality" DataFrame to MongoDB
    quality_df.write \
        .format("com.mongodb.spark.sql.DefaultSource") \
        .option("uri", "mongodb://127.0.0.1/wine_db") \
        .mode("append") \
        .save()

    # Stop the SparkSession
    spark.stop()

