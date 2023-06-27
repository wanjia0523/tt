from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, FloatType, IntegerType
from pyspark.sql.functions import col

# Create a SparkSession
spark = SparkSession.builder.appName("WineDataExploration").getOrCreate()

# Define the schema for the wine data
myschema = StructType([
    StructField("fixed_acidity", FloatType(), True),
    StructField("volatile_acidity", FloatType(), True),
    StructField("citric_acid", FloatType(), True),
    StructField("residual_sugar", FloatType(), True),
    StructField("chlorides", FloatType(), True),
    StructField("free_sulfur_dioxide", FloatType(), True),
    StructField("total_sulfur_dioxide", FloatType(), True),
    StructField("density", FloatType(), True),
    StructField("pH", FloatType(), True),
    StructField("sulphates", FloatType(), True),
    StructField("alcohol", FloatType(), True),
    StructField("quality", IntegerType(), True)
])

# Read the wine data from CSV
wine = spark.read.format("csv") \
    .schema(myschema) \
    .option("header", True) \
    .option("path", "hdfs:///lab_test/wine.csv") \
    .load()

# Print the schema of the wine DataFrame
wine.printSchema()

# Calculate descriptive statistics for numeric columns
numeric_columns = ["fixed_acidity", "volatile_acidity", "citric_acid", "residual_sugar",
                   "chlorides", "free_sulfur_dioxide", "total_sulfur_dioxide", "density",
                   "pH", "sulphates", "alcohol"]

statistics = wine.select(*[col(column).cast("double").alias(column) for column in numeric_columns]) \
    .describe().filter(col("summary").isin(["mean", "stddev", "min", "max", "25%", "50%", "75%"]))

statistics.show()

# Save the exploration results as JSON
output_path = "hdfs:///lab_test/exploration_output"
statistics.write.format("json").mode("overwrite").save(output_path)

# Stop the SparkSession
spark.stop()
