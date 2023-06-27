from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, FloatType, IntegerType

# Create a SparkSession
spark = SparkSession.builder.appName("CorrelationAnalysis").getOrCreate()

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

# Calculate the correlation matrix
correlation_matrix = wine.stat.corr()

# Show the correlation matrix
correlation_matrix.show()

# Stop the SparkSession
spark.stop()
