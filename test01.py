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
    spark = SparkSession.\
    builder.appName("MongoDBIntegration").\
    getOrCreate()

     # Build RDD on top of users data file
    lines = spark.sparkContext.textFile("hdfs:///lab_test/wine")
    
     # Creating new RDD by passing the parser fuction
    data = lines.map(parseInput)

    # Convert RDD into a DataFrame
    usersDataset = spark.createDataFrame(quality)
    
    # Write the data into MongoDB
    qualityDataset.write \
        .format("com.mongodb.spark.sql.DefaultSource") \
        .option("uri", "mongodb://127.0.0.1/winedata.quality") \
        .mode("append") \
        .save()

