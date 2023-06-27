from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, FloatType, IntegerType

spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

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

wine = spark.read.format("csv")\
    .schema(myschema)\
    .option("header", True)\
    .option("path", "hdfs:///lab_test/wine.csv")\
    .load()

wine.printSchema()

output = wine.select(wine.quality, wine.fixed_acidity, wine.volatile_acidity,
                     wine.citric_acid, wine.residual_sugar, wine.chlorides,
                     wine.free_sulfur_dioxide, wine.total_sulfur_dioxide,
                     wine.density, wine.pH, wine.sulphates, wine.alcohol)\
    .where(wine.quality >= 6)\
    .orderBy(wine.quality)\
    .cache()

output.createOrReplaceTempView("wine_data")

spark.sql("SELECT quality, fixed_acidity, volatile_acidity FROM wine_data WHERE residual_sugar < 1.0 ORDER BY quality").show()

output.write.format("json")\
    .mode("overwrite")\
    .option("path", "hdfs:///lab_test/job_output/")\
    .partitionBy("quality")\
    .save()
