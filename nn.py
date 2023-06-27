from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import GeneralizedLinearRegression
from pyspark.ml.evaluation import RegressionEvaluator

# Create a SparkSession
spark = SparkSession.builder.appName("NeuralNetworkRegression").getOrCreate()

# Read the dataset from HDFS
data_path = "hdfs://localhost:9000/lab_test/wine.csv"
dataset = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(data_path)

# Prepare the data for training
assembler = VectorAssembler(inputCols=["fixed acidity", "volatile acidity", "citric acid", "residual sugar",
                                       "chlorides", "free sulfur dioxide", "total sulfur dioxide", "density", "pH",
                                       "sulphates", "alcohol"], outputCol="features")
data = assembler.transform(dataset)
train_data, test_data = data.randomSplit([0.7, 0.3])

# Create a GeneralizedLinearRegression model
glr = GeneralizedLinearRegression(family="gaussian", link="identity", featuresCol="features", labelCol="quality")

# Train the model
model = glr.fit(train_data)

# Make predictions on the test data
predictions = model.transform(test_data)

# Evaluate the model
evaluator = RegressionEvaluator(labelCol="quality", metricName="rmse")
rmse = evaluator.evaluate(predictions)
mse = evaluator.evaluate(predictions, {evaluator.metricName: "mse"})

print("Root Mean Squared Error (RMSE):", rmse)
print("Mean Squared Error (MSE):", mse)

# Stop the SparkSession
spark.stop()
