from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator

# Create a SparkSession
spark = SparkSession.builder.appName("RandomForestRegression").getOrCreate()

# Read the dataset from a CSV file
dataset = spark.read.csv(r"C:\Users\user\Documents\wine.csv", header=True, inferSchema=True)

# Prepare the data for training
assembler = VectorAssembler(inputCols=["fixed acidity", "volatile acidity", "citric acid", "residual sugar",
                                       "chlorides", "free sulfur dioxide", "total sulfur dioxide", "density", "pH",
                                       "sulphates", "alcohol"], outputCol="features")
data = assembler.transform(dataset)
train_data, test_data = data.randomSplit([0.7, 0.3])

# Create a RandomForestRegressor
rf = RandomForestRegressor(labelCol="quality")

# Train the model
model = rf.fit(train_data)

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
