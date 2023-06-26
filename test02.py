from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions

if __name__ == "__main__":
    # Create a SparkSession
    # Creating a SparkSession
spark = SparkSession.builder.appName("WineQualityPrediction").getOrCreate()

# Reading the "quality" data from MongoDB into a DataFrame
quality_data = spark.read \
    .format("com.mongodb.spark.sql.DefaultSource") \
    .option("uri", "mongodb://127.0.0.1/wine_db.quality") \
    .load()

# Selecting relevant columns for prediction
selected_columns = ["fixed acidity", "volatile acidity", "citric acid", "residual sugar",
                    "chlorides", "free sulfur dioxide", "total sulfur dioxide", "density",
                    "pH", "sulphates", "alcohol", "quality"]
data = quality_data.select(selected_columns)

# Creating a feature vector by combining the input variables
assembler = VectorAssembler(inputCols=["fixed acidity", "volatile acidity", "citric acid",
                                       "residual sugar", "chlorides", "free sulfur dioxide",
                                       "total sulfur dioxide", "density", "pH", "sulphates",
                                       "alcohol"], outputCol="features")
data = assembler.transform(data)

# Splitting the data into training and testing sets
train_data, test_data = data.randomSplit([0.8, 0.2], seed=42)

# Creating a Linear Regression model
lr = LinearRegression(featuresCol="features", labelCol="quality")

# Training the model
model = lr.fit(train_data)

# Making predictions on the test data
predictions = model.transform(test_data)

# Displaying the predicted quality and actual quality side by side
predictions.select("prediction", "quality").show()

# Importing necessary libraries
from pyspark.ml.evaluation import RegressionEvaluator

# Evaluating the model
evaluator = RegressionEvaluator(labelCol="quality", predictionCol="prediction", metricName="rmse")
rmse = evaluator.evaluate(predictions)
print("Root Mean Squared Error (RMSE):", rmse)

# Closing the SparkSession
spark.stop()

