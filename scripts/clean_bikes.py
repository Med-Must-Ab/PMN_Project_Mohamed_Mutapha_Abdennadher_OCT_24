  GNU nano 2.0.9                                                                                                                File: clean_bikes.py                                                                                                                                                                                                                                        

from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import col


# Initialize Spark Configuration and Context
conf = SparkConf().setAppName("CleanBikes")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

# Load raw bikes data
df_bikes = sqlContext.read.format("com.databricks.spark.csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("delimiter", ";") \
    .load("hdfs://localhost/user/cloudera/raw_data/Project/bikes/")

df_bikes = df_bikes.toDF(*[col_name.strip().lower() for col_name in df_bikes.columns])


for column in df_bikes.columns:
    new_column_name = column.replace(".", "_")
    df_bikes = df_bikes.withColumnRenamed(column, new_column_name)

# Convert 'price' column to INT to avoid schema incompatibility
df_bikes = df_bikes.withColumn("price", col("price").cast("int"))


# Handle missing values: fill with default values where appropriate
df_bikes_clean = df_bikes.na.fill({
    "model": "Unknown Model",
    "category1": "Unknown Category",
    "category2": "Unknown Category",
    "frame": "Unknown Frame",
    "price": 0
})

# Remove duplicates based on bike.id
df_bikes_clean = df_bikes_clean.dropDuplicates(["bike_id"])

# Save the cleaned data to HDFS
df_bikes_clean.write.mode("overwrite").parquet("hdfs://localhost/user/cloudera/clean_data/Project/bikes_parquet/")

sc.stop()
