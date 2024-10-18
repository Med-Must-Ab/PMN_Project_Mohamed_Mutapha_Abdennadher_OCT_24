  GNU nano 2.0.9                                                                                                               File: clean_bikeshops.py                                                                                                                                                                                                                                     

from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext

# Initialize Spark Configuration and Context
conf = SparkConf().setAppName("CleanBikeshops")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

# Load raw bikeshops data
df_bikeshops = sqlContext.read.format("com.databricks.spark.csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("delimiter", ";") \
    .load("hdfs://localhost/user/cloudera/raw_data/Project/bikeshops/")

for column in df_bikeshops.columns:
    new_column_name = column.replace(".", "_")
    df_bikeshops = df_bikeshops.withColumnRenamed(column, new_column_name)

df_bikeshops = df_bikeshops.toDF(*[col_name.strip().lower() for col_name in df_bikeshops.columns])


# Handle missing values: fill with default values where appropriate
df_bikeshops_clean = df_bikeshops.na.fill({
    "bikeshop_name": "Unknown Bikeshop",
    "bikeshop_state": "Unknown State",
    "bikeshop_city": "Unkown City",
    "bikeshop_id": 0,
    "latitude": 0,
    "longitude": 0
})

# Remove duplicates based on bikeshop.id
df_bikeshops_clean = df_bikeshops_clean.dropDuplicates(["bikeshop_id"])

# Save the cleaned data to HDFS
df_bikeshops_clean.write.mode("overwrite").parquet("hdfs://localhost/user/cloudera/clean_data/Project/bikeshops_parquet/")

sc.stop()
