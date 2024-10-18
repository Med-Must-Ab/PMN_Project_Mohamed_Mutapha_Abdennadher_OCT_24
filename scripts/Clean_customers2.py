from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext

# Initialize Spark Configuration and Context
conf = SparkConf().setAppName("CleanCustomers")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

# Load raw customers data
df_customers = sqlContext.read.format("com.databricks.spark.csv") \
    .option("header", "false") \
    .option("inferSchema", "true") \
    .load("hdfs://localhost/user/cloudera/raw_data/Project/Customers/")

# Inspect Data
print("Original Data:")
df_customers.show(5, truncate=False)

# Remove the first row if it contains the header
header_row = df_customers.first()  # Get the first row
df_customers = df_customers.filter(df_customers['C0'] != header_row['C0'])  # Filter out the header row

# Rename columns based on the original header row
columns = [header_row['C' + str(i)].strip() for i in range(len(header_row))]
df_customers = df_customers.toDF(*columns)

# Show the DataFrame with proper column names
print("Data after renaming columns:")
df_customers.show(5, truncate=False)

# Duplicates and missing values cleaning
df_customers_clean = df_customers.dropna(subset=["CustomerKey", "FirstName", "LastName"]).dropDuplicates(["CustomerKey"])

# Debug: Show the data after cleaning
print("Data after cleaning:")
df_customers_clean.show(5, truncate=False)
print("Number of rows after cleaning: %d" % df_customers_clean.count())

df_customers.show(5, truncate=False)

# Save the cleaned data to HDFS in Parquet format
customers_clean.write.mode("overwrite").parquet("hdfs://localhost/user/cloudera/clean_data/Project/Customers_parquet/")



sc.stop()
