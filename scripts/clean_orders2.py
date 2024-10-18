from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import col, trim, unix_timestamp, from_unixtime, concat_ws, split

# Initialize Spark Configuration and Context
conf = SparkConf().setAppName("CleanOrders")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

# Load raw orders data
df_orders = sqlContext.read.format("com.databricks.spark.csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("delimiter", ";") \
    .load("hdfs://localhost/user/cloudera/raw_data/Project/orders/")

# Manually rename all columns for consistency
new_column_names = ["Unnamed_Column", "order.id", "order.line", "order.date", "customer.id", "product.id", "quantity"]

# Ensure the number of columns matches
if len(df_orders.columns) == len(new_column_names):
    for old_name, new_name in zip(df_orders.columns, new_column_names):
        df_orders = df_orders.withColumnRenamed(old_name, new_name)
else:
    print("Error: The number of columns in the DataFrame does not match the number of new column names.")
    sc.stop()
    exit(1)

# Standardize column names (replace '.' with '_')
df_orders = df_orders.toDF(*[col_name.strip().lower().replace(".", "_") for col_name in df_orders.columns])

df_orders.printSchema()  # Show the schema
df_orders.show(5, truncate=False)  # Inspect the data

# Handle missing values: drop rows where order.id, customer.id, or product.id is missing
df_orders_clean = df_orders.dropna(subset=["order_id", "customer_id", "product_id"])

# Remove duplicates based on order.id and order.line
df_orders_clean = df_orders_clean.dropDuplicates(["order_id", "order_line"])

# Trim the order_date column to ensure no extra spaces
df_orders_clean = df_orders_clean.withColumn("order_date", trim(col("order_date")))

# Use `unix_timestamp` to convert the order date into a standard format (yyyy-MM-dd)
# This handles both `M/d/yyyy` and `MM/dd/yyyy`
df_orders_clean = df_orders_clean.withColumn(
    "order_date_formatted",
    from_unixtime(unix_timestamp(col("order_date"), "MM/dd/yyyy"), "yyyy-MM-dd")
)


# Drop the old 'order_date' column
df_orders_clean = df_orders_clean.drop("order_date")

# Rename 'order_date_formatted' to 'order_date'
df_orders_clean = df_orders_clean.withColumnRenamed("order_date_formatted", "order_date")


# Show the cleaned data (you will see both original and formatted date)
df_orders_clean.show(50, truncate=False)

# Save the cleaned data with formatted date to HDFS
df_orders_clean.write.mode("overwrite").parquet("hdfs://localhost/user/cloudera/clean_data/Project/orders_parquet/")

# Stop Spark session
sc.stop()
