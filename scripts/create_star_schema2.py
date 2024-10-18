from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext

# Initialize Spark Configuration and Context
conf = SparkConf().setAppName("CreateStarSchema")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

# Load cleaned data from Parquet format
customers_df = sqlContext.read.parquet("hdfs://localhost/user/cloudera/clean_data/Project/Customers_parquet/")
bikes_df = sqlContext.read.parquet("hdfs://localhost/user/cloudera/clean_data/Project/bikes_parquet/")
bikeshops_df = sqlContext.read.parquet("hdfs://localhost/user/cloudera/clean_data/Project/bikeshops_parquet/")
orders_df = sqlContext.read.parquet("hdfs://localhost/user/cloudera/clean_data/Project/orders_parquet/")

# Create Dimension Tables with all relevant columns
customers_lookup = customers_df.select(
    "CustomerKey",
    "Prefix",
    "FirstName",
    "LastName",
    "BirthDate",
    "MaritalStatus",
    "Gender",
    "EmailAddress",
    "AnnualIncome",
    "TotalChildren",
    "EducationLevel",
    "Occupation",
    "HomeOwner"
)

bikes_lookup = bikes_df.select(
    "bike_id",
    "model",
    "category1",
    "category2",
    "frame",
    "price"
)

bikeshops_lookup = bikeshops_df.select(
    "bikeshop_id",
    "bikeshop_name",
    "bikeshop_city",
    "bikeshop_state",
    "latitude",
    "longitude"
)

# Create Fact Table by joining orders with customers and bikes
fact_orders = orders_df.join(customers_lookup, orders_df["customer_id"] == customers_lookup["CustomerKey"], how="inner") \
                       .join(bikes_lookup, orders_df["product_id"] == bikes_lookup["bike_id"], how="inner") \
                       .select(
                           orders_df["order_id"].alias("OrderID"),
                           orders_df["order_date"].alias("OrderDate"),
                           customers_lookup["CustomerKey"].alias("CustomerID"),
                           customers_lookup["FirstName"],
                           customers_lookup["LastName"],
                           customers_lookup["EmailAddress"],
                           bikes_lookup["bike_id"].alias("BikeID"),
                           bikes_lookup["model"],
                           bikes_lookup["category1"],
                           bikes_lookup["category2"],
                           bikes_lookup["frame"],
                           bikes_lookup["price"],
                           orders_df["quantity"]
                       )

# Save the dimension and fact tables in Parquet format
customers_lookup.write.mode("overwrite").parquet("hdfs://localhost/user/cloudera/modeled_data/Project/customers_lookup.parquet")
bikes_lookup.write.mode("overwrite").parquet("hdfs://localhost/user/cloudera/modeled_data/Project/bikes_lookup.parquet")
bikeshops_lookup.write.mode("overwrite").parquet("hdfs://localhost/user/cloudera/modeled_data/Project/bikeshops_lookup.parquet")
fact_orders.write.mode("overwrite").parquet("hdfs://localhost/user/cloudera/modeled_data/Project/fact_orders.parquet")

sc.stop()
