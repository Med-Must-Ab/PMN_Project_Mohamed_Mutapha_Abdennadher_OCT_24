from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import split, concat_ws, year, month, dayofmonth, date_format, quarter, from_unixtime, unix_timestamp
import datetime
from pyspark.sql import Row

# Initialize Spark
conf = SparkConf().setAppName("FixDateFormatsAndCreateCalendar")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

# Load the cleaned orders data from Parquet
df_orders = sqlContext.read.parquet("hdfs://localhost/user/cloudera/clean_data/Project/orders_parquet/")

# Convert order_date into string to handle various date formats
df_orders = df_orders.withColumn("order_date_str", df_orders["order_date"].cast("string"))

# Handle both M/d/yyyy and MM/dd/yyyy formats using split
df_orders = df_orders.withColumn("date_parts", split(df_orders["order_date_str"], "/"))

# Extract month, day, and year from the split result
df_orders = df_orders.withColumn("month", df_orders["date_parts"].getItem(0)) \
                     .withColumn("day", df_orders["date_parts"].getItem(1)) \
                     .withColumn("year", df_orders["date_parts"].getItem(2))

# Combine the components into a proper date format yyyy-MM-dd
df_orders = df_orders.withColumn("formatted_order_date", concat_ws("-", df_orders["year"], df_orders["month"], df_orders["day"]))

# Convert 'formatted_order_date' into a proper date type
df_orders = df_orders.withColumn("formatted_order_date", df_orders["formatted_order_date"].cast("date"))


# Apply the transformation to have OrderDate in the correct format
df_orders = df_orders.withColumn(
    "OrderDate",
    from_unixtime(unix_timestamp(df_orders["order_date_str"], "MM/dd/yyyy"), "yyyy-MM-dd")
)

# Get the minimum and maximum dates
min_date = df_orders.agg({"formatted_order_date": "min"}).collect()[0][0]
max_date = df_orders.agg({"formatted_order_date": "max"}).collect()[0][0]

# Print the results
print("Min order date: %s" % min_date)
print("Max order date: %s" % max_date)

# Step 2: Create Calendar Lookup Table

# Function to generate a range of dates between start and end dates
def generate_date_range(start_date, end_date):
    start = datetime.datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.datetime.strptime(end_date, "%Y-%m-%d")
    step = datetime.timedelta(days=1)
    dates = []
    while start <= end:
        dates.append(start)
        start += step
    return dates

# Convert min_date and max_date to strings to use them in the date range
min_date_str = str(min_date)
max_date_str = str(max_date)

# Generate date range between min and max order dates
date_range = generate_date_range(min_date_str, max_date_str)
# Convert date range into Spark DataFrame
rows = [Row(date=d.strftime("%Y-%m-%d")) for d in date_range]
df_calendar = sqlContext.createDataFrame(rows)

# Add calendar fields: year, month, day, day of week, quarter
df_calendar = df_calendar.withColumn("year", year(df_calendar["date"])) \
                         .withColumn("month", month(df_calendar["date"])) \
                         .withColumn("day", dayofmonth(df_calendar["date"])) \
                         .withColumn("day_of_week", date_format(df_calendar["date"], "EEEE")) \
                         .withColumn("quarter", quarter(df_calendar["date"]))

# Show the schema and first few rows of the generated calendar
df_calendar.printSchema()
df_calendar.show(5, truncate=False)

# Save the calendar DataFrame to HDFS as Parquet
df_calendar.write.mode("overwrite").parquet("hdfs://localhost/user/cloudera/modeled_data/Project/calendar_lookup.parquet")

# Stop the Spark session
sc.stop()



