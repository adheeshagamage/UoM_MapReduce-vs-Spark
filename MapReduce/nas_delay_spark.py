"""
The below Script calculates and diaplays yearwise nas delays happened in US for during the years of 2003 to 2010
"""


# Importing  Spark Seession and time modules
from pyspark.sql import SparkSession
import time

# Initiating SparkSession object
spark = SparkSession.builder.appName("FlightDelayData").getOrCreate()

# Getting data from DelayedFlights-updated.csv CSV file as a dataframe
#flightDelayData = spark.read.format("csv").option("header", "true").load("s3://sparkvsmapreduce/dataset/DelayedFlights-updated.csv")
flightDelayData = spark.read.option("header", "true").csv("s3://sparkvsmapreduce/dataset/DelayedFlights-updated.csv")

# Create an in-memory DataFrame to query
flightDelayData.createOrReplaceTempView("flight_delays")


#Executing and diplaying Yearwise NAS Delay query with Execution time

start_time_nas_delay = time.time()

spark.sql("""SELECT Year AS Year, avg((NASDelay/ArrDelay)*100) AS yearwise_nas_delay from flight_delays GROUP BY Year ORDER BY Year""").show()

end_time_nas_delay = time.time()
runtime_nas_delay = end_time_nas_delay - start_time_nas_delay
print("runtime for NAS delay query is: ", runtime_nas_delay, "s")
