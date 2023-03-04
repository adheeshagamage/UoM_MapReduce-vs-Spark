"""
The below Script calculates and diaplays Carrier delays delays happened in US for during the years of 2003 to 2010
"""


# Importing  Spark Seession and time modules
from pyspark.sql import SparkSession
import time

# Initiating SparkSession object
spark = SparkSession.builder.appName("Carrier Delay Query").getOrCreate()

# Getting data from DelayedFlights-updated.csv CSV file as a dataframe
#flightDelayData = spark.read.format("csv").option("header", "true").load("s3://sparkvsmapreduce/dataset/DelayedFlights-updated.csv")
flightDelayData = spark.read.option("header", "true").csv("s3://sparkvsmapreduce/dataset/DelayedFlights-updated.csv")

# Create an in-memory DataFrame to query
flightDelayData.createOrReplaceTempView("flight_delays")



#Executing and diplaying Yearwise carrier delay query with Execution time

start_time_carrier_delay = time.time()

spark.sql("""SELECT Year AS Year, avg((CarrierDelay /ArrDelay)*100) AS yearwise_carrier_delay from flight_delays GROUP BY Year ORDER BY Year""").show()

end_time_carrier_delay = time.time()
runtime_carrier_delay = end_time_carrier_delay - start_time_carrier_delay
print("runtime for carrier delay query is: ", runtime_carrier_delay, "s")
