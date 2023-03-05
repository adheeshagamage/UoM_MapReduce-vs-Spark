"""
The below Script calculates and diaplays yearwise security delays happened in US for during the years of 2003 to 2010
"""


# Importing  Spark Seession and time modules
from pyspark.sql import SparkSession
import time

# Initiating SparkSession object
spark = SparkSession.builder.appName("FlightDelayData").getOrCreate()

# Getting data from DelayedFlights-updated.csv CSV file as a dataframe
flightDelayData = spark.read.option("header", "true").csv("s3://sparkvsmapreduce/dataset/DelayedFlights-updated.csv")

# Create an in-memory DataFrame to query
flightDelayData.createOrReplaceTempView("flight_delays")

#Executing and diplaying Yearwise Security Delay query with Execution time

start_time_security_delay = time.time()
result_security = spark.sql("""SELECT Year AS Year, avg((SecurityDelay/ArrDelay)*100) AS yearwise_security_delay from flight_delays GROUP BY Year""")
end_time_security_delay = time.time()

result_security.show()

runtime_security_delay = end_time_security_delay - start_time_security_delay
print("runtime for Security Delay query is: ", runtime_security_delay, "s")
