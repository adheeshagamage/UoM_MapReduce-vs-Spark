"""
The below Script calculates and diaplays delays happened in US for during the years of 2003 to 2010
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


#Executing and diplaying Yearwise carrier delay query with Execution time

start_time_carrier_delay = time.time()
result_carrier = spark.sql("""SELECT Year AS Year, avg((CarrierDelay /ArrDelay)*100) AS yearwise_carrier_delay from flight_delays GROUP BY Year""")
end_time_carrier_delay = time.time()

result_carrier.show()

runtime_carrier_delay = end_time_carrier_delay - start_time_carrier_delay
print("runtime for carrier delay query is: ", runtime_carrier_delay, "s")



#Executing and diplaying Yearwise NAS Delay query with Execution time

start_time_nas_delay = time.time()
result_nas = spark.sql("""SELECT Year AS Year, avg((NASDelay/ArrDelay)*100) AS yearwise_nas_delay from flight_delays GROUP BY Year""")
end_time_nas_delay = time.time()

result_nas.show()

runtime_nas_delay = end_time_nas_delay - start_time_nas_delay
print("runtime for NAS delay query is: ", runtime_nas_delay, "s")



#Executing and diplaying Yearwise Weather Delay query with Execution time

start_time_weather_delay = time.time()
result_weather = spark.sql("""SELECT Year AS Year, avg((WeatherDelay/ArrDelay)*100) AS yearwise_weather_delay from flight_delays GROUP BY Year""")
end_time_weather_delay = time.time()

result_weather.show()

runtime_weather_delay = end_time_weather_delay - start_time_weather_delay
print("runtime for Weather Delay query is: ", runtime_weather_delay, "s")


#Executing and diplaying Yearwise Late Aircarft Delay query with Execution time

start_time_laircraft_delay = time.time()
result_laircraft = spark.sql("""SELECT Year AS Year, avg((LateAircraftDelay/ArrDelay)*100) AS yearwise_late_aircraft_delay from flight_delays GROUP BY Year""")
end_time_laircraft_delay = time.time()

result_laircraft.show()

runtime_laircraft_delay = end_time_laircraft_delay - start_time_laircraft_delay
print("runtime for Late Aircarft Delay query is: ", runtime_laircraft_delay, "s")



#Executing and diplaying Yearwise Security Delay query with Execution time

start_time_security_delay = time.time()
result_security = spark.sql("""SELECT Year AS Year, avg((SecurityDelay/ArrDelay)*100) AS yearwise_security_delay from flight_delays GROUP BY Year""")
end_time_security_delay = time.time()

result_security.show()

runtime_security_delay = end_time_security_delay - start_time_security_delay
print("runtime for Security Delay query is: ", runtime_security_delay, "s")
