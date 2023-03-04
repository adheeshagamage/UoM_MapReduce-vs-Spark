"""
The below Script calculates and diaplays delays happened in US for during the years of 2003 to 2010
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



#Executing and diplaying Yearwise carrier delay query with Execution time

start_time_carrier_delay = time.time()

#spark.sql("""SELECT Year As Year, SUM(CarrierDelay) As TotalCarrierDelay FROM  WHERE Year >= 2003 AND Year <= 2010 GROUP BY Year ORDER BY Year ASC""").show()
spark.sql("""SELECT Year AS Year, avg((CarrierDelay /ArrDelay)*100) AS yearwise_carrier_delay from flight_delays GROUP BY Year ORDER BY Year""").show()

end_time_carrier_delay = time.time()
runtime_carrier_delay = end_time_carrier_delay - start_time_carrier_delay
print("runtime for carrier delay query is: ", runtime_carrier_delay, "s")



#Executing and diplaying Yearwise NAS Delay query with Execution time

start_time_nas_delay = time.time()

#spark.sql("""SELECT Year As Year, SUM(NASDelay) As TotalNASDelay FROM FlightDelay WHERE Year >= 2003 AND Year <= 2010 GROUP BY Year ORDER BY Year ASC""").show()
spark.sql("""SELECT Year AS Year, avg((NASDelay/ArrDelay)*100) AS yearwise_nas_delay from flight_delays GROUP BY Year ORDER BY Year""").show()

end_time_nas_delay = time.time()
runtime_nas_delay = end_time_nas_delay - start_time_nas_delay
print("runtime for NAS delay query is: ", runtime_nas_delay, "s")



#Executing and diplaying Yearwise Weather Delay query with Execution time

start_time_weather_delay = time.time()

#spark.sql("""SELECT Year As Year, SUM(WeatherDelay) As TotalWeatherDelay FROM FlightDelay WHERE Year >= 2003 AND Year <= 2010 GROUP BY Year ORDER BY Year""").show()
spark.sql("""SELECT Year AS Year, avg((WeatherDelay/ArrDelay)*100) AS yearwise_weather_delay from flight_delays GROUP BY Year ORDER BY Year""").show()

end_time_weather_delay = time.time()
runtime_weather_delay = end_time_weather_delay - start_time_weather_delay
print("runtime for Weather Delay query is: ", runtime_weather_delay, "s")



#Executing and diplaying Yearwise Late Aircarft Delay query with Execution time

start_time_laircraft_delay = time.time()

#spark.sql("""SELECT Year As Year, SUM(LateAircraftDelay) As TotalLateAircraftDelay FROM FlightDelay WHERE Year >= 2003 AND Year <= 2010 GROUP BY Year ORDER BY Year ASC""").show()
spark.sql("""SELECT Year AS Year, avg((LateAircraftDelay/ArrDelay)*100) AS yearwise_weather_delay from flight_delays GROUP BY Year ORDER BY Year""").show()

end_time_laircraft_delay = time.time()
runtime_laircraft_delay = end_time_laircraft_delay - start_time_laircraft_delay
print("runtime for Late Aircarft Delay query is: ", runtime_laircraft_delay, "s")



#Executing and diplaying Yearwise Security Delay query with Execution time

start_time_security_delay = time.time()

#spark.sql("""SELECT Year As Year, SUM(SecurityDelay) As TotalSecurityDelay FROM FlightDelay WHERE Year >= 2003 AND Year <= 2010 GROUP BY Year ORDER BY Year ASC""").show()
spark.sql("""SELECT Year AS Year, avg((SecurityDelay/ArrDelay)*100) AS yearwise_weather_delay from flight_delays GROUP BY Year ORDER BY Year""").show()

end_time_security_delay = time.time()
runtime_security_delay = end_time_security_delay - start_time_security_delay
print("runtime for Security Delay query is: ", runtime_security_delay, "s")
