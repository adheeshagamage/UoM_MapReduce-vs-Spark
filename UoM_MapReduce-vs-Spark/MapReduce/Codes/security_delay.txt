USE default;

CREATE EXTERNAL TABLE IF NOT EXISTS flight_delay(
  Id int,
  Year int,
  Month int,
  DayofMonth int,
  DayOfWeek int,
  DepTime int,
  CRSDepTime int,
  ArrTime int,
  CRSArrTime int,
  UniqueCarrier string,
  FlightNum int,
  TailNum string,
  ActualElapsedTime int,
  CRSElapsedTime int,
  AirTime int,
  ArrDelay int,
  DepDelay int,
  Origin string,
  Dest string,
  Distance int,
  TaxiIn int,
  TaxiOut int,
  Cancelled string,
  CancellationCode string,
  Diverted string,
  CarrierDelay int,
  WeatherDelay int,
  NASDelay int,
  SecurityDelay int,
  LateAircraftDelay int
)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ','
LOCATION 's3://sparkvsmapreduce/dataset/';

SET hive.cli.print.header=true;

SELECT Year As year, avg((SecurityDelay/ArrDelay)*100) AS yearwise_security_delay FROM flight_delay
WHERE Year >= 2003 AND Year <= 2010
GROUP BY Year ORDER BY Year;