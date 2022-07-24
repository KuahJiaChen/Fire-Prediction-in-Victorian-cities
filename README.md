# Fire-Prediction-in-Victorian-cities
## Background
#
StopFire is a campaign started by Monash University to predict and stop the fire in
Victorian cities. They have employed sensors in different cities of Victoria and have collected a large amount of data. The data is so big that their techniques have failed to provide the results on time to predict fire. They have hired us as the data analyst to migrate their data to the NoSQL database (MongoDB). They want us to analyze their data and provide them with results. In addition, they want us to build an application, a complete setup from streaming to storing and analyzing the data for them using Apache Kafka, Apache Spark Streaming, and MongoDB.

## Provided Dataset
#
- hotspot_historic.csv
- climate_historic.csv
- hotspot_AQUA_streaming.csv
- hotspot_TERRA_streaming.csv
- climate_streaming.csv

## Information on Dataset
#
Climate data is recorded on a daily basis whereas Fire data is recorded based on the
occurrence of a fire on a particular day. Therefore, for one climate data, there can be zero or many fire data. All climate data is an average value for the particular day except for max wind speed.

The data is NOT row per weather station basis. You can simply think of it as, Station 1 was reporting data for X number of days and then Station 2 started reporting data because Station 1 was shut down for instance.

Global Horizontal Irradiance (GHI) is the total solar radiation incident on a horizontal surface.

<b> Total precipitation (rain and/or melted snow) </b> reported during the day in inches and hundredths; will usually not end with the midnight observation --i.e., may include the latter part of the previous day.

<b>.00</b> indicates no measurable precipitation (includes a trace). <b> Missing = 99.99 </b> (For metric version, units = millimeters to tenths & missing = 999.9.)

Note: Many stations do not report '0' on days with no precipitation --therefore, '99.99' will often appear on these days. Also, for example, a station may only report a 6-hour amount for the period during which rain fell. See Flag field information below the source of data.

- A = 1 report of 6-hour precipitation amount.
- B = Summation of 2 reports of 6-hour precipitation amount.
- C = Summation of 3 reports of 6-hour precipitation amount.
- D = Summation of 4 reports of 6-hour precipitation amount.
- E = 1 report of 12-hour precipitation amount.
- F = Summation of 2 reports of 12-hour precipitation amount.
- G = 1 report of 24-hour precipitation amount.
- H = Station reported '0' as the amount for the day (eg, from 6-hour reports), but also reported at least one occurrence of precipitation in hourly observations --this could indicate a trace occurred but should be considered as incomplete data for the day.
- I = Station did not report any precipitation data for the day and did not report any occurrences of precipitation in its hourly observations --it's still possible that precipitation occurred but was not reported.

## File Description
#
### 1. MongoDB_with_pymongo.ipynb
This file consists of reading data from CSV files, building a model, saving to MongoDB and running various queries against the database.

### 2. Producer1.ipynb
This file consists of a python program that loads all the data from climate_streaming.csv and randomly (with replacement) feeds the data to the stream every 10 seconds. 

### 3. Producer2.ipynb
This file consists of a python program that loads all the data from hotspot_AQUA_streaming.csv and randomly (with replacement) feeds the data to the stream every 2 seconds. AQUA is the satellite from NASA that reports the latitude, longitude, confidence, and surface temperature of a location

### 4. Producer3.ipynb
This file consists of a python program that loads all the data from hotspot_TERRA_streaming.csv and randomly (with replacement) feeds the data to the stream every 2 seconds. TERRA is another satellite from NASA that reports the latitude, longitude, confidence, and surface temperature of a location.

### 5. Streaming_Application.ipynb
This file consists of a streaming application using the Apache Spark Structured Streaming API which processes data in batches of 10 seconds. The streaming application will receive streaming data from all three producers and processes it in a specific way.

### 6. Data_Visualisation.ipynb
This file consists of python programs that use python to create streaming data visualization for incoming climate data and use pymongo to get the data from the MongoDB collection(s) created in Streaming_Application.ipynb and perform the static visualizations.