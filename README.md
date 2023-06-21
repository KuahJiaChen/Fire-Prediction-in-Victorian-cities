# Fire Prediction in Victorian Cities

Author: Kuah Jia Chen

## Background
The StopFire campaign initiated by Monash University aims to predict and prevent fires in Victorian cities. They have deployed sensors in various cities of Victoria, collecting a large volume of data. However, due to the data's size, their existing techniques have failed to provide timely fire predictions. As data analysts, we have been hired to migrate their data to a NoSQL database (MongoDB) and analyze it. Additionally, we are tasked with developing an application using Apache Kafka, Apache Spark Streaming, and MongoDB to facilitate data streaming, storage, and analysis.

## Provided Dataset
The campaign has provided us with the following datasets:
- hotspot_historic.csv
- climate_historic.csv
- hotspot_AQUA_streaming.csv
- hotspot_TERRA_streaming.csv
- climate_streaming.csv

## Dataset Information
Climate data is recorded daily, while fire data is based on fire occurrences on specific days. Therefore, for each climate data entry, there can be zero or multiple fire data entries. The climate data represents average values for each day, except for the maximum wind speed.

The data is not organized on a per-weather-station basis. For example, a station may report data for a certain number of days, and then another station starts reporting data if the first station shuts down. 

Global Horizontal Irradiance (GHI) refers to the total solar radiation incident on a horizontal surface.

Total precipitation is reported in inches and hundredths and represents rain and/or melted snow during the day. It may include a portion of the previous day. A value of '.00' indicates no measurable precipitation, while 'Missing = 99.99' signifies missing data.

Note: Stations often do not report '0' on days with no precipitation, resulting in '99.99' appearing instead. Different flags indicate the reporting frequency for precipitation amounts over specific time intervals.

## File Description

### 1. MongoDB_with_pymongo.ipynb
This notebook reads data from CSV files, builds a model, saves it to MongoDB, and executes various queries against the database.

### 2. Producer1.ipynb
This notebook contains a Python program that loads data from climate_streaming.csv and randomly feeds it (with replacement) to the stream every 10 seconds.

### 3. Producer2.ipynb
This notebook includes a Python program that loads data from hotspot_AQUA_streaming.csv and randomly feeds it (with replacement) to the stream every 2 seconds. AQUA is a satellite from NASA that reports latitude, longitude, confidence, and surface temperature of a location.

### 4. Producer3.ipynb
This notebook consists of a Python program that loads data from hotspot_TERRA_streaming.csv and randomly feeds it (with replacement) to the stream every 2 seconds. TERRA is another satellite from NASA that reports latitude, longitude, confidence, and surface temperature of a location.

### 5. Streaming_Application.ipynb
This notebook presents a streaming application using the Apache Spark Structured Streaming API. It processes data in batches of 10 seconds and receives streaming data from all three producers, applying specific processing steps.

### 6. Data_Visualisation.ipynb
This notebook contains Python programs for streaming data visualization of incoming climate data. It also uses pymongo to retrieve data from the MongoDB collection(s) created in Streaming_Application.ipynb and perform static visualizations.

## Conclusion

In this project, we have addressed the challenge of predicting and preventing fires in Victorian cities. By leveraging Python, various Python libraries, and technologies like Apache Kafka, Apache Spark Streaming, and MongoDB, we have analyzed a large volume of data collected from sensors deployed across different cities in Victoria.

Through data migration, storage, and analysis, we have gained insights into the relationship between climate data and fire occurrences. This information can help in developing strategies to mitigate fire risks and enhance fire prevention measures.

By building a streaming application and visualizing the data, we have demonstrated the potential of real-time data analysis for prompt decision-making and early fire detection.

The integration of NoSQL databases and streaming technologies has proven to be effective in handling large-scale datasets and facilitating efficient data processing for fire prediction.

With further research and development, the findings from this project can contribute to the improvement of fire prediction models and the implementation of proactive measures to protect Victorian cities from the devastating impact of fires.

We hope that the outcomes of this project will assist the StopFire campaign in achieving their goal of predicting and stopping fires, making Victorian cities safer for the community.
