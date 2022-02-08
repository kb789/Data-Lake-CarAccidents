# A Data Lake to analyze U.S. Car Accident and Population Data in 2016-2017 
## Project Overview

In this project, PySpark on AWS EMR was used to clean, model, and pipeline data from two large datasets: US Accidents: A Countrywide Traffic Accident Dataset and  County-Level-Data-USDA-Economic-Research-Service.


## Data Pipeline

1. Start a Spark Session
2. Read in the population csv file.
3. Create the Population table, adjust the datatypes as needed to match the data dictionary, drop unneeded columns, save it to a parquet file.
4. Read in the accident csv file.
5. Create the Weather table.
6. Create the Location table.
7. Create the Time table.
8. Create the Accident Detail table.
9. Load the Population table from the parquet file.
10. Join the Population Table with the Location table on state name and county name to create Population-Location dataframe
11. Create Accident Detail Table 2016_2017 by filtering using the Time Table
12.  Join the Accident Detail Table 2016_2017 Table with the Population-Location Dataframe on Latitude and Longitude to create final Accident_Detail_2016_2017 table.
13. Save the Weather, Location, Time and Accident_Detail_2016_2017 tables as parquet files

