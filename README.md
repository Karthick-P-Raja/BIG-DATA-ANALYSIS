# BIG-DATA-ANALYSIS

"COMPANY NAME": CODETECH IT SOLUTIONS

"NAME": KARTHICK RAJA P

"INTERN ID": CT04WY05

"DOMAIN": DATA ANALYSIS

"DURATION": 4 WEEKS

"MENTOR": NEEELA SANTHOSH

DESCRIPTION

The given code performs big data analysis on the New York City Taxi Trip dataset using PySpark in a Jupyter Notebook environment. The script follows a structured approach to load, clean, analyze, visualize, and save the dataset.

🔹 Step 1: Initialize PySpark and Load Data

The script first initializes a Spark session and loads the dataset from a local file system.

Code Breakdown:

from pyspark.sql import SparkSession

# Create Spark session
spark = SparkSession.builder.master("local").appName("NYC_Taxi_Analysis").getOrCreate()

# Load CSV file
df = spark.read.csv("file:///Users/karthickraja/Downloads/prj.csv", header=True, inferSchema=True)

# Display schema and basic statistics
df.printSchema()  
df.describe().show()
df.columns
df.count()
✔ Spark Session: Required to use PySpark.
✔ Loading Data: Reads a CSV file with header=True (column names) and inferSchema=True (detects data types).
✔ Schema Inspection: printSchema() and describe() display column types and basic statistics.

🔹 Step 2: Data Cleaning

The script removes null values and duplicate rows to ensure data quality.

Code Breakdown:

df = df.dropna()  # Remove missing values
df = df.dropDuplicates()  # Remove duplicate rows
✔ dropna(): Eliminates rows with missing values.
✔ dropDuplicates(): Removes duplicate records for consistency.

🔹 Step 3: Convert Date-Time Columns

Since timestamps are often stored as strings, they are converted into timestamp format for proper time-based calculations.

Code Breakdown:

from pyspark.sql.functions import col, to_timestamp

df = df.withColumn("pickup_datetime", to_timestamp(col("pickup_datetime")))
df = df.withColumn("dropoff_datetime", to_timestamp(col("dropoff_datetime")))
df.printSchema()
✔ to_timestamp(): Converts string-based timestamps into proper timestamp objects.
✔ Now, pickup_datetime and dropoff_datetime are in the correct format.

🔹 Step 4: Compute Trip Duration

The trip duration is calculated in minutes by subtracting the pickup_datetime from the dropoff_datetime.

Code Breakdown:

from pyspark.sql.functions import unix_timestamp

df = df.withColumn("trip_duration_min", 
    (unix_timestamp(col("dropoff_datetime")) - unix_timestamp(col("pickup_datetime"))) / 60)

df.select("trip_duration", "trip_duration_min").show(5)
✔ unix_timestamp(): Converts timestamps into **






You said:
minimum 500 words as passage
ChatGPT said:
Big Data Analysis on NYC Taxi Dataset Using PySpark – A Comprehensive Description
This project focuses on analyzing a large dataset of New York City taxi trips using PySpark in a Jupyter Notebook environment. The goal is to load, clean, process, analyze, visualize, and store the dataset efficiently. PySpark is used because it enables distributed computing, making it ideal for handling large datasets that traditional tools like Pandas cannot process efficiently.

Step 1: Initializing PySpark and Loading Data

The first step in this project is to set up a Spark session, which is necessary to use PySpark functionalities. The dataset is stored as a CSV file and is loaded into a PySpark DataFrame.

Code Implementation:

from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder.master("local").appName("NYC_Taxi_Analysis").getOrCreate()

# Load dataset from local file system
df = spark.read.csv("file:///Users/karthickraja/Downloads/prj.csv", header=True, inferSchema=True)

# Display dataset schema and summary statistics
df.printSchema()  
df.describe().show()
df.columns
df.count()
✔ Spark Session: Required to use PySpark in Jupyter Notebook.
✔ Loading Data: The dataset is read using header=True (first row as column names) and inferSchema=True (automatic type detection).
✔ Data Inspection: printSchema() displays column names and data types, while describe().show() provides summary statistics like mean, min, max, and standard deviation.

Step 2: Cleaning the Data

Data cleaning is crucial to ensure accuracy. The dataset may contain null values, duplicate records, or improperly formatted data.

Code Implementation:

df = df.dropna()  # Remove missing values
df = df.dropDuplicates()  # Remove duplicate rows
✔ Handling Missing Data: dropna() removes any rows with null values.
✔ Removing Duplicates: dropDuplicates() eliminates identical rows to ensure unique records.

Step 3: Converting Timestamps for Analysis

The dataset contains columns like pickup_datetime and dropoff_datetime, which are initially stored as strings. These columns must be converted into timestamp format to enable time-based computations.

Code Implementation:

from pyspark.sql.functions import col, to_timestamp

df = df.withColumn("pickup_datetime", to_timestamp(col("pickup_datetime")))
df = df.withColumn("dropoff_datetime", to_timestamp(col("dropoff_datetime")))
df.printSchema()
✔ to_timestamp() Conversion: Converts text-based dates into actual timestamps for accurate time-based calculations.
✔ Now, pickup_datetime and dropoff_datetime are in the correct format.

Step 4: Calculating Trip Duration

Trip duration is a critical metric in this dataset. It is calculated in minutes by subtracting the pickup_datetime from the dropoff_datetime.

Code Implementation:

from pyspark.sql.functions import unix_timestamp

df = df.withColumn("trip_duration_min", 
    (unix_timestamp(col("dropoff_datetime")) - unix_timestamp(col("pickup_datetime"))) / 60)

df.select("trip_duration", "trip_duration_min").show(5)
✔ unix_timestamp() Conversion: Converts timestamps into seconds for mathematical operations.
✔ Trip Duration in Minutes: The difference between dropoff_datetime and pickup_datetime is divided by 60 to get the duration in minutes.

Step 5: Exploratory Data Analysis (EDA)

EDA helps us understand trends and patterns in the dataset. The code performs group-by analysis on various columns.

Analyzing Passenger Count
The number of passengers in each ride is analyzed to determine common taxi occupancy patterns.

Code Implementation:

df.groupBy("passenger_count").count().orderBy("count", ascending=False).show()
✔ Insight: Helps identify the most common passenger group sizes.

Analyzing Average Trip Duration by Vendor
Each ride is associated with a vendor ID, representing the taxi company. We analyze the average trip duration for each vendor.

Code Implementation:

df.groupBy("vendor_id").avg("trip_duration_min").show()
✔ Insight: Allows comparison between different taxi companies.

Analyzing Trip Frequency by Hour of the Day
We extract the hour of the day from pickup_datetime to see when taxis are most active.

Code Implementation:

from pyspark.sql.functions import hour

df = df.withColumn("hour_of_day", hour(col("pickup_datetime")))
df.groupBy("hour_of_day").count().orderBy("hour_of_day").show()
✔ Insight: Helps identify peak hours for taxi usage.

Step 6: Data Visualization

To better understand the data, we create visualizations using Pandas, Matplotlib, and Seaborn.

Boxplot of Trip Duration by Hour
This visualization shows how trip durations vary at different times of the day.

Code Implementation:

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

df_pd = df.select("hour_of_day", "trip_duration_min").toPandas()

plt.figure(figsize=(10, 5))
sns.boxplot(x="hour_of_day", y="trip_duration_min", data=df_pd)
plt.title("Trip Duration by Hour of Day")
plt.xlabel("Hour of Day")
plt.ylabel("Trip Duration (min)")
plt.show()
✔ Insight: Helps identify when long and short trips are more frequent.

Histogram of Trip Durations
A histogram provides a distribution of trip durations.

Code Implementation:

plt.figure(figsize=(10, 5))
sns.histplot(df_pd["trip_duration_min"], bins=50, kde=True)
plt.xlim(0, 100)
plt.title("Distribution of Trip Durations")
plt.xlabel("Trip Duration (minutes)")
plt.ylabel("Count of Trips")
plt.show()
✔ Insight: Helps determine the most common trip durations.

Step 7: Saving the Processed Data

After processing and analyzing the data, we save it for future use.

Save as CSV Using Pandas
df.toPandas().to_csv("processed_nyc_taxi.csv", index=False)
Save as CSV Using PySpark
df.write.csv("/Users/karthickraja/Downloads/finalPrj.csv", header=True, mode="overwrite")
✔ Now, the cleaned dataset is stored for further analysis or machine learning.


