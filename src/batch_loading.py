import findspark
findspark.init()

from pyspark.sql import SparkSession, DataFrame # type: ignore
from functools import reduce

# Initialize Spark session
spark = SparkSession.builder.appName("NYC_Taxi_Batch").getOrCreate()

# Define file paths (update with your actual file paths)
csv_files = [
    "./data/yellow_tripdata_2015-01.csv",
    "./data/yellow_tripdata_2016-01.csv",
    # "./data/yellow_tripdata_2016-02.csv",
    # "./data/yellow_tripdata_2016-03.csv",
]

# Read CSV files into DataFrames
dfs = [spark.read.csv(file, header=True, inferSchema=True) for file in csv_files]

# Combine all data
taxi_data = reduce(lambda df1, df2: df1.unionByName(df2), dfs)

# Show schema and sample data
taxi_data.printSchema()
taxi_data.show(5)

# Write to Parquet format
taxi_data.write.mode("overwrite").parquet("nyc_taxi_parquet")