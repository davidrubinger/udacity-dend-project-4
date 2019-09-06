import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = input_data + "song_data/*/*/*/*.json"
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select("song_id", "title", "artist_id", "year", "duration")
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(
        "songs_table.parquet",
        mode="overwrite",
        partitionBy=["year", "artist_id"]
    )

    # extract columns to create artists table
    artists_table = (
        df
        .select(
            "artist_id",
            col("artist_name").alias("name"),
            col("artist_location").alias("location"),
            col("artist_latitude").alias("latitude"),
            col("artist_longitude").alias("longitude"))
        .distinct()
    )
    
    # write artists table to parquet files
    artists_table.write.parquet("artists_table.parquet", mode="overwrite")




def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = ""
    
    process_song_data(spark, input_data, output_data)    


if __name__ == "__main__":
    main()
