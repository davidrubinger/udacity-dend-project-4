import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (udf, col, year, month, dayofmonth, hour,
    weekofyear, date_format, dayofweek, max, monotonically_increasing_id)
from pyspark.sql.types import TimestampType


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


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = input_data + "log_data/*/*/*.json"

    # read log data file
    df = spark.read.json(log_data)

    # filter by actions for song plays
    df = df.filter(col("page") == "NextSong") 

    # extract columns for users table    
    users_table = (
         df
        .withColumn("max_ts_user", max("ts").over(Window.partitionBy("userID")))
        .filter(
            (col("ts") == col("max_ts_user")) &
            (col("userID") != "") &
            (col("userID").isNotNull())
        )
        .select(
            col("userID").alias("user_id"),
            col("firstName").alias("first_name"),
            col("lastName").alias("last_name"),
            "gender",
            "level"
        )
    )

    # write users table to parquet files
    users_table.write.parquet("users_table.parquet", mode="overwrite")

    # create datetime column from original timestamp column
    get_datetime = udf(
        lambda x: datetime.fromtimestamp(x / 1000).replace(microsecond=0),
        TimestampType()
    )
    df = df.withColumn("start_time", get_datetime("ts"))

    # extract columns to create time table
    time_table = (
        df
        .withColumn("hour", hour("start_time"))
        .withColumn("day", dayofmonth("start_time"))
        .withColumn("week", weekofyear("start_time"))
        .withColumn("month", month("start_time"))
        .withColumn("year", year("start_time"))
        .withColumn("weekday", dayofweek("start_time"))
        .select("start_time", "hour", "day", "week", "month", "year", "weekday")
        .distinct()
    )

    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(
        "time_table.parquet", mode="overwrite", partitionBy=["year", "month"]
    )

    # read in song data to use for songplays table
    song_df = spark.read.parquet("songs_table.parquet")

    # extract columns from joined song and log datasets to create songplays table 
    artists_table = spark.read.parquet("artists_table.parquet")
    songs = (
        song_df
        .join(artists_table, "artist_id", "full")
        .select("song_id", "title", "artist_id", "name")
    )
    songplays_table = df.join(
        songs, [df.song == songs.title, df.artist == songs.name], "left"
    )
    songplays_table = (
        songplays_table
        .join(time_table, "start_time", "left")
        .select(
            "start_time",
            col("userId").alias("user_id"),
            "level",
            "song_id",
            "artist_id",
            col("sessionId").alias("session_id"),
            "location",
            col("userAgent").alias("user_agent"),
            "year",
            "month"
        )
        .withColumn("songplay_id", monotonically_increasing_id())
    )

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(
        "songplays_table.parquet",
        mode="overwrite",
        partitionBy=["year", "month"]
    )


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = ""
    
    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
