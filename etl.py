import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (udf, col, year, month, dayofmonth, hour,
    weekofyear, date_format, dayofweek, max, monotonically_increasing_id)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType, TimestampType)
import logging
import boto3
from botocore.exceptions import ClientError
import sys


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']
os.environ['AWS_DEFAULT_REGION']=config['AWS']['AWS_DEFAULT_REGION']


def create_spark_session():
    """Create a Spark session"""
    
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def create_bucket(bucket_name, region=None, acl="private"):
    """
    Create an S3 bucket in a specified region
    
    Args:
        bucket_name: Bucket to create
        region: String region to create bucket in, e.g., 'us-west-2'
        acl: Canned access control list to apply to the bucket. 'public-read'
            makes sure everything posted is publicly readable
    """
    
    # Create bucket
    try:
        if region is None:
            s3_client = boto3.client('s3')
            s3_client.create_bucket(Bucket=bucket_name)
        else:
            s3_client = boto3.client('s3', region_name=region)
            location = {'LocationConstraint': region}
            s3_client.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration=location,
                ACL=acl
            )
    except ClientError as e:
        logging.error(e)
        return False
    return True


def process_song_data(spark, input_data, output_data):
    """
    Transform raw song data from S3 into analytics tables on S3
    
    This function reads in song data in JSON format from S3; defines the schema
    of songs and artists analytics tables; processes the raw data into
    those tables; and then writes the tables into partitioned parquet files on
    S3.
    
    Args:
        spark: a Spark session
        input_data: an S3 bucket to read song data in from
        output_data: an S3 bucket to write analytics tables to
    """
    
    # get filepath to song data file
    song_data = input_data + "song_data/*/*/*/*.json"
    
    # read song data file
    song_data_schema = StructType([
        StructField("artist_id", StringType(), False),
        StructField("artist_latitude", StringType(), True),
        StructField("artist_longitude", StringType(), True),
        StructField("artist_location", StringType(), True),
        StructField("artist_name", StringType(), False),
        StructField("song_id", StringType(), False),
        StructField("title", StringType(), False),
        StructField("duration", DoubleType(), False),
        StructField("year", IntegerType(), False)
    ])
    df = spark.read.json(song_data, schema=song_data_schema)
    
    # extract columns to create songs table
    songs_table = df.select("song_id", "title", "artist_id", "year", "duration")
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(
        output_data + "songs_table.parquet",
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
    artists_table.write.parquet(
        output_data + "artists_table.parquet", mode="overwrite"
    )


def process_log_data(spark, input_data, output_data):
    """
    Transform raw log data from S3 into analytics tables on S3
    
    This function reads in log data in JSON format from S3; defines the schema
    of songplays, users, and time analytics tables; processes the raw data into
    those tables; and then writes the tables into partitioned parquet files on
    S3.
    
    Args:
        spark: a Spark session
        input_data: an S3 bucket to read log data in from
        output_data: an S3 bucket to write analytics tables to
    """
    
    # get filepath to log data file
    log_data = input_data + "log_data/*/*/*.json"
    
    # read log data file
    log_data_schema = StructType([
        StructField("artist", StringType(), True),
        StructField("auth", StringType(), False),
        StructField("firstName", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("itemInSession", IntegerType(), False),
        StructField("lastName", StringType(), True),
        StructField("length", DoubleType(), True),
        StructField("level", StringType(), False),
        StructField("location", StringType(), True),
        StructField("method", StringType(), False),
        StructField("page", StringType(), False),
        StructField("registration", DoubleType(), True),
        StructField("sessionId", IntegerType(), False),
        StructField("song", StringType(), True),
        StructField("status", IntegerType(), False),
        StructField("ts", DoubleType(), False),
        StructField("userAgent", StringType(), True),
        StructField("userId", StringType(), True)
    ])
    df = spark.read.json(log_data, schema=log_data_schema)
    
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
    users_table.write.parquet(
        output_data + "users_table.parquet", mode="overwrite"
    )
    
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
        output_data + "time_table.parquet",
        mode="overwrite",
        partitionBy=["year", "month"]
    )
    
    # read in song data to use for songplays table
    song_df = spark.read.parquet(output_data + "songs_table.parquet")
    
    # extract columns from joined song and log datasets to create songplays table 
    artists_table = spark.read.parquet(output_data + "artists_table.parquet")
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
        output_data + "songplays_table.parquet",
        mode="overwrite",
        partitionBy=["year", "month"]
    )


def main():
    """Run ETL pipeline"""
    
    spark = create_spark_session()
    
    # S3 bucket name to create and output tables to
    output_bucket = sys.argv[1]
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://" + output_bucket + "/"
    
    create_bucket(
        output_bucket,
        region=os.environ['AWS_DEFAULT_REGION'],
        acl="public-read"
    )
    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
