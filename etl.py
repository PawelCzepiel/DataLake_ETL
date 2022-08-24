import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType as R, StructField as Fld
from pyspark.sql.types import TimestampType as Ts, DoubleType as Dbl, StringType as Str, IntegerType as Int, DateType as Date, DecimalType as Dec

config = configparser.ConfigParser()
config.read('../dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    Description: Function builds (or gets if it exists) Spark Session and configures it with:
                 - spark.jars.packages: comma-separated list of Maven
                   coordinates of jars to include on the driver and executor classpaths. 
                 - org.apache.hadoop:hadoop-aws:2.7.0: Module containing code to support
                   integration with Amazon Web Services.
    Arguments:
               None
    Returns:
               Spark Session
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Description: Function:
                - loads song data from json files located in S3 bucket
                - creates staging dataframe with all records
                - builds songs and artists tables and saves them into S3 bucket in parquet format
    Arguments:
                - spark - Spark Session
                - input_data - source S3 bucket path
                - output_data - target S3 bucket path
    Returns:
               None
    """
    
    # get filepath to song data file
    song_data = input_data + "song_data/*/*/*/*.json"
    
    songSchema = R([
        Fld("num_song",Int()),
        Fld("artist_id",Str()),
        Fld("artist_latitude",Str()),
        Fld("artist_longitude",Str()),
        Fld("artist_location",Str()),
        Fld("artist_name",Str()),
        Fld("song_id",Str()),
        Fld("title",Str()),
        Fld("duration",Dec()),
        Fld("year",Int())
    ])
    
    # read song data file
    df = spark.read.json(song_data, schema=songSchema)

    # extract columns to create songs table
    songs_table = df[['song_id','title','artist_id','year','duration']]
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(os.path.join(output_data,'songs'), partitionBy=['year','artist_id'])
    
    # extract columns to create artists table
    artists_table = df.select(F.col('artist_name').alias('name'),
                            F.col('artist_location').alias('location'),
                            F.col('artist_latitude').alias('latitude'),
                            F.col('artist_longitude').alias('longitude')
                            )
    
    # write artists table to parquet files
    artists_table.write.parquet(os.path.join(output_data,'artists'))


def process_log_data(spark, input_data, output_data):
    """
    Description: Function:
                - loads logs data from json files located in S3 bucket
                - creates staging dataframe with all records
                - builds users and time dimension tables + songplays fact table
                  and saves them into S3 bucket in parquet format
    Arguments:
                - spark - Spark Session
                - input_data - source S3 bucket path
                - output_data - target S3 bucket path
    Returns:
               None
    """
    # get filepath to log data file
    log_data = input_data + "log_data/*/*/*.json"

    logSchema = R([
        Fld("artist",Str()),
        Fld("auth",Str()),
        Fld("firstName",Str()),
        Fld("gender",Str()),
        Fld("itemInSession",Str()),
        Fld("lastName",Str()),
        Fld("length",Str()),
        Fld("level",Str()),
        Fld("location",Str()),
        Fld("method",Str()),
        Fld('page',Str()),
        Fld('registration',Str()),
        Fld('sessionId',Int()),
        Fld('song',Str()),
        Fld('status',Int()),
        Fld('ts',Dbl()),
        Fld('userAgent',Str()),
        Fld('userID',Int())
    ])
    
    # read log data file
    df = spark.read.json(log_data, schema=logSchema)

    # filter by actions for song plays
    df = df.where(df.page == 'NextSong')

    # extract columns for users table    
    users_table = df.select(
                        F.col('userID').alias('user_id'),
                        F.col('firstName').alias('first_name'),
                        F.col('lastName').alias('last_name'),
                        F.col('gender'),
                        F.col('level')
                        )
    
    # write users table to parquet files
    users_table.write.parquet(os.path.join(output_data,'users'))

    # create timestamp column from original timestamp column
    get_timestamp = F.udf(lambda x:  datetime.fromtimestamp(x/1000.0).strftime('%Y-%m-%d %H:%M:%S'))
    df = df.withColumn("timestamp", get_timestamp(F.col('ts')).cast(Ts()))
    
    # create datetime column from original timestamp column
    get_datetime = F.udf(lambda x: datetime.fromtimestamp(x/1000).strftime('%Y-%m-%d'))
    df = df.withColumn("datetime", get_datetime(F.col('ts')).cast(Date()))
    
    # extract columns to create time table
    time_table = df.select(F.col('timestamp').alias('start_time'))\
        .withColumn('hour', F.hour('start_time'))\
        .withColumn('day', F.dayofmonth('start_time'))\
        .withColumn('week', F.weekofyear('start_time'))\
        .withColumn('month', F.month('start_time'))\
        .withColumn('year', F.year('start_time'))\
        .withColumn('weekday', F.dayofweek('start_time'))
    
    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(os.path.join(output_data,'time'),partitionBy=['year','month'])

    # read in song data to use for songplays table
    
    song_data = input_data + "song_data/A/A/A/*.json"
    
    songSchema = R([
            Fld("num_song",Int()),
            Fld("artist_id",Str()),
            Fld("artist_latitude",Str()),
            Fld("artist_longitude",Str()),
            Fld("artist_location",Str()),
            Fld("artist_name",Str()),
            Fld("song_id",Str()),
            Fld("title",Str()),
            Fld("duration",Dec()),
            Fld("year",Int())
    ])

    song_df = spark.read.json(song_data, schema=songSchema)
    
    # extract columns from joined song and log datasets to create songplays table 
    df = df.orderBy('ts')
    df = df.withColumn('songplay_id', F.monotonically_increasing_id())
    
    song_df.createOrReplaceTempView('songs')
    df.createOrReplaceTempView('events')

    songplays_table = spark.sql("""
            SELECT
                se.songplay_id,
                se.timestamp as start_time,
                se.userID as user_id,
                se.level,
                ss.song_id,
                ss.artist_id,
                se.sessionId as session_id,
                se.location,
                se.userAgent as user_agent,
                year(se.timestamp) as year,
                month(se.timestamp) as month
            FROM events se
            LEFT JOIN songs ss 
            ON se.song = ss.title 
            AND se.artist = ss.artist_name 
            AND se.length = ss.duration
        """)


    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(os.path.join(output_data,"songplays"), partitionBy = ['year','month'])


def main():
    """
    Description: Function:
                - establishes Spark Session
                - defines input and output S3 bucket paths
                - triggers processing of song&log json files into star schema tables
    Arguments:
                None
    Returns:
                None
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://pwlcpl-sparkify/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
