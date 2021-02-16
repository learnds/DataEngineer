import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql import functions as F


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    '''
    This function creates a Spark session with context used for processing data
    Input parameters: None
    Returns: Spark session 
    '''
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    '''
    This function processes song data that is stored in S3 and creates 
    songs and artists tables in parquet format
    Input parameters
        spark: Spark session object 
        input_data: Input folder/bucket where song data is stored
        output_data: Output folder/bucket where parquet files have to be written to 
    Returns: No return value
    '''
    # get filepath to song data file
    song_data = os.path.join(input_data, "song_data/A/*/*/*.json")
    
    # read song data file
    print("Reading song data")
    df = spark.read.json(song_data)
    
    df.createOrReplaceTempView("song_log_table")
    # extract columns to create songs table
    songs_table = spark.sql("select distinct song_id, title, artist_id, year, duration from song_log_table")
    print("Songs table count:",songs_table.count())
    
    # write songs table to parquet files partitioned by year and artist
    print("Writing songs table")
    songs_table_output = os.path.join(output_data,"songs.parquet")
    songs_table.write.mode("overwrite").parquet(songs_table_output)

    # extract columns to create artists table
    artists_table = spark.sql("select distinct artist_id, artist_name name,artist_location location, \
                    artist_latitude latitude, artist_longitude longitude from song_log_table \
                    where artist_name is not null")
    print("Artists table count:",artists_table.count())
    
    # write artists table to parquet files
    print("Writing artists table")
    artists_table_output = os.path.join(output_data,"artists.parquet")
    artists_table.write.mode("overwrite").parquet(artists_table_output)


def process_log_data(spark, input_data, output_data):
    '''
    This function processes log data that is stored in S3 and creates 
    users, time and songplays tables in parquet format
    Input parameters
        spark: Spark session object 
        input_data: Input folder/bucket where song data is stored
        output_data: Output folder/bucket where parquet files have to be written to 
    Returns: No return value
    ''' 
    # get filepath to log data file
    log_data =  os.path.join(input_data, "log_data/*/*/*.json")
    
    #local setting
    #log_data =  os.path.join(input_data, "log_data/")

    # read log data file
    print("Reading log data")
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df.createOrReplaceTempView("log_data_table")
    songplay_filter = spark.sql("select * from log_data_table where page='NextSong'")
    print("Total songplays table count:",songplay_filter.count())
    
    songplay_filter.createOrReplaceTempView("log_table")
    
    # extract columns for users table    
    users_table = spark.sql("select distinct userId user_id, firstName first_name, \
                            lastName last_name, gender, level from log_table")
    print("Users table count:",users_table.count())
    
    # write users table to parquet files
    print("Writing users table")
    users_table_output = os.path.join(output_data,"users.parquet")
    users_table.write.mode("overwrite").parquet(users_table_output) 

    ## Code replaced by spark.sql
    #############################
    # create timestamp column from original timestamp column
    #get_timestamp = udf()
    #df = 
    
    # create datetime column from original timestamp column
    #get_datetime = udf()
    #df = 
    ##################################
    
    # extract columns to create time table
    time_table = spark.sql("select distinct ts, from_unixtime(ts/1000.0, 'MM-dd-yyyy HH:mm:ss') start_time, \
                            from_unixtime(ts/1000.00,'HH') hour, \
                            from_unixtime(ts/1000.0,'dd') day, from_unixtime(ts/1000.0,'ww') week, \
                            from_unixtime(ts/1000.0,'MM') month, from_unixtime(ts/1000.0,'yyyy') year, \
                            from_unixtime(ts/1000.0,'u') weekday \
                            from log_table where ts is not null")
    print("Time table count:",time_table.count())
    
    # write time table to parquet files partitioned by year and month
    print("Writing time table")
    time_table_output = os.path.join(output_data,"time.parquet")
    time_table = time_table.repartition("year","month")
    time_table.write.mode("overwrite").parquet(time_table_output) 

    # read in song data to use for songplays table
    print("Reading songs table from parquet file")
    songs_table_input = os.path.join(output_data,"songs.parquet")
    songs_df = spark.read.parquet(songs_table_input)
    songs_df.createOrReplaceTempView("songs_table")
    print("Songs table count from parquet file:",songs_df.count())
    
    print("Reading artists table from parquet file")
    artists_table_input = os.path.join(output_data,"artists.parquet")
    artists_df = spark.read.parquet(artists_table_input)
    artists_df.createOrReplaceTempView("artists_table") 
    print("Artists table count from parquet file:",artists_df.count())

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql("select from_unixtime(a.ts/1000.0, 'MM-dd-yyyy HH:mm:ss') start_time, \
                                from_unixtime(ts/1000.0,'yyyy') year, \
                                from_unixtime(ts/1000.0,'MM') month, \
                                a.userId  user_id, a.level, a.song, b.song_id, a.artist,\
                                    c.artist_id, a.sessionId  session_id, a.location, a.userAgent user_agent  \
                                from log_table a, songs_table b, artists_table c \
                                where a.song = b.title \
                                and a.artist = c.name")
    print("Adding songplay_id column")
    songplays_table = songplays_table.withColumn("songplay_id",  F.monotonically_increasing_id())
    
    print("Songplays table count:",songplays_table.count())

    # write songplays table to parquet files partitioned by year and month
    print("Writing songplays table")
    songplays_table_output = os.path.join(output_data,"songplays.parquet")
    songplays_table = songplays_table.repartition("year","month")
    songplays_table.write.mode("overwrite").parquet(songplays_table_output) 


def main():
    '''
    The main block - creates a spark session, connects to the S3 service and calls relevant functions 
                    to process data into spark in-memory tables and then write the resulting Star schema tables into S3 service 
    '''    
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://sparkify-cts-output/"
    
    # Local settings
    #input_data = "data/"
    #output_data = "output/"
    
    print("Processing song data")    
    process_song_data(spark, input_data, output_data)    
    

    print("Processing log data")
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
