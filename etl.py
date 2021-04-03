'''
Filename: /home/kellermann/git_repo/sparkify-lake/etl.py
Path: /home/kellermann/git_repo/sparkify-lake
Created Date: Wednesday, March 31st 2021, 10:35:11 am
Author: Leandro Kellermann de Oliveira

Copyright (c) 2021 myProjects
'''

import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType
from pyspark.sql.types import DoubleType, IntegerType
from pyspark.sql.types import StringType, TimestampType
from pyspark.sql.functions import monotonically_increasing_id, udf
from pyspark.sql.functions import year, month, dayofmonth, hour
from pyspark.sql.functions import weekofyear, dayofweek


def create_spark_session() -> SparkSession:
    """Method to create an SparkSession to connect to AWS EMR Cluster.

    Returns:
        SparkSession: SparkSession object that connects to AWS EMR Cluster.
    """
    session = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return session


def process_song_data(session: SparkSession, input: str, output: str) -> None:
    """Method to process songs json files.

    Args:
        session (SparkSession): a SparkSession object.
        input (str): path to .json input files.
        output (str): path to parquet output files.

    Json example:
    {
        "num_songs": 1,
        "artist_id": "ARJIE2Y1187B994AB7",
        "artist_latitude": null,
        "artist_longitude": null,
        "artist_location": "",
        "artist_name": "Line Renaud",
        "song_id": "SOUPIRU12A6D4FA1E1",
        "title": "Der Kleine Dompfaff",
        "duration": 152.92036,
        "year": 0
    }

    """
    # get filepath to song data filesong_data/A/B/C/TRABCEI128F424C983.json
    song_data = os.path.join(input, 'song_data/*/*/*/*.json')
    """
    schema_song_table = StructType([
        StructField("num_songs", IntegerType()),
        StructField("artist_id", StringType()),
        StructField("artist_latitude", DoubleType()),
        StructField("artist_longitude", DoubleType()),
        StructField("artist_location", StringType()),
        StructField("artist_name", StringType()),
    #   StructField("song_id", StringType()),
        StructField("title", StringType()),
        StructField("duration", DoubleType()),
        StructField("year", IntegerType()),
    ])
    """

    schema_song_table = StructType([
        StructField("artist_id", StringType()),
        StructField("artist_latitude", DoubleType()),
        StructField("artist_location", StringType()),
        StructField("artist_longitude", StringType()),
        StructField("artist_name", StringType()),
        StructField("song_id", StringType(), nullable=True),
        StructField("duration", DoubleType()),
        StructField("num_songs", IntegerType()),
        StructField("title", StringType()),
        StructField("year", IntegerType()),
    ])

    # Columns to generate dimension songs_table:
    songs_keep = ['song_id',
                  'title',
                  'artist_id',
                  'year',
                  'duration',
                  ]

    # Columns to generate dimension artists_table:
    artists_keep = ['artist_id',
                    'artist_name as name',
                    'artist_location as location',
                    'artist_latitude as latitude',
                    'artist_longitude as longitude',
                    ]

    # read song data file
    df = session.read.json(song_data, schema=schema_song_table, )

    # Create songs dimension table.
    songs_table = df.select(songs_keep).dropDuplicates()

    songs_table.show()

    # Write songs dimension table in output directory:
    songs_table.write.mode('overwrite')\
        .partitionBy('artist_id', 'year').parquet(output+'songs')

    # Create artists dimension table:
    artists_table = df.selectExpr(artists_keep).dropDuplicates()

    # Write songs dimension table in output directory:
    artists_table.write.mode('overwrite').parquet(output+'artists')

    print('Song logs processed!\n##########\n')


def process_log_data(session: SparkSession, input: str, output: str) -> None:
    """Method to process log events data.

    Args:
        session (SparkSession): Spark session opbject.
        input (str): path to input files.
        output (str): path to parquet output files

    Log data schema:
        artist (str): artist name.
        auth (str?): authentication.
        firstName (str): first name of the user.
        gender (str): gender of the user
        itemInSession (int): order in which music was played by user in a
                             session (0 is first).
        lastName (str): last name of the user.
        length (real): duration of the song in seconds.
        level (str): level of the user plan (free or paid)
        location (str): user location.
        method (str): method applied.
        page (str): page of the aplication.
        registration (big int): user registration number.
        sessionId (int): user session id.
        song (str): song name.
        status (int): status code.
        ts (timestamp): timestamp at which a item in session started.
        userAgent (str): information about how the user accessed
                         the platform (brownser, OS, app, etc).
        userId (int): user ID.

    """
    # Get filepath to log data file
    log_data = os.path.join(input, 'log_data/*/*/*.json')

    # Columns in time table
    time_keep = ['start_time',
                 'hour',
                 'day',
                 'week',
                 'month',
                 'year',
                 'weekday']

    # Columns in users table:
    users_keep = ['userId as user_id',
                  'firstName as first_name',
                  'lastName as last_name',
                  'gender',
                  'level',
                  ]

    # Columns in log table that will appear on songplays table:
    keep_log = ['artist',
                'start_time',
                'length',
                'level',
                'location',
                'sessionId',
                'song',
                'userId',
                'year',
                'userAgent'
                ]

    # Columns in songplays table:
    songplays_keep = ['songplays_id',
                      'start_time',
                      'userId as user_id',
                      'level',
                      'song_id',
                      'artist_id',
                      'sessionId as session_id',
                      'location',
                      'userAgent as user_agent',
                      ]

    # read log data file
    df = session.read.json(log_data)

    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # Create timestamp column:
    get_ts = udf(lambda x: x / 1000, TimestampType())
    df = df.withColumn('ts', get_ts(df.ts))

    # Create datetime column from original timestamp column:
    get_datetime = udf(lambda x: datetime.fromtimestamp(x), TimestampType())
    df = df.withColumn('start_time', get_datetime(df.ts))\

    # Creating other time columns:
    df = df.withColumn('hour', hour('start_time'))\
        .withColumn('day', dayofmonth('start_time'))\
        .withColumn('week', weekofyear('start_time'))\
        .withColumn('month', month('start_time'))\
        .withColumn('year', year('start_time'))\
        .withColumn('weekday', dayofweek('start_time'))

    # Cxtract columns to create time table
    time_table = df.select(time_keep).dropDuplicates()

    # Write parquet:
    time_table.write.mode('overwrite').partitionBy('year', 'month')\
        .parquet((output+'time'))

    # Generating users table. Then save as parquet.
    users_table = df.selectExpr(users_keep).dropDuplicates()
    users_table.write.mode('overwrite')\
        .parquet(str(os.path.join(output, 'users')))

    # Filter log columns that will appear on songplays:
    df = df.select(keep_log)

    # Read artists dimension table:
    artists_df = session.read.parquet(output+'artists')

    # Join artist and df:
    artist_join = df.join(artists_df, df.artist ==
                          artists_df.name).drop(artists_df.location)

    # Read songs dimension table:
    songs_df = session.read.parquet(output+'songs/*/*/*')

    # Join artist_join with songs dimension table
    # to generate the "raw" songplays table:
    songplays = artist_join.join(songs_df,
                                 (artist_join.song == songs_df.title)
                                 & (artist_join.length == songs_df.duration))\
        .withColumn('songplays_id', monotonically_increasing_id())

    # Keep only selected columns:
    songplays = songplays.selectExpr(songplays_keep)

    print('\n############\nSongplays table:')
    songplays.show()
    print('############\n\n')

    # Save parquet file:
    songplays.write.mode('overwrite')\
        .parquet(output + 'songplays')


def main():
    """Main method to execute the ETL process."""

    config = configparser.ConfigParser()
    config.read('dl.cfg')
    os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
    os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']

    session = create_spark_session()

    input = 's3a://udacity-dend/'
    output = 's3a://mkt-sparkify/'

    # You can use the sample dataset in this repository to test the application:
    # input = 'input/'
    # output = output/'
    process_song_data(session, input, output)
    process_log_data(session, input, output)

    print('Finish!')


if __name__ == "__main__":
    main()
