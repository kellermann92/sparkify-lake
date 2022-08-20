import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id, udf
from pyspark.sql.types import TimestampType
from pyspark.sql.functions import year, month, dayofmonth, hour
from pyspark.sql.functions import weekofyear, dayofweek


# Columns in time table
TIME_KEEP = ['start_time',
            'hour',
            'day',
            'week',
            'month',
            'year',
            'weekday'
        ]

# Columns in users table:
USERS_KEEP = ['userId as user_id',
             'firstName as first_name',
             'lastName as last_name',
             'gender',
             'level',
        ]

# Columns in log table that will appear on songplays table:
LOG_KEEP = ['artist',
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
SONGPLAYS_KEEP = ['songplays_id',
                  'start_time',
                  'userId as user_id',
                  'level',
                  'song_id',
                  'artist_id',
                  'sessionId as session_id',
                  'location',
                  'userAgent as user_agent',
                ]

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
    time_table = df.select(TIME_KEEP).dropDuplicates()

    # Write parquet:
    time_table.write.mode('overwrite').partitionBy('year', 'month')\
        .parquet((output+'time'))

    # Generating users table. Then save as parquet.
    users_table = df.selectExpr(USERS_KEEP).dropDuplicates()
    users_table.write.mode('overwrite')\
        .parquet(str(os.path.join(output, 'users')))

    # Filter log columns that will appear on songplays:
    df = df.select(LOG_KEEP)

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
    songplays = songplays.selectExpr(SONGPLAYS_KEEP)

    print('\n############\nSongplays table:')
    songplays.show()
    print('############\n\n')

    # Save parquet file:
    songplays.write.mode('overwrite')\
        .parquet(output + 'songplays')