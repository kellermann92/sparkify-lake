import os
from .schemas import song_table
from pyspark.sql import SparkSession

    # Columns to generate dimension songs_table:
SONGS_KEEP = ['song_id',
                  'title',
                  'artist_id',
                  'year',
                  'duration',
                  ]

    # Columns to generate dimension artists_table:
ARTISTS_KEEP = ['artist_id',
                    'artist_name as name',
                    'artist_location as location',
                    'artist_latitude as latitude',
                    'artist_longitude as longitude',
                    ]

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
    #song_data = os.path.join(input, 'song_data')
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


    # read song data file
    df = session.read.json(song_data, schema=song_table)

    #df = session.read.option("recursiveFileLookup", "true").json(song_data, schema=song_table)

    # Create songs dimension table.
    songs_table = df.select(SONGS_KEEP).dropDuplicates()

    songs_table.write.mode('overwrite')\
        .partitionBy('artist_id', 'year').parquet(output+'songs')

    # Create artists dimension table:
    artists_table = df.selectExpr(ARTISTS_KEEP).dropDuplicates()

    # Write songs dimension table in output directory:
    artists_table.write.mode('overwrite').parquet(output+'artists')

    print('Song logs processed!\n##########\n')