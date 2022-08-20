from pyspark.sql.types import StructField, StructType
from pyspark.sql.types import DoubleType, IntegerType
from pyspark.sql.types import StringType

song_table = StructType([
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