from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, LongType, StringType
from pyspark.sql import functions as func

spark = SparkSession.builder.appName("PopularMovies").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

movieNamesSchema = StructType([ \
    StructField("movieID", IntegerType(), True), \
    StructField("movieTitle", StringType(), True) \
    ])

# Create schema when reading u.data
ratingSchema = StructType([StructField("userID", IntegerType(), True),
                           StructField("movieID", IntegerType(), True),
                           StructField("rating", IntegerType(), True),
                           StructField("timestamp", LongType(), True)])

movieNamesDF = spark.read \
    .option("sep", "|") \
    .option("charset", "ISO-8859-1") \
    .schema(movieNamesSchema) \
    .csv("file:///opt/spark-data/ml-100k/u.item") \
    .repartition(10, func.col("movieID"))

movieNamesDF.createTempView("movieNames")

# Load up movie data as dataframe
moviesDF = spark.read \
    .option("sep", "\t") \
    .schema(ratingSchema) \
    .csv("file:///opt/spark-data/ml-100k/u.data")\
    .repartition(10, func.col("movieID"))

moviesDF.createTempView("moviesRatings")

l_sql="""
With agg_data as (
  Select 
      mr.movieID
    , count(*) as ratings_cnt 
  From 
    moviesRatings mr 
  Group BY
    mr.movieID
)
Select /*+ BROADCAST(agg) */
    agg.movieID
  , mn.movieTitle
  , agg.ratings_cnt
From    
  agg_data agg
Join
  movieNames mn 
on 
  mn.movieID = agg.movieID
Order by
  agg.ratings_cnt Desc          
"""

#spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 104857600)
spark.sql(l_sql).explain(extended=True)

# l_result = spark.sql(l_sql).show()

# for l_one_movie in l_result:
#     print(l_one_movie)

# Stop the session
spark.stop()
