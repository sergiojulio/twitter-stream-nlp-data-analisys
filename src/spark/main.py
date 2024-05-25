from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.functions import window
from pyspark.sql.types import TimestampType, StringType, FloatType, StructType, StructField
import pyspark.sql.functions as F
from pyspark.sql.functions import udf
import time
import re
from textblob import TextBlob
import os

# ENV
kafka_topic = os.environ["KAFKA_TOPIC"]
kafka_server = os.environ["KAFKA_SERVER"]
#
postgres_db = os.environ["POSTGRES_DB"]
postgres_user = os.environ["POSTGRES_USER"]
postgres_pass = os.environ["POSTGRES_PASS"]
postgres_server = os.environ["POSTGRES_SERVER"]


def write_to_pgsql(df, epoch_id):

    df.write \
    .format('jdbc') \
    .options(url="jdbc:postgresql://" + postgres_server + "/" + postgres_db,
            driver="org.postgresql.Driver",
            dbtable="stream",
            user=postgres_user,
            password=postgres_pass,
            ) \
    .mode('append') \
    .save()


def polarity(string):

    if type(string) != str:
        return 0

    blob = TextBlob(string)
    p = c = i = 0
    for sentence in blob.sentences:
        c = sentence.sentiment.polarity + c
        i += 1
        
    if i > 0:
        p = c / i
        p = round(p,2)
    else:
        p = 0

    return p


def init_spark():

    spark = SparkSession \
        .builder \
        .appName("twitter-stream-nlp-data-analysis") \
        .getOrCreate()

    spark.conf.set("spark.sql.shuffle.partitions", 1)    

    spark.sparkContext.setLogLevel("ERROR")

    sc = spark.sparkContext
    return spark,sc


if __name__ == "__main__":

    print("Stream Data Processing Starting... topic:" + kafka_topic)
    print(time.strftime("%Y-%m-%d %H:%M:%S"))

    spark,sc = init_spark()


    # .option("startingOffsets", "latest") 

    streamdf = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_server) \
        .option("subscribe", kafka_topic) \
        .load() 
    
    print("Printing Schema:")

    streamdf.printSchema()

    schema = StructType([
        StructField("created", TimestampType()),
        StructField("text", StringType())
    ])

    udf_polarity = udf(polarity, FloatType()) # if the function returns an int


    streamdf = streamdf.selectExpr("CAST(value AS STRING)") \
            .select(F.from_json("value", schema=schema).alias("data")) \
            .select("data.*") \
            .withColumn("polarity", udf_polarity(F.col("text"))) # bottleneck


    # outputs
    
    # csv_output = streamdf \
    #      .writeStream \
    #      .format("csv")\
    #      .option("format", "append")\
    #      .trigger(processingTime = "5 seconds")\
    #      .option("path", "../kafka/csv")\
    #      .option("checkpointLocation", "../kafka/checkpoint") \
    #      .outputMode("append") \
    #      .start()

    console_output = streamdf \
        .writeStream  \
        .trigger(processingTime='5 seconds') \
        .outputMode("update")  \
        .option("truncate", "true")\
        .format("console") \
        .start() 
        #.awaitTermination()

    # remove row with polarity null streamdf
    streamdf = streamdf.filter(streamdf.polarity. isNotNull())
    #

    db_output = streamdf \
        .writeStream  \
        .trigger(processingTime='5 seconds') \
        .outputMode("update")  \
        .foreachBatch(write_to_pgsql) \
        .start()
        #.awaitTermination() 

    spark.streams.awaitAnyTermination()
    
    print("Stream Processing Successfully Completed")

