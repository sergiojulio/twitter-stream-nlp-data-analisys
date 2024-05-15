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
# path


def clean_tweet(tweet):
    stopwords = ["for", "on", "an", "a", "of", "and", "in", "the", "to", "from"]
    temp = tweet.lower()
    temp = re.sub("'", "", temp) 
    temp = re.sub("@[A-Za-z0-9_]+","", temp)
    temp = re.sub("#[A-Za-z0-9_]+","", temp)
    temp = re.sub(r'http\S+', '', temp)
    temp = re.sub('[()!?]', ' ', temp)
    temp = re.sub('\[.*?\]',' ', temp)
    temp = re.sub("[^a-z0-9]"," ", temp)
    temp = temp.split()
    temp = [w for w in temp if not w in stopwords]
    temp = " ".join(word for word in temp)
    return temp

# os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.1.0,org.apache.spark:spark-sql-kafka-0-10_2.11:2.1.0 pyspark-shell'

def write_to_pgsql(df, epoch_id):

    # discard polarity null

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

    blob = TextBlob(clean_tweet(string))

    p = c = i = 0

    for sentence in blob.sentences:
        c = sentence.sentiment.polarity  + c
        i += 1
        
    if i > 0:
        p = c / i
        p = round(p,2)
    else:
        p = 0

    return p



def init_spark():

  """
  conf = pyspark.SparkConf().setAppName('MyApp').setMaster('spark://spark-master:7077')
  sc = pyspark.SparkContext(conf=conf)
    #.config("spark.jars", "/code/src/spark/postgresql-42.6.2.jar") \
  """

  spark = SparkSession \
    .builder \
    .appName("twitter-stream-nlp-data-analysis") \
    .getOrCreate()
  
  spark.sparkContext.setLogLevel("ERROR")

  sc = spark.sparkContext
  return spark,sc

  """
  Without spark action APIs(collect/take/first/saveAsTextFile) nothing will be executed on executors. 
  Its not possible to distribute plain python code just by submitting to spark. 
  """


if __name__ == "__main__":

    print("Stream Data Processing Starting... topic:" + kafka_topic)
    print(time.strftime("%Y-%m-%d %H:%M:%S"))

    spark,sc = init_spark()

    streamdf = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_server) \
        .option("subscribe", kafka_topic) \
        .option("startingOffsets", "latest") \
        .load() 
    
    print("Printing Schema:")

    streamdf.printSchema()

    schema = StructType([
        StructField("created", TimestampType()),
        StructField("text", StringType())
    ])

    udf_polarity = udf(polarity, FloatType()) # if the function returns an int

            #.withColumn("created", (F.to_timestamp(F.col("created"), "yyyy-MM-dd HH:mm:ss"))) \

    streamdf = streamdf.selectExpr("CAST(value AS STRING)") \
            .select(F.from_json("value", schema=schema).alias("data")) \
            .select("data.*") \
            .withColumn("polarity", udf_polarity(F.col("text"))) 

    # output

    csv_output = streamdf \
         .writeStream \
         .format("csv")\
         .option("format", "append")\
         .trigger(processingTime = "5 seconds")\
         .option("path", "/home/sergio/dev/docker/twitter-stream-nlp-data-analysis/src/kafka/csv")\
         .option("checkpointLocation", "/home/sergio/dev/docker/twitter-stream-nlp-data-analysis/src/kafka/checkpoint") \
         .outputMode("append") \
         .start()
    # spark.read.csv("oldLocation").coalesce(1).write.csv("newLocation")

    console_output = streamdf \
        .writeStream  \
        .trigger(processingTime='5 seconds') \
        .outputMode("update")  \
        .option("truncate", "true")\
        .format("console") \
        .start() 
        #.awaitTermination()

    # remove row with polarity null streamdf

    db_output = streamdf \
        .writeStream  \
        .trigger(processingTime='5 seconds') \
        .outputMode("update")  \
        .foreachBatch(write_to_pgsql) \
        .start()
        #.awaitTermination() 

    spark.streams.awaitAnyTermination()
    
    print("Stream Processing Successfully Completed")

