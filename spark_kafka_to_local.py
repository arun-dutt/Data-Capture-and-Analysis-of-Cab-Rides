from pyspark.sql import SparkSession


# Create spark session
spark = SparkSession.builder \
    .master("local") \
    .appName("Kafka To HDFS") \
    .getOrCreate()


# Create dataframe from kafka data
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "18.211.252.152:9092") \
    .option("subscribe","de-capstone3") \
    .option("startingOffsets","earliest") \
    .load()
df.printSchema()
# Transfrom dataframe by dropping few columns and chnaging value column data type
 df = df.withColumn('value_str', df['value'].cast('string').alias('key_str')).drop('value') \
        .drop('key','topic','partition','offset','timestamp','timestampType')
 
# writing the dtatframe to local file directory and keep it running until terminated 
df.writeStream \
  .outputMode("append") \
  .format("json") \
  .option("truncate","false") \
  .option("path", "/home/clickstream_data") \
  .option("checkpointLocation","/home/clickstream_checkpoint") \
  .start() 

df.awaitTermination()
