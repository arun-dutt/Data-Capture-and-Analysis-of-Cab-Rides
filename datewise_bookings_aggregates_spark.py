
import os
import sys
os.environ["PYSPARK_PYTHON"] = "/opt/cloudera/parcels/Anaconda/bin/python"
os.environ["JAVA_HOME"] = "/usr/java/jdk1.8.0_161/jre"
os.environ["SPARK_HOME"] = "/opt/cloudera/parcels/SPARK2-2.3.0.cloudera2-1.cdh5.13.3.p0.316101/lib/spark2/"
os.environ["PYLIB"] = os.environ["SPARK_HOME"] + "/python/lib"
sys.path.insert(0, os.environ["PYLIB"] +"/py4j-0.10.6-src.zip")
sys.path.insert(0, os.environ["PYLIB"] +"/pyspark.zip")


from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark import SparkContext, SparkConf, SQLContext
from pyspark.sql.functions import unix_timestamp, from_unixtime



spark = SparkSession.builder.master("local").appName("datewise_booking").getOrCreate()
sc = spark.sparkContext
sc

df = spark.read.csv("/user/root/bookings_data/part-m-00000",inferSchema = True)


df = df.withColumnRenamed("_c0","booking_id")\
	   .withColumnRenamed("_c1","customer_id") \
	   .withColumnRenamed("_c2","driver_id") \
	   .withColumnRenamed("_c3","customer_app_version")  \
	   .withColumnRenamed("_c4","customer_phone_os_version") \
	   .withColumnRenamed("_c5","pickup_lat") \
	   .withColumnRenamed("_c6","pickup_lon") \
	   .withColumnRenamed("_c7","drop_lat") \
	   .withColumnRenamed("_c8","drop_lon") \
	   .withColumnRenamed("_c9","pickup_timestamp")  \
	   .withColumnRenamed("_c10","drop_timestamp")  \
	   .withColumnRenamed("_c11","trip_fare") \
	   .withColumnRenamed("_c12","tip_amount")  \
	   .withColumnRenamed("_c13","currency_code") \
	   .withColumnRenamed("_c14","cab_color")  \
	   .withColumnRenamed("_c15","cab_registration_no") \
	   .withColumnRenamed("_c16","customer_rating_by_driver")  \
	   .withColumnRenamed("_c17","rating_by_customer")  \
	   .withColumnRenamed("_c18","passenger_count")

df =  df.withColumn("date", date_format('pickup_timestamp', "yyyy-MM-dd"))

df.show(5)

date = df.select('date').groupBy('date').count()

date.show()
date.count()
date.coalesce(1).write.format('com.databricks.spark.csv').mode('overwrite').save('home/datewise_aggreation', header = 'true')


df.coalesce(1).write.format('com.databricks.spark.csv').mode('overwrite').save('home/booking_data_csv', header = 'true')


