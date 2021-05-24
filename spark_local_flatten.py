from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Create spark session
spark = SparkSession.builder \
    .master("local") \
    .appName("Kafka To HDFS") \
    .getOrCreate()

# Reading json data into dataframe 
df = spark.read.json('/home/clickstream_data/part-00000-0668931f-4794-492f-b78b-fec98641896b-c000.json')

# extrating columns from jason value in dataframe and create new dataframe with new cloumns 
df = df.select(\
	get_json_object(df["value_str"],"$.customer_id").alias("customer_id"),\
	get_json_object(df["value_str"],"$.app_version").alias("app_version"),\
	get_json_object(df["value_str"],"$.OS_version").alias("OS_version"),\
	get_json_object(df["value_str"],"$.lat").alias("lat"),\
	get_json_object(df["value_str"],"$.lon").alias("lon"),\
	get_json_object(df["value_str"],"$.page_id").alias("page_id"),\
	get_json_object(df["value_str"],"$.button_id").alias("button_id"),\
	get_json_object(df["value_str"],"$.is_button_click").alias("is_button_click"),\
	get_json_object(df["value_str"],"$.is_page_view").alias("is_page_view"),\
	get_json_object(df["value_str"],"$.is_scroll_up").alias("is_scroll_up"),\
	get_json_object(df["value_str"],"$.is_scroll_down").alias("is_scroll_down"),\
	get_json_object(df["value_str"],"$.timestamp").alias("timestamp"),\
	)

# print schema of dataframe with new columns
print(df.schema)	

#print 10 records from dataframe
df.show(10)

# Save dataframe to csv file with headers in first row in local file directory
df.coalesce(1).write.format('com.databricks.spark.csv').mode('overwrite').save('user/root/clickstream_data_flatten', header = 'true')