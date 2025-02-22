from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.types import TimestampNTZType

import logging
import shutil

KAFKA_BROKER = 'localhost:9092'
KAFKA_TOPIC_NAME = 'stock'

KAFKA_INPUT_CONFIG = {
    "kafka.bootstrap.servers": KAFKA_BROKER,
    "subscribe": KAFKA_TOPIC_NAME,
    "startingOffsets": "latest",
    "failOnDataLoss":"false"
}
KAFKA_OUTPUT_CONFIG = {
    "kafka.bootstrap.servers": KAFKA_BROKER,
    "topic": KAFKA_TOPIC_NAME,
    "checkpointLocation": "./check.txt"
}


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# "org.apache.spark:spark-streaming-kafka-0-10_2.13:3.5.1,"+
# "org.apache.commons:commons-pool2:2.8.0,"+
# "org.apache.spark:spark-token-provider-kafka-0-10_2.13:3.5.1"

# create spark session
def create_spark_connection():
    spark = None

    try:
        spark = SparkSession \
                .builder \
                .appName("StockPriceAverage") \
                .master("local[*]") \
                .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.0,"+
                        "org.apache.kafka:kafka-clients:3.5.1,") \
                .config("spark.executor.memory", "512M") \
                .config("spark.executor.cores", "1") \
                .config("spark.driver.cores", "1") \
                .config("spark.driver.memory", "512M") \
                .getOrCreate()

        # spark.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully!")
    except Exception as e:
        logging.error(f"Couldn't create the spark session due to exception {e}")

    return spark

# Read Stream from kafka
def readStream(df_schema, spark_connect):
    try:
        df = spark_connect.readStream \
                    .format("kafka") \
                    .options(**KAFKA_INPUT_CONFIG)\
                    .load()\
                    .select(from_json(col("value").cast("string"),df_schema).alias("json_data"))\
                    .select("json_data.*")
        logging.info("kafka dataframe created successfully")
    except Exception as e:
        logging.warning(f"kafka dataframe could not be created because: {e}")

    return df

# data processing
def process(value_df):
    try:
        df = value_df.select("time", "open", "close")
        df = df.withColumn("open_close_price", round((col("close") - col("open")), 2))
        result_df = df.select("time", "open_close_price")
        result_df.printSchema()
        logging.info("spark processed successfully")
    except Exception as e:
        logging.warning(f"spark could not be processed because: {e}")
    

    return result_df

# Save processed data to data folder
def save(df_batch, batch_id):
    df_batch.write.parquet("data", mode="append")
    df_batch.show()


# Remove data directory when shutting down
def remove_data_directory():
    try:
        shutil.rmtree("data")
        logging.info("Data directory removed successfully!")
    except Exception as e:
        logging.error(f"Couldn't remove the data directory due to exception {e}")
        


if __name__ == '__main__':
    # create spark connection
    spark_connect = create_spark_connection()

    if spark_connect is not None:
        # Input Schema
        df_schema = StructType([
            StructField('time', TimestampNTZType(), True), \
            StructField('open', DoubleType(), True), \
            StructField('high', DoubleType(), True), \
            StructField('low', DoubleType(), True), \
            StructField('close', DoubleType(), True)
        ])
        # Read Stream from kafka
        df = readStream(df_schema, spark_connect)
        # transformed_df = spark_df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
        df.printSchema()
        
        
        
        # value_df = transformed_df.withColumn("value", from_json(transformed_df["value"], df_schema)).select("value.*") 

        # value_df.printSchema()
        # result_df = process(value_df)

        
        output_df = df.select(to_json(struct(*df.columns)).alias("value"))
        print(output_df)
        # try:
        #     # streaming_query = output_df.writeStream\
        #     #                     .foreachBatch(save)\
        #     #                     .start()
        #     write_stream = (output_df.writeStream\
        #                         .format("kafka")\
        #                         .options(**KAFKA_OUTPUT_CONFIG)\
        #                        .start())
        #     write_stream.awaitTermination()
        #     logging.info("spark dataframe writed successfully")
        # except Exception as e:
        #     logging.warning(f"spark dataframe could not be writed because: {e}")

        # try:
        #     (streaming_query).awaitTermination()
        # except Exception as e:
        #     logging.warning(e)
        # finally:
        #     remove_data_directory()
        
