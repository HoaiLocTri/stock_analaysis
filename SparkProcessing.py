from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.types import TimestampNTZType
from KafkaData import KAFKA_BROKER, KAFKA_TOPIC_NAME

import logging


KAFKA_CONFIG = {
    "kafka.bootstrap.servers": KAFKA_BROKER,
    "subscribe": KAFKA_TOPIC_NAME,
    "startingOffsets": "latest",
    "failOnDataLoss":"false"
}

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


# create spark session
def create_SparkSession():
    spark = None

    try:
        spark = SparkSession \
                .builder \
                .appName("StockPriceAverage") \
                .master("local[*]") \
                .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.0,"+
                         "org.apache.commons:commons-pool2:2.11.1,"+
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
                    .options(**KAFKA_CONFIG)\
                    .load()\
                    .select(from_json(col("value").cast("string"),df_schema).alias("json_data"))\
                    .select("json_data.*")
        logging.info("kafka dataframe readed successfully")
    except Exception as e:
        logging.warning(f"kafka dataframe could not be readed because: {e}")

    return df

# data processing
def processing(df):

    try:
        df = df.select("time", "open", "close")
        df = df.withColumn("open_close_price", round((col("close") - col("open")), 2)).select("time", "open_close_price")
        df.printSchema()
        logging.info("spark processed successfully")
    except Exception as e:
        logging.warning(f"spark could not be processed because: {e}")

    return df

# Save processed data to data folder
def save(df_batch, batch_id):
    df_batch.write.parquet("./data/", mode="append")
    df_batch.show()

def writeStream(df):
    try:
        # df = df.select(to_json(struct(*df.columns)).alias("value"))
        # df.printSchema()
        output_df = df.writeStream\
                    .foreachBatch(save)\
                    .start()
        output_df.awaitTermination()
        logging.info("kafka dataframe writed successfully")
    except Exception as e:
        logging.warning(f"kafka dataframe could not be writed because: {e}")


if __name__ == '__main__':
    # create spark session
    spark_connect = create_SparkSession()

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
        df.printSchema()

        # Processing data
        result_df = processing(df)

        # Write Stream for kafka
        writeStream(result_df)

