import json
import logging
import os
import queue
import threading

import pandas as pd
from kafka import KafkaConsumer, KafkaProducer
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType

logger = logging.getLogger()
Producer = None

# Driver function
def loop():
    kafka_consumer_executor()
    while True:
        carbonsense_queue_length = carbonsense_data_buffer.qsize()
        moisturemate_queue_length = moisturemate_data_buffer.qsize()
        luxmeter_queue_length = luxmeter_data_buffer.qsize()
        smart_thermo_queue_length = smart_thermo_data_buffer.qsize()
        if (
            (carbonsense_queue_length >= 1)
            and (moisturemate_queue_length >= 1)
            and (luxmeter_queue_length >= 1)
            and (smart_thermo_queue_length >= 1)
        ):
            carbonsense_dataframe = carbonsense_data_buffer.get()
            moisturemate_dataframe = moisturemate_data_buffer.get()
            luxmeter_dataframe = luxmeter_data_buffer.get()
            smart_thermo_dataframe = smart_thermo_data_buffer.get()
            transformed_dataframe = transformation(
                carbonsense_dataframe,
                moisturemate_dataframe,
                luxmeter_dataframe,
                smart_thermo_dataframe,
            )

            logger.info(f" transformed data frame successfully!!!")
            publish_to_kafka(transformed_dataframe)


def publish_to_kafka(dataframe):
    try:
        dataframe_rows = dataframe.toJSON().collect()
    except MemoryError:
        logging.error("A MemoryError occurred. Please free up memory and try again.")
    message_to_publish = [json.loads(message) for message in dataframe_rows]
    Producer.send("transformed_dataframe", value=message_to_publish)
    Producer.flush()


def kafka_consumer_executor():
    carbon_thread = threading.Thread(target=carbonsense_sensor_consumer)
    moisturemate_thread = threading.Thread(target=moisturemate_sensor_consumer)
    luxmeter_thread = threading.Thread(target=luxmeter_sensor_consumer)
    smartthermo_thread = threading.Thread(target=smart_thermo_sensor_consumer)
    carbon_thread.start()
    moisturemate_thread.start()
    smartthermo_thread.start()
    luxmeter_thread.start()


def json_deserializer(data):
    return json.loads(data.decode("utf-8"))


# Consumer methods to consume messages that are being published in their respective topics.
def carbonsense_sensor_consumer():
    carbonsense_consumer = KafkaConsumer(
        "carbonsense",
        bootstrap_servers=KAFKA_BROKER_URL,
        value_deserializer=json_deserializer,
        auto_offset_reset="latest",
        enable_auto_commit=True,
    )
    data_consumption(carbonsense_consumer, carbonsense_data_buffer)


def moisturemate_sensor_consumer():
    moisturemate_consumer = KafkaConsumer(
        "moisturemate",
        bootstrap_servers=KAFKA_BROKER_URL,
        value_deserializer=json_deserializer,
        auto_offset_reset="latest",
        enable_auto_commit=True,
    )
    data_consumption(moisturemate_consumer, moisturemate_data_buffer)


def luxmeter_sensor_consumer():
    luxmeter_consumer = KafkaConsumer(
        "luxmeter",
        bootstrap_servers=KAFKA_BROKER_URL,
        value_deserializer=json_deserializer,
        auto_offset_reset="latest",
        enable_auto_commit=True,
    )
    luxmeter_data_consumption(luxmeter_consumer, luxmeter_data_buffer)


def smart_thermo_sensor_consumer():
    smart_thermo_consumer = KafkaConsumer(
        "smart_thermo",
        bootstrap_servers=KAFKA_BROKER_URL,
        value_deserializer=json_deserializer,
        auto_offset_reset="latest",
        enable_auto_commit=True,
    )
    smart_thermo_data_consumption(smart_thermo_consumer, smart_thermo_data_buffer)


# Messages are being recieved and then converted into dataframes and are eventually being placed into queues to be read here.
def data_consumption(consumer, queue_buffer):
    while True:
        messages = []
        for message in consumer:
            messages.append(message.value)
            if len(messages) == 4:
                dataframe = create_dataframe(messages)
                logging.info("Message recieved and dataframe created successfully")
                queue_buffer.put(dataframe)
                break


def luxmeter_data_consumption(consumer, queue_buffer):
    while True:
        messages = []
        for message in consumer:
            messages.append(message.value)
            if len(messages) == 4:
                dataframe = create_dataframe_luxmeter(messages)
                logging.info("Message recieved and dataframe created successfully")
                queue_buffer.put(dataframe)
                break


def smart_thermo_data_consumption(consumer, queue_buffer):
    while True:
        for message in consumer:
            if len(message.value) == 4:
                dataframe = create_dataframe(message.value)
                logging.info("Message recieved and dataframe created successfully")
                queue_buffer.put(dataframe)
                break


# Dataframes creator methods that convert messages into pyspark dataframes
def create_dataframe(JSON_message):
    dataframe = spark.createDataFrame(JSON_message)
    dataframe.createOrReplaceTempView("dataframe")
    return dataframe


def create_dataframe_luxmeter(JSON_messages):
    tuples = [
        (
            msg.get(list(msg.keys())[0]).get("timestamp"),
            list(msg.keys())[0],
            msg.get(list(msg.keys())[0]).get("light_level"),
        )
        for msg in JSON_messages
    ]
    tmp_df = pd.DataFrame(tuples, columns=["timestamp", "room_id", "light_level"])
    luxmeter_df = spark.createDataFrame(tmp_df)
    return luxmeter_df


# These function perform different transformation techniques on the dataframes as per requirements.
def transformation(df1, df2, df3, df4):
    df = df1.join(
        (df2.join((df3.join(df4, ["timestamp", "room_id"])), ["timestamp", "room_id"])),
        ["timestamp", "room_id"],
    )
    fahrenheit_to_celsius = udf(lambda x: (x - 32) * (5 / 9), DoubleType())
    df = df.withColumn("temperature", fahrenheit_to_celsius(df["temperature"]))
    transformed_dataframe = transformation_operations(df)
    return transformed_dataframe


def transformation_operations(data_frame):
    count_timestamps = (data_frame.select("timestamp").distinct()).count()
    count_room_ID = (data_frame.select("room_id").distinct()).count()
    drop_nulls = (data_frame.dropna()).count()
    null_count = (data_frame.count()) - drop_nulls
    duplicate_room_id_count = 4 - count_room_ID
    if (count_timestamps == 1) and (null_count == 0) and (duplicate_room_id_count == 0):
        transformed_dataframe = data_frame
        logger.info("successfully transformed")
        column_order = [
            "timestamp",
            "room_id",
            "temperature",
            "humidity",
            "light_level",
            "co2",
            "humidity_ratio",
        ]
        transformed_dataframe = transformed_dataframe.select(*column_order)
        return transformed_dataframe
    else:
        error_info = f"{null_count} null values, {count_timestamps} timestamps found,{duplicate_room_id_count} duplicate room ID's in the dataframe."
        logger.info(f" transformation failed, error info: {error_info}")


# driver function initiator function
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    KAFKA_BROKER_URL = os.environ.get("KAFKA_BOOTSTRAP_SERVER")
    Producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER_URL,
        value_serializer=lambda encoder: json.dumps(encoder).encode("utf8"),
        api_version=(0, 10, 1),
    )
    spark = SparkSession.builder.appName("Transformation").getOrCreate()
    smart_thermo_data_buffer = queue.Queue()
    luxmeter_data_buffer = queue.Queue()
    carbonsense_data_buffer = queue.Queue()
    moisturemate_data_buffer = queue.Queue()
    loop()
