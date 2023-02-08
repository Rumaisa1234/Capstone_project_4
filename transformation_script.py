import json
import queue
import threading
import time

from kafka import KafkaConsumer, KafkaProducer
from pyspark.sql import SparkSession
from pyspark.sql.types import *

# Driver function
if __name__ == "__main__":
    Producer = KafkaProducer(
        bootstrap_servers=["broker:29092"],
        value_serializer=lambda x: json.dumps(x).encode("utf8"),
        api_version=(0, 10, 1),
    )
    spark = SparkSession.builder.appName("Transformation").getOrCreate()
    data_buffer = queue.Queue()
    loop()


# Data decoding that was encrypted during kafka publishing by the producer
def json_deserializer(data):
    return json.loads(data.decode("utf-8"))


def carbonsense_sensor_consumer():
    consumer = KafkaConsumer(
        "carbonsense",
        bootstrap_servers=["broker:29092"],
        value_deserializer=json_deserializer,
        auto_offset_reset="latest",
        enable_auto_commit=True,
    )
    consumption_of_data(consumer)


def moisturemate_sensor_consumer():
    consumer = KafkaConsumer(
        "moisturemate",
        bootstrap_servers=["broker:29092"],
        value_deserializer=json_deserializer,
        auto_offset_reset="latest",
        enable_auto_commit=True,
    )
    consumption_of_data(consumer)


def luxmeter_sensor_consumer():
    consumer = KafkaConsumer(
        "luxmeter",
        bootstrap_servers=["broker:29092"],
        value_deserializer=json_deserializer,
        auto_offset_reset="latest",
        enable_auto_commit=True,
    )
    consumption_of_data(consumer)


def smart_thermo_sensor_consumer():
    consumer = KafkaConsumer(
        "luxmeter",
        bootstrap_servers=["broker:29092"],
        value_deserializer=json_deserializer,
        auto_offset_reset="latest",
        enable_auto_commit=True,
    )
    consumption_of_data(consumer)


def consumption_of_data(consumer):
    while True:
        messages = []
        for message in consumer:
            messages.append(message.value)
            if len(messages) == 4:
                dataframe = create_dataframe(messages)
                data_buffer.put(dataframe)
                break


def create_dataframe(JSON_message):
    dataframe = spark.createDataFrame(JSON_message)
    dataframe.createOrReplaceTempView("dataframe")
    return dataframe


def kafka_consumer_executor():
    t1 = threading.Thread(target=carbonsense_sensor_consumer)
    t2 = threading.Thread(target=moisturemate_sensor_consumer)
    t3 = threading.Thread(target=luxmeter_sensor_consumer)
    t4 = threading.Thread(target=smart_thermo_sensor_consumer)
    t3.start()
    time.sleep(60)
    t1.start()
    t2.start()
    t4.start()


def send_to_kafka(df):
    json_data = df.toJSON().collect()
    for data in json_data:
        Producer.send("readcarbonsense", value=data)
        Producer.flush()


def loop():
    kafka_consumer_executor()
    while True:
        length_of_buffer = data_buffer.qsize()
        if length_of_buffer == 3:
            carbonsense_dataframe = data_buffer.get()
            moisturemate_dataframe = data_buffer.get()
            luxmeter_dataframe = data_buffer.get()
            smart_thermo_dataframe = data_buffer.get()
            transformed_dataframe = transformation(
                carbonsense_dataframe,
                moisturemate_dataframe,
                luxmeter_dataframe,
                smart_thermo_dataframe,
            )
            send_to_kafka(transformed_dataframe)


def transformation(df1, df2, df3):
    # df=df1.join((df2.join(df3,["timestamp","room_id"])),["timestamp","room_id"])
    df = df1.join(
        (df2.join((df3.join(df4, ["timestamp", "room_id"])), ["timestamp", "room_id"])),
        ["timestamp", "room_id"],
    )
    transformed_dataframe = transformation_operations(df)
    return transformed_dataframe


def transformation_operations(data_frame):
    count_timestamps = (data_frame.select("timestamp").distinct()).count()
    drop_nulls = (data_frame.dropna()).count
    if (count_timestamps == 1) and (drop_nulls == data_frame.count()):
        transformed_dataframe = data_frame
        # casting types can be done here
        return transformed_dataframe
