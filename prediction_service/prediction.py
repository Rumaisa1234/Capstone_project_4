import json
import logging
import pickle

import pandas as pd
from kafka import KafkaConsumer, KafkaProducer

transformed_dataframe_consumer = None
predicted_data_producer = None


def json_deserializer(data):
    return json.loads(data.decode("utf-8"))


def json_serializer(data):
    return json.dumps(data).encode("utf-8")


def get_predictions(consumer):
    for messages in consumer:
        for message in messages.value:
            transformed_df = pd.DataFrame([message])
            tmp_df = transformed_df.drop(["timestamp", "room_id"], axis=1)
            predictions = model.predict(tmp_df)
            pred_column = pd.DataFrame(predictions, columns=["occupancy"])
            final_df = pd.concat([transformed_df, pred_column], axis=1)
            publish_to_kafka(final_df)
            logger.info("successfully sent our predicted data {final_df} to kafka")


def publish_to_kafka(dataframe):
    json_message = dataframe.to_json(orient="records")
    final_message = json.loads(json_message)
    predicted_data_producer.send("predicted_data", final_message)
    predicted_data_producer.flush()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger()
    model = pickle.load(open("./model.pkl", "rb"))
    transformed_dataframe_consumer = KafkaConsumer(
        "transformed_dataframe",
        bootstrap_servers=["broker:29092"],
        value_deserializer=json_deserializer,
        auto_offset_reset="latest",
        enable_auto_commit=True,
    )
    predicted_data_producer = KafkaProducer(
        bootstrap_servers=["broker:29092"], value_serializer=json_serializer
    )
    get_predictions(transformed_dataframe_consumer)
