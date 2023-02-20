import json
import threading

from kafka import KafkaConsumer

from flask import Flask, render_template


def json_deserializer(data):
    return json.loads(data.decode("utf-8"))


app = Flask(__name__)

latest_messages = []


def consume_messages():
    global latest_messages

    carbon_consumer = KafkaConsumer(
        "predicted_data",
        bootstrap_servers=["broker:29092"],
        value_deserializer=json_deserializer,
        auto_offset_reset="latest",
        enable_auto_commit=True,
    )

    for message in carbon_consumer:

        # Save the message to the latest_messages list
        latest_messages.append(message.value)
        latest_messages = latest_messages[-4:]


def start_message_consumer():
    consumer_thread = threading.Thread(target=consume_messages)
    consumer_thread.daemon = True
    consumer_thread.start()


@app.route("/")
def get_my_data():
    return render_template("index.html", messages=latest_messages)


@app.route("/latest_data")
def latest_data():
    return render_template("latest_data.html", messages=latest_messages)


if __name__ == "__main__":
    start_message_consumer()
    app.run(host="0.0.0.0", port=5000)
