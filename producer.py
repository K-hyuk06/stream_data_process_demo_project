from kafka import KafkaProducer
import json
import datetime


class Producer:
    def __init__(self, kafka_server):
        self.producer = KafkaProducer(
            key_serializer=str.encode,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            bootstrap_servers=kafka_server,
        )

    def produce_room_record(self, record):
        key = record.key
        value = record.value
        self.producer.send("room", key=key, value=value)

    def produce_entrance_record(self, record):
        key = record.key
        value = record.value
        self.producer.send("entrace", key=key, value=value)


if __name__ == "__main__":
    producer = KafkaProducer(
        key_serializer=str.encode,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        bootstrap_servers="localhost:9092",
    )
    key = "Jane"
    value = {
        "name": "Jane",
        "type": "in",
        "time": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }
    pass
