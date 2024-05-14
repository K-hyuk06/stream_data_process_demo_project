from kafka import KafkaConsumer

consumer = KafkaConsumer(
    "my-topic", group_id="my-group", bootstrap_servers=["localhost:9092"]
)

consumer.poll()