from kafka import KafkaConsumer
from record import EntranceRealTimeProcess, EntranceRecord
import time
import json


def realtime_entrance_processor():
    entrance_consumer = KafkaConsumer(
        "entrance",
        auto_offset_reset="latest",
        group_id="entrance_realtime",
        client_id="app1",
        bootstrap_servers=["localhost:9092"],
        key_deserializer=lambda k: k.decode("utf-8"),
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    )
    one_minute = 5
    next_check = time.time() + one_minute
    realTimeProcess = EntranceRealTimeProcess()

    while True:
        records = entrance_consumer.poll(2)
        for partition_info, messages in records.items():
            print(partition_info)
            for message in messages:
                print(message.value)
                try:
                    value = message.value

                    entranceRecord = EntranceRecord(
                        name=message.key,
                        record_type=value["record_type"],
                        record_time=value["value"],
                    )
                    realTimeProcess.process_record(entranceRecord)
                except Exception as e:
                    print(e)

        if next_check < time.time():
            realTimeProcess.check_present_time()
            next_check = time.time() + one_minute


if __name__ == "__main__":

    entrance_consumer = KafkaConsumer(
        "entrance",
        auto_offset_reset="latest",
        group_id="entrance_realtime",
        client_id="app1",
        bootstrap_servers=["localhost:9092"],
        key_deserializer=lambda k: k.decode("utf-8"),
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    )
    one_minute = 5
    next_check = time.time() + one_minute
    realTimeProcess = EntranceRealTimeProcess()

    while True:
        records = entrance_consumer.poll(2)
        for partition_info, messages in records.items():
            print(partition_info)
            for message in messages:
                print(message.value)
                try:
                    value = message.value

                    entranceRecord = EntranceRecord(
                        name=message.key,
                        record_type=value["record_type"],
                        record_time=value["value"],
                    )
                    realTimeProcess.process_record(entranceRecord)
                except Exception as e:
                    print(e)

        if next_check < time.time():
            realTimeProcess.check_present_time()
            next_check = time.time() + one_minute
