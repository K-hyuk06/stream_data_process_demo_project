from flask import Flask, request
from record import EntranceRecord
import json
from kafka import KafkaProducer
import datetime

app = Flask(__name__)
producer = KafkaProducer(
    key_serializer=str.encode,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    bootstrap_servers="localhost:9092",
)


@app.route("/entrance", methods=["POST"])
def entrace():
    try:
        record = json.loads(request.data)

        producer.send(
            "entrance",
            key=record["name"],
            value={
                "record_type": record["value"],
                "value": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            },
        )

        return {"success": True}
    except Exception as e:
        return {"success": False, "detail": str(e)}


@app.route("/office_status", methods=["POST"])
def office_status():
    try:
        record = json.loads(request.data)
        print(f"key:{record['room_no']},value:{record['value']}")

        # producer.send(
        #     "office_status",
        #     key=str(record["room_no"]),
        #     value=record["value"],
        # )

        return {"success": True}
    except Exception as e:
        return {"success": False, "detail": str(e)}


@app.route("/office_rule", methods=["POST"])
def office_rule():
    try:
        record = json.loads(request.data)
        print(f"key:{record['room_no']},value:{record['value']}")

        # producer.send(
        #     "office_rule",
        #     key=str(record["room_no"]),
        #     value=record["value"],
        # )

        return {"success": True}
    except Exception as e:
        return {"success": False, "detail": str(e)}


app.run()
