from kafka import KafkaConsumer
import time
from record import *


class RoomRealTimeProcess:
    def __init__(self):
        self.roomAvgContainer = RoomAvgContainer()
        self.roomConditionContainer = RoomConditionContainer()

    def process(self, roomRecord=None, roomConditionRecord=None):
        if roomRecord != None:
            self.roomAvgContainer.set_room_record(roomRecord)

        if roomConditionRecord != None:
            self.roomConditionContainer.process(roomConditionRecord)

    def check_condition_meet(self):
        ruleContainers = self.roomConditionContainer.containers
        for room_no, rule in ruleContainers.items():

            avg_status = self.roomAvgContainer.containers[room_no].avg_status
            # {'temperature': {'min': 10, 'max': 10, 'mean': 10, 'count': 1}, 'humidity': {'min': 30, 'max': 30, 'mean': 30, 'count': 1}}

            for schedule_id, condition in rule.items():
                for sensor_name, detail_condition in condition.items():
                    obj_value = avg_status[sensor_name][detail_condition["agg_type"]]

                    rule_out = False

                    if detail_condition["up"]:
                        pass
                    elif obj_value <= detail_condition["up"]:
                        rule_out = True

                    if detail_condition["down"]:
                        pass
                    elif obj_value > detail_condition["down"]:
                        rule_out = True

                    if rule_out:
                        print(f"room :{room_no}, schedule : {schedule_id} break")

                    pass
            # {1: {'temperature': {'agg_type': 'min', 'up': 20, 'down': None}, 'humidity': {'agg_type': 'mean', 'up': 20, 'down': None}}}

    def process_sensor_record(
        self,
    ):
        pass

    def process_condition(
        self,
    ):
        pass


def check_room_iot():
    room_consumer = KafkaConsumer()

    one_minute = 5
    next_check = time.time() + one_minute

    while True:
        pass


if __name__ == "__main__":
    roomRecord = RoomRecord(room_no=1, status={"temperature": 30})

    roomRealTimeProcess = RoomRealTimeProcess()
