import datetime
import time


class EntranceRecord:
    def __init__(self, name, record_type, record_time):
        self.name = name
        self.type = record_type
        self.time = datetime.datetime.strptime(record_time, "%Y-%m-%d %H:%M:%S")

    def calculate(self, time_val):
        return abs(self.time.timestamp() - time_val)


class EntranceRealTimeProcess:
    def __init__(self):
        self.save = {}
        self.time_limit = 5

    def process_record(self, record: EntranceRecord):

        come_in = False

        if record.name not in self.save:
            self.save[record.name] = record
        else:
            out_time = record.calculate(self.save[record.name].time.timestamp())
            come_in = True
            print(out_time)

        if come_in and out_time > self.time_limit:
            print(f"{record.name} 씨는 너무 밖에 오래 있었습니다. 경고입니다")
        if come_in:
            self.delete_record(record.name)

    def check_present_time(self):
        present_time = time.time()

        for key, record in self.save.items():
            out_time_val = record.calculate(present_time)
            print(out_time_val)
            if out_time_val >= self.time_limit:
                print(f"{key}는 사무실에 돌아오지 않았습니다. ")

    def delete_record(self, name):
        self.save.pop(name)


class RoomRecord:
    def __init__(self, room_no, status):
        self.room_no = room_no
        self.status = status

    def get_room_no(self):
        return self.room_no

    def get_status(self):
        return self.status


class RoomAvgRecord:
    def __init__(self, room_no):
        self.room_no = room_no
        self.avg_status = {}

    def update(self, status):

        for sensor_name in status.keys():
            if sensor_name not in self.avg_status:
                self.avg_status[sensor_name] = {
                    "min": status[sensor_name],
                    "max": status[sensor_name],
                    "mean": status[sensor_name],
                    "count": 1,
                }
            else:
                self.avg_status[sensor_name]["min"] = min(
                    self.avg_status[sensor_name]["min"], status[sensor_name]
                )
                self.avg_status[sensor_name]["max"] = max(
                    self.avg_status[sensor_name]["max"], status[sensor_name]
                )
                self.avg_status[sensor_name]["mean"] = (
                    self.avg_status[sensor_name]["count"]
                    * self.avg_status[sensor_name]["mean"]
                    + status[sensor_name]
                ) / (self.avg_status[sensor_name]["count"] + 1)
                self.avg_status[sensor_name]["count"] += 1

    def clean(self):
        self.avg_status = {}


class RoomAvgContainer:
    def __init__(self):
        self.containers = {}

    def set_room_record(self, roomRecord: RoomRecord):
        room_no = roomRecord.get_room_no()
        status = roomRecord.status
        if room_no not in self.containers:
            self.containers[room_no] = RoomAvgRecord(room_no)
            self.containers[room_no].update(status)
        else:
            self.containers[room_no].update(status)

    def clean(self):
        for key in self.containers.keys():
            self.containers[key].clean()


class RoomConditionRecord:
    def __init__(self, room_no, schedule_id, conditions, order):
        self.condition = conditions
        self.room_no = room_no
        self.schedule_id = schedule_id
        self.order = order  # insert,update,delete

    def set_conditions(self, conditions):
        self.condition = conditions

    def get_condition(self):
        return self.condition

    def get_room_no(self):
        return self.room_no

    def get_schedule_id(self):
        return self.schedule_id

    def get_order(self):
        return self.order


class RoomConditionContainer:
    def __init__(self):
        self.containers = {}

    def process(self, roomCondition: RoomConditionRecord):
        order = roomCondition.get_order()
        if order == "insert":
            self.set_condition(roomCondition=roomCondition)
        elif order == "update":
            self.update_condition(roomCondition=roomCondition)
        elif order == "delete":
            self.delete_condition(roomCondition=roomCondition)

    def set_condition(self, roomCondition: RoomConditionRecord):
        room_no = roomCondition.get_room_no()
        schedule_id = roomCondition.get_schedule_id()
        condition = roomCondition.get_condition()
        if room_no not in self.containers:
            self.containers[room_no] = {}
            self.containers[room_no][schedule_id] = condition
        else:
            self.containers[room_no][schedule_id] = condition

    def delete_condition(self, roomCondition: RoomConditionRecord):
        result = False
        detail = None
        try:
            room_no = roomCondition.get_room_no()
            schedule_id = roomCondition.get_schedule_id()
            schedules = self.containers[room_no]
            schedules.pop(schedule_id)
            result = True

        except Exception as e:
            detail = str(e)

        return {"result": result, "detail": detail}

    def update_condition(self, roomCondition: RoomConditionRecord):
        result = False
        detail = None
        try:
            room_no = roomCondition.get_room_no()
            schedule_id = roomCondition.get_schedule_id()
            condition = roomCondition.get_condition()
            self.containers[room_no][schedule_id] = condition
            result = True

        except Exception as e:
            detail = str(e)

        return {"result": result, "detail": detail}


# {
#     "temperature": {"agg_type": "min", "up": 30, "down": None},
#     "humidity": {"agg_type": "mean", "up": 25, "down": 40},
# }

if __name__ == "__main__":
    record1 = EntranceRecord("Jane", "in")
    time.sleep(10)
    record2 = EntranceRecord("Jane", "out")

    print(abs(record1.calculate(record2)))

    roomRecord = RoomRecord(1, {"temperature": 10, "humidity": 30})
    roomAvgContainer = RoomAvgContainer()
    roomAvgContainer.set_room_record(roomRecord=roomRecord)

    key = str(1)
    value = {
        "schedule_id": 1,
        "order": "insert",
        "condition": {
            "temperature": {"agg_type": "min", "up": 20, "down": None},
            "humidity": {"agg_type": "mean", "up": 20, "down": None},
        },
    }

    roomCondition = RoomConditionRecord(
        room_no=int(key),
        schedule_id=value["schedule_id"],
        order=value["order"],
        conditions=value["condition"],
    )

    roomConditionContainer = RoomConditionContainer()
    roomConditionContainer.process(roomCondition)
