import time
import random
from datetime import datetime
from datetime import timezone
from iot_data import TIMESTAMP
from iot_data import IotDataProvider

import search_engine


def now_iso_utc():
    return datetime.now(timezone.utc).isoformat()

def create_log_messages():
    logs = ['INFO: user logged in', "INFO: user logged out", "INFO: New entity created",
            "DEBUG: Database connection open", "INFO: User registration complete", "DEBUG: Closed db connection"]
    logs *= 20
    logs += ["ERROR: no left space on device"]
    random.shuffle(logs)
    return logs


LOG_MESSAGES = create_log_messages()


def send_log_message(timestamp, iteration, source_id):
    current_index = iteration % len(LOG_MESSAGES)
    message = LOG_MESSAGES[current_index]
    message = f'{message} {iteration}'
    document = {
        'message': message,
        'source_id': source_id,
        TIMESTAMP: timestamp.isoformat()
    }
    search_engine.create_document('generated_logs', [document])

def send_blob(timestamp, source_id, index):
    departments = ['it', 'hr', 'qa', 'ops']
    projects = ['OS-security', 'OS-ML', 'OS-SQL', 'Dashboards']
    current_department = departments[index % len(departments)]
    current_project = projects[index % len(projects)]
    blob = f'{{"id":"{index}", "source":"{source_id}", "department":"{current_department}", "project":"{current_project}" }}"'
    message = {
        TIMESTAMP: timestamp.isoformat(),
        'device-id': source_id,
        'id':index,
        'blob':blob
    }
    search_engine.create_document('blobs', [message])

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    epoch_seconds = int(time.time())
    # send some historic data
    list_data_to_send = []
    for i in range(48 * 3600):
        epoch_seconds -= 1
        now = datetime.fromtimestamp(epoch_seconds, timezone.utc)
        list_data_to_send += IotDataProvider.bulk_data_generate(now, epoch_seconds)
        if i % 200 == 0:
            search_engine.send_iot_data(list_data_to_send)
            print(f'Send {len(list_data_to_send)} historic data')
            list_data_to_send = []
    while True:
        try:
            epoch_seconds = int(time.time())
            now = datetime.fromtimestamp(epoch_seconds, timezone.utc)
            list_data_to_send = IotDataProvider.bulk_data_generate(now, epoch_seconds)
            search_engine.send_iot_data(list_data_to_send)
            send_log_message(now, epoch_seconds, 512)
            send_log_message(now, epoch_seconds, 256)
            send_log_message(now, epoch_seconds, 257)
            send_blob(now, 256, epoch_seconds)
            print("Data sent to search engine")
            time.sleep(1)
        except Exception as e:
            print(f"Something went wrong: '{str(e)}'")
            time.sleep(1)






