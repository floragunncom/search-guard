import random
import math


TIMESTAMP = 'timestamp'

class IotDataProvider:

    @staticmethod
    def compute_temperature_in_iteration(amplitude, current_iteration, period):
        iteration_step = (2 * math.pi) / period
        return math.sin(current_iteration * (iteration_step % period)) * amplitude

    @staticmethod
    def random_iot_data(timestamp, device_id):
        temperature = random.randint(0, 30)
        humidity = IotDataProvider.compute_humidity_in_iteration()
        return IotDataProvider.create_iot_data(device_id, humidity, temperature, timestamp)

    @staticmethod
    def sin_iot_data(timestamp, device_id, period, current_iteration, amplitude):
        value = IotDataProvider.compute_temperature_in_iteration(amplitude, current_iteration, period)
        return IotDataProvider.create_iot_data(device_id, IotDataProvider.compute_humidity_in_iteration(), value, timestamp)

    @staticmethod
    def compute_humidity_in_iteration():
        return random.randint(5, 20)

    @staticmethod
    def create_iot_data(device_id, humidity, temperature, timestamp):
        measurement = {
            'device-id': device_id,
            'name': f'my_name_{device_id}',
            'temperature': temperature,
            'humidity': humidity,
            TIMESTAMP: timestamp.isoformat()
        }
        return measurement

    @staticmethod
    def bulk_data_generate(now, epoch_seconds):
        return [#IotDataProvider.random_iot_data(now, 1024),
                IotDataProvider.sin_iot_data(now, 250, 120, epoch_seconds, 14),
                IotDataProvider.sin_iot_data(now, 251, 120, epoch_seconds, 11),
                IotDataProvider.sin_iot_data(now, 252, 120, epoch_seconds, 9),
                IotDataProvider.sin_iot_data(now, 253, 120, epoch_seconds, 7),
                IotDataProvider.sin_iot_data(now, 254, 120, epoch_seconds, 5),
                IotDataProvider.sin_iot_data(now, 255, 600, epoch_seconds, 5),
                IotDataProvider.sin_iot_data(now, 256, 300, epoch_seconds, 30),
                IotDataProvider.sin_iot_data(now, 257, 600, epoch_seconds, 30),
                IotDataProvider.sin_iot_data(now, 258, 90, epoch_seconds, 30),
                IotDataProvider.sin_iot_data(now, 259, 180, epoch_seconds, 32),
                IotDataProvider.sin_iot_data(now, 512, 60, epoch_seconds, 25)]