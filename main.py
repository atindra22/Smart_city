# from pyspark.sql import SparkSession
# from config.config import configuration
# from pyspark.sql.types import StructType
# from pyspark.sql.types import StructField
# from pyspark.sql.types import StringType
# from pyspark.sql.types import DoubleType
# from pyspark.sql.types import DateType

import os
from confluent_kafka import SerializingProducer
import simplejson as json

from datetime import datetime, timedelta
import random
import uuid
import time


#setting up the environment


# if __name__ == "__main__":
#     spark = (SparkSession.builder.appName('smart_city')
#             .config('spark.jars.packages',
#                      'prg.apache.hadoop:hadoop-aws:3.3.1',
#                      'com.amazonaws:aws-java-sdk:1.11.469')
#             .config('spark.hadoop.fs.s3a.impl','org.apache.hadoop.fs.s3a.S3AFileSystem')
#             .config('spark.hadoop.fs.s3a.access.key', configuration.get('AWS_ACCESS_KEY'))
#             .config('spark.hadoop.fs.s3a.secret.key', configuration.get('AWS_SECRET_KEY'))
#             .config('spark.hadoop.fs.s3a.aws.credentials.provider','org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')
#             .getOrCreate()
#             )
    

#inputting data
    # text_input_dir = 'file://C:\Users\Atindra\Desktop\Smart_city\input'
    # json_input_dir = 'file://C:\Users\Atindra\Desktop\Smart_city\input\input_json'
    # csv_input_dir = 'file://C:\Users\Atindra\Desktop\Smart_city\input\input_csv'
    # pdf_input_dir = 'file://C:\Users\Atindra\Desktop\Smart_city\input\input_pdf'
    # video_input_dir = 'file://C:\Users\Atindra\Desktop\Smart_city\input\input_video'
    # img_input_dir = 'file://C:\Users\Atindra\Desktop\Smart_city\input\input_image'

#creating data schema
    # data_schema = StructType([ 
    #     StructField('file_name', StringType(), True),
    #     StructField('position', StringType(), True),
    #     StructField('classcode', StringType(), True),
    #     StructField('salary_start', DoubleType(), True),
    #     StructField('salary_end', DoubleType(), True),
    #     StructField('start_date', DateType(), True),
    #     StructField('end_date', DateType(), True),
    #     StructField('req', StringType(), True),
    #     StructField('notes', StringType(), True),
    #     StructField('duties', StringType(), True),
    #     StructField('selection', StringType(), True),
    #     StructField('experience_length', StringType(), True),
    #     StructField('job_type', StringType(), True),
    #     StructField('education_length', StringType(), True),
    #     StructField('school_type', StringType(), True),
    #     StructField('application_location', StringType(), True),
    # ])

    

LONDON_COORDINATES = {"latitude": 51.5074,"longitude": -0.1278}
BIRMINGHAM_COORDINATES = {"latitude": 52.4862,"longitude": -1.8904}

#calculate the movement increments

LATITUDE_INCREMENT = (BIRMINGHAM_COORDINATES['latitude'] - LONDON_COORDINATES['latitude'])/100
LONGITUDE_INCREMENT = (BIRMINGHAM_COORDINATES['longitude'] - LONDON_COORDINATES['longitude'])/100

# Environment variables for configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
VEHICLE_TOPIC = os.getenv('VEHICLE_TOPIC', 'vehicle_data')
GPS_TOPIC = os.getenv('GPS_TOPIC', 'gps_data')
TRAFFIC_TOPIC = os.getenv('TRAFFIC_TOPIC', 'traffic_data')
WEATHER_TOPIC = os.getenv('WEATHER_TOPIC', 'weather_data')
EMERGENCY_TOPIC = os.getenv('EMERGENCY_TOPIC','emergency_data')

random.seed(42)

start_time = datetime.now()
start_location = LONDON_COORDINATES.copy()

def get_next_time():
    global start_time
    start_time += timedelta(seconds=random.randint(30,60)) #update frequency
    return start_time

def generate_gps_data(device_id, timestamp, vehicle_type='private'):
    return{
        'id': uuid.uuid4(),
        'deviceId': device_id,
        'timestamp': timestamp,
        'speed': random.uniform(0,40), #km/h
        'direction': 'North-East',
        'vehicle-type': vehicle_type
    }

def generate_traffic_camera_data(device_id, timestamp,location):
    return{
        'id': uuid.uuid4(),
        'deviceId': device_id,
        # 'cameraId': camera_id,
        'location': location,
        'timestamp': timestamp,
        'snapshot': 'Base64EncodedString'

    }

def generate_weather_data(device_id, timestamp, location):
    return{
        'id': uuid.uuid4(),
        'deviceId': device_id,
        'location': location,
        'timestamp': timestamp,
        'temperature': random.uniform(-5, 26),
        'weather_condition': random.choice(['Sunny', 'Cloudy', 'Rain', 'Snow']),
        'precipitaion': random.uniform(0,25),
        'windspeed': random.uniform(0,100),
        'humidity': random.randint(0,100),
        'air_quality_index':random.uniform(0,500) #AQI value

    }

def generate_emergency_incident_data(device_id, timestamp, location):
    return{
        'id': uuid.uuid4(),
        'deviceId': device_id,
        'incidentId': uuid.uuid4(),
        'type': random.choice(['Accident', 'Fire', 'Medical', 'Police', 'None']),
        'timestamp': timestamp,
        'location': location,
        'status': random.choice(['Active', 'Resolved']),
        'description': 'Description of the incident'
    }


def simulate_vehicle_movement():
    global start_location
    #start moving towards birmingham
    start_location['latitude'] += LATITUDE_INCREMENT
    start_location['longitude'] += LONGITUDE_INCREMENT

    #add some randomness to simulate actual road travel
    start_location['latitude'] += random.uniform(-0.0005, 0.0005)
    start_location['longitude'] += random.uniform(-0.0005, 0.0005)

    return start_location


def generate_vehicle_data(device_id):
    location = simulate_vehicle_movement()
    return {
        'id': uuid.uuid4(),
        'deviceId': device_id,
        'timestamp': get_next_time().isoformat(),
        'location': (location['latitude'], location['longitude']),
        'speed': random.uniform(10,40),
        'direction': 'North-East',
        'make': 'BMW',
        'model': 'C500',
        'year' : 2024,
        'fueltype': 'Hybrid'
    }

def json_serealizer(obj):
    if isinstance(obj,uuid.UUID):
        return str(obj)
    raise TypeError(f'Object of type {obj.__class__.__name__} is not a JSON serializable')

def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')


def produce_data_to_kafka(producer, topic, data):
    producer.produce(
        topic,
        key=str(data['id']),
        value=json.dumps(data, default=json_serealizer).encode('utf-8'),
        on_delivery=delivery_report
    )

def simulate_journey(producer, device_id):
    while True:
        vehicle_data = generate_vehicle_data(device_id)
        gps_data = generate_gps_data(device_id, vehicle_data['timestamp'])
        traffic_camera_data = generate_traffic_camera_data(device_id, vehicle_data['timestamp'], vehicle_data['location'])
        weather_data = generate_weather_data(device_id, vehicle_data['timestamp'], vehicle_data['location'])
        emergency_incident_data = generate_emergency_incident_data('device_id', vehicle_data['timestamp'],vehicle_data['location'] )

        if(vehicle_data['location'][0] >= BIRMINGHAM_COORDINATES['latitude']
           and vehicle_data['location'][1] <= BIRMINGHAM_COORDINATES['longitude']):
            print('vehicle has reached Birmingham. Simulation ending...')
            break
        # print(vehicle_data)
        # print(gps_data)
        # print(traffic_camera_data)
        # print(weather_data)
        # print(emergency_incident_data)

        produce_data_to_kafka(producer, VEHICLE_TOPIC, vehicle_data)
        produce_data_to_kafka(producer, GPS_TOPIC, gps_data)
        produce_data_to_kafka(producer, TRAFFIC_TOPIC, traffic_camera_data)
        produce_data_to_kafka(producer, WEATHER_TOPIC, weather_data)
        produce_data_to_kafka(producer, EMERGENCY_TOPIC, emergency_incident_data)

        time.sleep(1)



if __name__ == "__main__":
    producer_config = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'error_cb': lambda err: print(f'Kafka error: {err}')


    }
    producer = SerializingProducer(producer_config)

    try:
        simulate_journey(producer, 'vehicle-altroz')

    except KeyboardInterrupt:
        print('simulation ended by the user')
    except Exception as e:
        print(f'Unexpected Error occured: {e}')