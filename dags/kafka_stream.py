from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import json
import requests
from kafka import KafkaProducer
import time
import logging
default_args = {
    "owner":'Raza',
    'start_date': datetime(2024, 7, 1)
}

def get_data():
    res = requests.get('https://randomuser.me/api/')
    res = res.json()
    res = res['results'][0]
    return res

def format_data(res):
    data = {}
    location = res['location']
    data['first_name'] = res['name']['first']
    data['last_name'] = res['name']['last']
    data['gender'] = res['gender']
    data['address'] = f"{str(location['street']['number'])} {location['street']['name']}, " \
                        f"{location['city']}, {location['state']}, {location['country']}"
    data['post_code'] = location['postcode']
    data['email'] = res['email']
    data['username'] = res['login']['username']
    data['dob'] = res['dob']['date']
    data['registered_date'] = res['registered']['date']
    data['phone'] = res['phone']
    data['picture'] = res['picture']['medium']
    
    return data

def stream_data():

    # print(json.dumps(data, indent=4))

    producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000)    
    
    current_time = time.time()
    while True:
        if time.time() > current_time + 60:
            break
        try:
            res = get_data()
            data = format_data(res)
            
            producer.send('users_created', json.dumps(data).encode('utf-8'))
        
        except Exception as e:
            logging.error('An error occudred: ', e)

dag = DAG(
    dag_id='random_names_kafka_stream',
    default_args=default_args,
    schedule_interval='@daily',
    tags=['random', 'kafka', 'streaming'],
    catchup=False
)

stream_names = PythonOperator(
    task_id='extract_from_reddit',
    python_callable=stream_data,
    dag=dag
)

stream_names