# python3.6

import random
import os
from paho.mqtt import client as mqtt_client
from pathlib import Path
from mongo_helper import add_temperature_records,add_backgroundjob
import shutil

broker = 'localhost'
port = 1883

# Generate a Client ID with the subscribe prefix.
client_id = f'subscribe-{random.randint(0, 100)}'
# username = 'emqx'
# password = 'public'

source = '/home/nvidia/camera_stream'
destination = '/home/nvidia/mqttpaho/input_data'

def connect_mqtt() -> mqtt_client:
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print("Connected to MQTT Broker!")
        else:
            print("Failed to connect, return code %d\n", rc)

    client = mqtt_client.Client(client_id)
    # client.username_pw_set(username, password)
    client.on_connect = on_connect
    client.connect(broker, port)
    return client

def publish(client,topic):
    msg_count = 1
    while True:
        time.sleep(1)
        msg = f"messages: {msg_count}"
        result = client.publish(topic, msg)
        # result: [0, 1]
        status = result[0]
        if status == 0:
            print(f"Send `{msg}` to topic `{topic}`")
        else:
            print(f"Failed to send message to topic {topic}")
        msg_count += 1
        if msg_count > 5:
            break


def subscribe(client: mqtt_client):
    def on_message(client, userdata, msg):
        if msg.topic == 'device1/filetransfer_status/':
            transfer_allfiles("device1")            
        elif msg.topic == "device1/sensor_data/":            
            add_temperature_records(msg.payload.decode())

        print(f"Received `{msg.payload.decode()}` from `{msg.topic}` topic")

    client.subscribe("device1/filetransfer_status/")
    client.subscribe("device1/sensor_data/")
    client.on_message = on_message

def transfer_allfiles(device1:str):    
    try:        
        shutil.rmtree(destination +"/"+device1, ignore_errors=False, onerror=None)
        print('Folder deleted')
    except:
        print("Folder doesn't exist")

    Path(destination +"/"+device1).mkdir(parents=True, exist_ok=True)    
    allfiles = os.listdir(source)
    # iterate on all files to move them to destination folder
    for f in allfiles:
        src_path = os.path.join(source, f)
        dst_path = os.path.join(destination +"/"+device1, f)              
        os.rename(src_path, dst_path)

    add_backgroundjob(destination +"/"+device1)




def run():
    client = connect_mqtt()
    subscribe(client)    
    client.loop_forever()


if __name__ == '__main__':
    run()