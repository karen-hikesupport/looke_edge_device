# python3.6

import random
import os
from paho.mqtt import client as mqtt_client
from pathlib import Path
from mongo_helper import add_temperature_records,add_backgroundjob
import shutil,json

broker = 'localhost'
port = 1883

# Generate a Client ID with the subscribe prefix.
client_id = f'subscribe'
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

    def on_disconnect(client, userdata, rc):    
        print("disconnected OK")

    client = mqtt_client.Client(client_id)
    # client.username_pw_set(username, password)
    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
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
        print(f"Received `{msg.payload.decode()}` from `{msg.topic}` topic")

    

    client.subscribe("$looke/#")    
    client.on_message = on_message
    client.message_callback_add("$looke/sensor_data_status/+", on_message_sensordata)
    client.message_callback_add("$looke/filetransfer_status/+", on_message_filestatus)
    

def transfer_allfiles(recordStr:str): 
    record =json.loads(recordStr)     
    try:        
        shutil.rmtree(destination +"/"+record["device_id"], ignore_errors=False, onerror=None)
        print('Folder deleted')
    except:
        print("Folder doesn't exist")

    Path(destination +"/"+record["device_id"]).mkdir(parents=True, exist_ok=True)    
    
    allfiles = record["files"]
    # iterate on all files to move them to destination folder
    for f in allfiles:
        try:  
            src_path = os.path.join(source, f)
            dst_path = os.path.join(destination +"/"+record["device_id"], f)              
            os.rename(src_path, dst_path)
        except:
            print("record file are not exist")
    device_destination_folder =destination +"/"+record["device_id"]
    add_backgroundjob(record,device_destination_folder)


def on_message_filestatus(client, userdata, msg):
    print("on_message_filestatus")
    record =json.loads(msg.payload.decode())
    print(record)
    transfer_allfiles(msg.payload.decode())  
    #print(f"Received `{msg.payload.decode()}` from `{msg.topic}` topic")

def on_message_sensordata(client, userdata, msg):
    print("on_message_sensordata")
    add_temperature_records(msg.payload.decode()) 
    print(f"Received `{msg.payload.decode()}` from `{msg.topic}` topic")


def run():
    client = connect_mqtt()
    subscribe(client)    
    client.loop_forever()


if __name__ == '__main__':
    run()