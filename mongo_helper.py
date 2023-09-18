# importing module
from pymongo import MongoClient
import socket
import json
import datetime
import os
  

# creation of MongoClient
client=MongoClient()  
# Connect with the portnumber and host
client = MongoClient("mongodb://localhost:27017/")  
# Access database
mydatabase = client["db"]  
# Access collection of the database
recordcollection=mydatabase["records"]
jobcollection=mydatabase["jobqueue"]


exportChannel = os.getenv('channel_id')
device_id = os.getenv('device_id')
deck_id = os.getenv('deck_id')
pen_id = os.getenv('pen_id')



def add_backgroundjob(filepath:str):    
    doc1 = {
        "exporterchannel": exportChannel,
        "deck":"1",
        "penId":"1",
        "device_id":"1",
        "record_time": datetime.datetime.now(),
        "device_filepath":filepath               
    }
    jobcollection.insert_one(doc1)


def add_temperature_records(recordStr:str):
    record =json.loads(recordStr)
    doc1 = {
        "exporterchannel": exportChannel,
        "deck":"1",
        "penId":"1",
        "is_event":False,
        "eventTime": datetime.datetime.now(),
        "temperature":record["raw_temperature"],
        "RH":record["raw_humidity"],
        "WBT":"2",        
    }
    recordcollection.insert_one(doc1)