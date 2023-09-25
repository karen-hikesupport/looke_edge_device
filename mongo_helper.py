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


def add_backgroundjob(record:any,device_destination_folder:str):    
    doc1 = {
        "exporterchannel": record["exporterchannel"],
        "deck":record["deck"],
        "penId":record["pen"],
        "job_status":0,
        "deviceId":record["device_id"],
        "location":record["location"],
        "eventTime": datetime.datetime.now(),
        "video_image_path":device_destination_folder,     
        "files":record["files"],
        "device_tasks":record["tasks"],
        "device_configuration":record["config"]              
    }
    jobcollection.insert_one(doc1)


def add_temperature_records(recordStr:str):
    record =json.loads(recordStr)
    #print(record["sensor_data"]["raw_temperature"])
    doc1 = {
        "exporterchannel": record["exporterchannel"],
        "deck":record["deck"],
        "penId":record["pen"],        
        "is_event":False,
        "deviceId":record["device_id"],
        "location":record["location"],
        "eventTime": datetime.datetime.now(),
        "temperature":record["temperature"],
        "RH":record["RH"],
        "WBT":record["WBT"], 
        "NH3":record["NH3"],
        "lattitude":37.8136,
        "longitude":144.9631,
        "CO2":record["CO2"],
        "CH4":record["CH4"],
        "is_synced":False       
    }
    recordcollection.insert_one(doc1)