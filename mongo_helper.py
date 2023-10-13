# importing module
from pymongo import MongoClient
from bson import ObjectId
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
settingscollection=mydatabase["settings"]
eventscollection=mydatabase["events"]
lnccollection=mydatabase["lnc"]


cloudClient = MongoClient("mongodb://looke:looke123@107.23.147.153:27017/lookedb")  
cloudDatabase = cloudClient["lookedb"]  
exportCollection=cloudDatabase["exporter.channels"]
thingsCollection=cloudDatabase["things"]


def add_backgroundjob(record:any,device_destination_folder:str):    
    doc1 = {
        "exporterchannel": ObjectId(record["exporterchannel"]),
        "deck":record["deck"],
        "penId":record["pen"],
        "job_status":0,
        "deviceId":ObjectId(record["device_id"]),
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

    amonia_alert =4
    rh_alert =4
    wbt_alert =4
    temperature_alert =4
    for x in settingscollection.find({"exporterchannel" : record["exporterchannel"] }):
        if x["key"] == 'amonia_alert':
            amonia_alert = float(x["value"])
        elif x["key"] == "rhAlert":
            rh_alert = float(x["value"])
        elif x["key"] == "wbtAlert":
            wbt_alert = float(x["value"])
        elif x["key"] == "temperatureAlert":
            temperature_alert = float(x["value"])
        else:
            print("no match alert setting")

    is_event = False
    if amonia_alert <= float(record["NH3"]):
        record_event(record,1,float(record["NH3"]))
        is_event=True

    if rh_alert <= float(record["RH"]):
        record_event(record,4,float(record["RH"]))
        is_event=True

    if wbt_alert <= float(record["WBT"]):
        record_event(record,6,float(record["WBT"]))
        is_event=True

    if temperature_alert <= float(record["temperature"]):
        record_event(record,7,float(record["temperature"]))
        is_event=True


    doc1 = {
        "exporterchannel": ObjectId(record["exporterchannel"]),
        "deck":record["deck"],
        "penId":record["pen"],        
        "is_event":is_event,
        "deviceId":ObjectId(record["device_id"]),
        "device_name":record["device_name"],
        "device_thing":record["device_thing"],        
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



def record_event(record:any, event_type:any, event_value:any):
        event = {
        "exporterchannel": ObjectId(record["exporterchannel"]),
        "deck":record["deck"],
        "penId":record["pen"],                
        "deviceId":ObjectId(record["device_id"]),
        "device_name":record["device_name"],
        "device_thing":record["device_thing"],  
        "location":record["location"],
        "eventTime": datetime.datetime.now(),
        "value":event_value, 
        "eventType": event_type,
        "eventStatus":1,
        "is_synced":False   
        }
        eventscollection.insert_one(event)
        print("events captured")
    
def deletelnc():
     result = lnccollection.delete_many({})  


def initlnc():
        lnc = lnccollection.find_one({})
        print(lnc)
        if lnc is None:
            thing = thingsCollection.find_one({ 'thing_id': 'Edge_Device','is_registered':True })                
            if thing is None:
                 return False
            else:
                lnc =exportCollection.find_one({'_id' : ObjectId(thing["exporterchannel"])})                
                lnccollection.insert_one(lnc)
                print('inserted lnc')
                return True
        else:
             print(lnc["updatedAt"])
        
        return False



initlnc()
#deletelnc()