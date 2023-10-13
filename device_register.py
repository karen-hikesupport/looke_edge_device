# importing module
from pymongo import MongoClient
import socket
  
# creation of MongoClient
client=MongoClient()
  
# Connect with the portnumber and host
client = MongoClient("mongodb://localhost:27017/")
  
# Access database
mydatabase = client["db"]
  
# Access collection of the database
mycollection=mydatabase["devices"]
  

def get_local_ip():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        # doesn't even have to be reachable
        s.connect(('192.255.255.255', 1))
        IP = s.getsockname()[0]
    except:
        IP = '127.0.0.1'
    finally:
        s.close()
    return IP

hostname = socket.gethostname()
ip_address = socket.gethostbyname(hostname)

print(f"Hostname: {hostname}")
print(f"IP Address: {ip_address}")

local_ip = get_local_ip()
print(local_ip)


doc1 = {  
  "DataInterval": 1,
  "Duration": 12,
  "IntervalBetweenRound": 12,
  "FeedWaterTime": [],
  "__v": 0,
  "dataInterval": 1,
  "deck": "1",
  "duration": 12,
  "feedWaterTime": [],
  "intervalBetweenRound": 12,
  "name": "devuce 1",
  "pen": "1",
  "ipaddress":local_ip
}

mycollection.update_one(
    { "ipaddress": local_ip },
    { "$set":doc1}    
)
# inserting the data in the database
cursor = mycollection.find({"ipaddress":local_ip})
for record in cursor:
    print(record)



