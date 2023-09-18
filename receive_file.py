import os
import sys
import time
import glob
import _thread
import json
import binascii
import base64
import hashlib
import paho.mqtt.client as mqtt


HOST = "localhost"
PORT = 1883
SUBTOPIC = "/file/+"
TEMPDIR = "temp"

client = mqtt.Client()  # mqtt client


def my_json(msg):
    print(msg)
    print('this is my json')
    a= json.dumps(msg)
    print(a)
    return json.dumps(msg)  # object2string


def my_exit(err):   # exit programm
    os._exit(err)
    os.kill(os.getpid)


def my_md5(fname):  # calculate md5sum
    hash_md5 = hashlib.md5()
    with open(fname, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


def my_temp_file(deviceId,mydata, myhash, mynumber, timeid, filename):
    """ save data to temp file
        and send recieved chunknumber
    """
    filename = os.path.basename(filename)
    if hashlib.md5(mydata.encode()).hexdigest() == myhash:
        fname = TEMPDIR+"/"+str(timeid)+"_"+filename+"_.temp"
        f = open(fname, "ab")
        if mynumber == 0:
            f = open(fname, "wb")
        try:
            f.write(base64.b64decode(mydata))
        except Exception as e:
            print("ERR: write file", fname, e)
            return 1
        finally:
            f.close()
        print("saved chunk", mynumber, "to", fname)
        client.publish("/file/"+deviceId+"/status", my_json({"chunknumber": mynumber}))
        print('publish the pubtopic')

def my_check_temp_files(filename, timeid, filehash):
    """ check temp file and rename to original
    """
    os.sync()
    print('my check temp files')
    filename = os.path.basename(filename)
    for l in os.listdir(TEMPDIR):
        nameid = l.split("_")[0]
        if nameid == timeid:
            if my_md5(TEMPDIR+"/"+l) == filehash:
                os.rename(TEMPDIR+"/"+l, TEMPDIR+"/"+str(timeid)+"_"+filename)
    for f in glob.glob(TEMPDIR+"/*.temp"):
        os.remove(f)
    print("OK: saved file", str(timeid)+"_"+filename)


def my_event(top, msg, qos, retain):
    """ convert msg to json,
        send data to file
    """
    print(msg)
    print(top)        
    if "sensor/" in top:
        print(msg)
        #add_data_to_influx(msg)
    else:
      try:
        if type(msg) is bytes:
            msg = msg.decode()
        j = json.loads(msg)
      except Exception as e:
        print("ERR: msg2json", e)
        #my_exit(2)
      try:
        if j["end"] is False:
            my_temp_file(
                j["deviceId"],
                j["chunkdata"],
                j["chunkhash"],
                j["chunknumber"],
                j["timeid"],
                j["filename"])
        if j["end"] is True:
            my_check_temp_files(j["filename"], j["timeid"], j["filehash"])
            #my_exit(0)
      except Exception as e:
        print("ERR: parse json", e)
        #my_exit(3)


def on_connect(client, userdata, flags, rc):
    print("OK Connected with result code "+str(rc))
    client.subscribe(SUBTOPIC, qos=0)
    print("Subscribe: " + SUBTOPIC)
    client.subscribe("sensor/+/data", qos=0)


def on_message(client, userdata, msg):
    _thread.start_new_thread(my_event, (
        msg.topic,
        msg.payload,
        msg.qos,
        msg.retain))


def main():
    if not os.path.exists(TEMPDIR):
        try:
            os.makedirs(TEMPDIR)
        except:
            print("ERR create dir "+TEMPDIR)
            return 1
    client.connect(HOST, PORT, 60)
    client.on_connect = on_connect
    client.on_message = on_message
    client.loop_forever()


if __name__ == "__main__":
    main()

