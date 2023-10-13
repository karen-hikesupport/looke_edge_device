
import cv2
import threading
import numpy as np
import paho.mqtt.client as mqtt
import socketserver
from threading import Condition
from http import server

PAGE="""\
<html>
<head>
<title>Raspberry Pi - Surveillance Camera</title>
</head>
<body>
<center><h1>Raspberry Pi - Surveillance Camera</h1></center>
<center><img src="stream.mjpeg" width="640" height="480"></center>
</body>
</html>
"""


class Stream_receiver:

    def __init__(self, topic='',host="localhost",port=1883):        
        self.topic=topic
        self.frame=None  # empty variable to store latest message received
        
        self.client = mqtt.Client()  # Create instance of client 

        self.client.on_connect = self.on_connect  # Define callback function for successful connection
        
        self.client.message_callback_add(self.topic,self.on_message)
        
        self.client.connect(host,port)  # connecting to the broking server
        
        t=threading.Thread(target=self.subscribe)       # make a thread to loop for subscribing
        t.start() # run this thread
        print("start")
    def subscribe(self):
        self.client.loop_forever() # Start networking daemon
        
    def on_connect(self,client, userdata, flags, rc):  # The callback for when the client connects to the broker
        client.subscribe(self.topic)  # Subscribe to the topic, receive any messages published on it
        print("Subscring to topic :",self.topic)


    def on_message(self,client, userdata, msg):  # The callback for when a PUBLISH message is received from the server.
        print(msg.payload)
        nparr = np.frombuffer(msg.payload, np.uint8)
        print(nparr)
        self.frame = cv2.imdecode(nparr,  cv2.IMREAD_COLOR)        
        print(self.frame)
        cv2.imwrite("stream.jpeg", self.frame)
        #camera.start_recording(nparr, format='mjpeg')

        try:
            address = ('localhost', 8000)
            server = StreamingServer(address, StreamingHandler)
            server.serve_forever()
        except Exception as error:
            print("An exception occurred:", error)

       

        #frame= cv2.resize(frame, (640,480))   # just in case you want to resize the viewing area
        #cv2.imshow('recv', self.frame)
        #if cv2.waitKey(1) & 0xFF == ord('q'):
        return

class StreamingHandler(server.BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == '/':
            self.send_response(301)
            self.send_header('Location', '/index.html')
            self.end_headers()
        elif self.path == '/index.html':
            content = PAGE.encode('utf-8')
            self.send_response(200)
            self.send_header('Content-Type', 'text/html')
            self.send_header('Content-Length', len(content))
            self.end_headers()
            self.wfile.write(content)
        elif self.path == 'stream.jpeg':
            self.send_response(200)
            self.send_header('Age', 0)
            self.send_header('Cache-Control', 'no-cache, private')
            self.send_header('Pragma', 'no-cache')
            self.send_header('Content-Type', 'multipart/x-mixed-replace; boundary=FRAME')
            self.end_headers()
            try:                                                          
                self.wfile.write(b'--FRAME\r\n')
                self.send_header('Content-Type', 'image/jpeg')
                self.send_header('Content-Length', len(self.frame))
                self.end_headers()
                self.wfile.write(self.frame)
                self.wfile.write(b'\r\n')
            except Exception as e:
                logging.warning(
                    'Removed streaming client %s: %s',
                    self.client_address, str(e))
        else:
            self.send_error(404)
            self.end_headers()

class StreamingServer(socketserver.ThreadingMixIn, server.HTTPServer):
    allow_reuse_address = True
    daemon_threads = True
     

if __name__ == "__main__":
    # creatign 4 instances of the MQ_subs class
    j=Stream_receiver(topic="test1")
    
    