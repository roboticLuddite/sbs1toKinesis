'''
Created on May 23, 2017

@author: paulu
'''

import time 
import collections
import json
import sbs1
from boto import kinesis
import boto.kinesis.exceptions
import socket


class sbs1ToKinesisStreamer (object):
    
    def startStreaming(self):
        
        self.kinesisConn = kinesis.connect_to_region(self.connectionDetails['kinesis']['region'])
        
        while True:                
        
            self.clientsocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.clientsocket.connect((self.connectionDetails['sbs1']['host'], self.connectionDetails['sbs1']['port']))
            
            for rawSBS1String in self.linesplit():                                
                processedSBS1 = sbs1.SBS1Message(rawSBS1String)                                                
                putResponse = self.kinesisConn.put_record(self.connectionDetails['kinesis']['streamID'],  
                                                          json.dumps(processedSBS1.__dict__),
                                                           self.connectionDetails['kinesis']['partionKey'])           
                
                                                                           
            print('restarting buffering')
            
                
    def linesplit(self):
        
        try:
            buffer = self.clientsocket.recv(4096)            
        except ValueError:            
            return          
        buffering = True
        while buffering:
            if "\n" in buffer:
                (line, buffer) = buffer.split("\n", 1)
                yield line + "\n"
            else:
                more = self.clientsocket.recv(4096)
                if not more:
                    buffering = False
                else:
                    buffer += more
        if buffer:
            yield buffer
    
    def __init__(self, connectionDetails):
        self.connectionDetails = connectionDetails




if __name__ == '__main__':
    
    config =  collections.defaultdict(dict)
    
    config['sbs1']['host'] = '192.168.2.104'
    config['sbs1']['port'] = 30003
    config['kinesis']['region'] = 'us-east-1'
    config['kinesis']['streamID'] = 'sbs1Stream'
    config['kinesis']['partionKey'] = 'underwoodpaul'
    
    
    streamer = sbs1ToKinesisStreamer(config)
    streamer.startStreaming()    
    
    pass