import time 
import collections
import json
import sbs1
import sys
import thread
from boto import kinesis
import boto.kinesis.exceptions
from datetime import datetime
import socket


class sbs1ToKinesisStreamer (object):
	
	def streamSbs1StreamAsync(self, port):
		
		while True: 
			try: 
				self.clientsocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
				self.clientsocket.connect((self.connectionDetails['sbs1']['host'], port))
			except:
				print("Unable to find SBS1 stream on port:"+ port +". Waiting 5 seconds...")
				time.sleep(5)
				continue
			
			kinsisPutRecordsBufferFlushIndex = datetime.now().second
			kinesisPutRecordBuffer = []						
			try: 
				for rawSBS1String in self.linesplit():
					#sys.stdout.write(rawSBS1String)
					#sys.stdout.flush()
					
					processedSBS1 = sbs1.SBS1Message(rawSBS1String)
					kinesisPutRecordBuffer.append({
						"Data": json.dumps(processedSBS1.__dict__),
						"PartitionKey": self.connectionDetails['kinesis']['partionKey']
					})
							
					currentSecond = datetime.now().second						
					if(currentSecond != kinsisPutRecordsBufferFlushIndex):
						kinsisPutRecordsBufferFlushIndex = currentSecond
						#print("purging buffer")
						self.kinesisConn.put_records(kinesisPutRecordBuffer, self.connectionDetails['kinesis']['streamID'])
						kinesisPutRecordBuffer = []
			except:
				print("Issue connecting to SBS1 socket. Waiting 5 seconds.")
				time.sleep(5)		
			print('restarting buffering')
			time.sleep(5)

	def startStreaming(self):
		print("Starting stream.")
		self.kinesisConn = kinesis.connect_to_region(self.connectionDetails['kinesis']['region'])
		
		thread.start_new_thread(self.streamSbs1StreamAsync, (self, config['sbs1']['portADSB'], ) )
		thread.start_new_thread(self.streamSbs1StreamAsync, (self, config['sbs1']['portMLAT'], ) )
		while 1:
			pass

	def linesplit(self):
		try:
			buffer = self.clientsocket.recv(100)
		except ValueError:
			return
		buffering = True
		while buffering:
			if "\n" in buffer:
				(line, buffer) = buffer.split("\n", 1)
				yield line + "\n"
			else:
				more = self.clientsocket.recv(100)
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
	config['sbs1']['portADSB'] = 30003
	config['sbs1']['portMLAT'] = 30106
	config['kinesis']['region'] = 'us-east-1'
	config['kinesis']['streamID'] = 'sbs1Stream'
	config['kinesis']['partionKey'] = 'underwoodpaul'
	streamer = sbs1ToKinesisStreamer(config)
	streamer.startStreaming()
