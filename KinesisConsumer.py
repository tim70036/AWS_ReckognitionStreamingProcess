###############################################################
###############################################################
##		This program pulls data from Kinesis Data Stream.    ##
##		You should modify Extract() to customize the outputs ##
##		that are extracted from data stream.                 ##
###############################################################
###############################################################

import boto3
import json
from datetime import datetime
import time
import pprint

# import var set up
from var import *
# Set your own variables in a file named var.py: 
		# AWS_REGION
		# streamName
		# collectionId
		# videoStreamArn
		# dataStreamArn
		# streamProcessorName
		# IAMRoleArn -> rekognition role for processor
		

from pinpoint import SendDrunkMessage

client = boto3.client('kinesis', region_name=AWS_REGION)

##########################################################
##########################################################
def main():

	CheckStream(streamName)

	shardIter = GetShardIter(streamName)

	ConsumeStream(shardIter)
##########################################################
##########################################################
lastTime = None
def ExtractData(response):

	global lastTime
	global prevFace
	global bigCnt
	global noDetectCnt

	# Customize processing data
	recordArr = response["Records"]

	# Empty?
	if not recordArr:
		print('\nThis is an Empty Record.\n')
		return

	# Iterate each record
	for obj in recordArr:

		# Data record time
		time = obj["ApproximateArrivalTimestamp"]
		print("\nRecord Time :" + time.strftime("%Y-%m-%d %H:%M:%S"))
		


		# Decode binary data to JSON string , and then to dict
		data = obj["Data"].decode()
		data = json.loads(data)

		# Output certrain field
		print("\nFaceSearchResponse :")
		if "FaceSearchResponse" in data:
			pprint.pprint(data["FaceSearchResponse"], indent=1)

			print('\nDetected People : ' + str(len(data["FaceSearchResponse"])) + '\n')

			FaceMovement(data["FaceSearchResponse"])

		else:
			print("\nFaceSearchResponse does not exist in this record")

		# Check every 20 sec
		if not lastTime:
			lastTime = time
		elif ((time-lastTime).total_seconds() > 15):
			print('\nReach time check interval ' + str((time-lastTime).total_seconds()) + ' sec.')
			print('Big Movement Count : ' + str(bigCnt))
			print('No Detect Count    : ' + str(noDetectCnt) + '\n')

			if bigCnt > 8 or noDetectCnt > 6:
				print('\nYOUUUUUU AREEEEE FUCKINGGGG DRUNKKKK!!!!!\n')
				print('YOUUUUUU AREEEEE FUCKINGGGG DRUNKKKK!!!!!\n')
				print('YOUUUUUU AREEEEE FUCKINGGGG DRUNKKKK!!!!!\n')
				SendDrunkMessage()
			
			bigCnt = 0
			noDetectCnt = 0
			lastTime = time
			


##########################################################
##########################################################
prevFace = []
bigCnt = 0
noDetectCnt = 0
def FaceMovement(data):

	global prevFace
	global bigCnt
	global noDetectCnt

	if data and "DetectedFace" in data[0]:
		if not prevFace:
			prevFace = data[0]["DetectedFace"]['Landmarks']
			print('\nThis is the first face.\n')
			return

		print("\nXY Movement: ")
		curFace = data[0]["DetectedFace"]['Landmarks']
		itemName = ["eyeLeft", "eyeRight", "Nose"]
		deltaX = []
		deltaY = []
		for i in range(0,len(itemName)):
			deltaX.append(curFace[i]['X'] - prevFace[i]['X'])
			deltaY.append(curFace[i]['Y'] - prevFace[i]['Y'])
			print(itemName[i] + " : (" + str(deltaX[i]) + " , " + str(deltaY[i]) + ')')

		drunkThreshold = 0.26
		if deltaX[0] > drunkThreshold or deltaX[0] < -drunkThreshold:
			bigCnt = bigCnt + 1
			print('\nDetect big movement ' + str(bigCnt))
			print('Detect big movement ' + str(bigCnt))
			print('Detect big movement ' + str(bigCnt)+ '\n')

		prevFace = curFace		

	else:
		noDetectCnt = noDetectCnt + 1
		print('\nNo detectedFace in this record. ' + str(noDetectCnt) + 'times' + '\n')

##########################################################
##########################################################
def ConsumeStream(shardIter):
	print('=====================================')
	print('Start getting record...')

	# Infinite Loop
	while True:

		response = client.get_records(ShardIterator=shardIter, Limit=1)
		ExtractData(response)
		print('=====================================')
		time.sleep(1)

		# If there is new data 
		while 'NextShardIterator' in response:
			response = client.get_records(ShardIterator=response['NextShardIterator'], Limit=1)
			ExtractData(response)
			print('=====================================')
			# wait for 3 seconds
			time.sleep(1)
##########################################################
##########################################################
def CheckStream(streamName):
	print('Listing all stream : ')
	response = client.list_streams()
	isCreated = False
	for stream in response['StreamNames']:
		print(stream)
		if streamName == stream :
			isCreated = True
	print('=====================================')

	# Create stream if not exist
	if not isCreated:
		print('Creating ' + streamName + ' ...')
		client.create_stream(StreamName = streamName, ShardCount=2)
		while True:
			response = client.describe_stream()

			if response['StreamDescription']['StreamStatus'] == 'ACTIVE' :
				break
			print(streamName + ' is not active yet...')
			time.sleep(3)
	else :
		print('Exist ' + streamName + ', no need for creating.')
	print('=====================================')
##########################################################
##########################################################
def GetShardIter(streamName):
	print('Getting ShardIter')
	# Get stream shard ID
	response = client.describe_stream(StreamName=streamName)
	shardID = response['StreamDescription']['Shards'][0]['ShardId']
	print('Receive shard id : ' + shardID)


	shardIter = client.get_shard_iterator(
	    StreamName=streamName,
	    ShardId=shardID,
	    ShardIteratorType='LATEST',
	)
	shardIter = shardIter['ShardIterator']

	print('Receive shard iterator.')
	return shardIter
##########################################################
##########################################################
if __name__ == '__main__':
	main()

