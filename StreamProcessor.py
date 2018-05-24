###############################################################
###############################################################
##		This program set up a Rekognition Stream Processor.  ##
##		This Processor pulls streaming data from Kinesis     ##
##		Video Stream. After streaming data is processed,     ##
##		the result is sent to Kinesis Data Stream.           ##
###############################################################
###############################################################

import boto3
import time
import datetime
import sys

# Import var set up
from var import *
# Set your own variables in a file named var.py: 
		# AWS_REGION
		# streamName
		# collectionId
		# videoStreamArn
		# dataStreamArn
		# streamProcessorName
		# IAMRoleArn -> rekognition role for processor

client = boto3.client('rekognition', region_name=AWS_REGION)

##########################################################
##########################################################
def main():

	# Create collection if not exist
	CheckCollection()

	# Create processor if not exist
	CheckProcessor()

	# Start processor if not running
	StartProcessor()
##########################################################
##########################################################
def CheckCollection():

	print('Checking collections.')
	response = client.list_collections()

	# Create empty collection if not exist
	if collectionId not in response['CollectionIds']:
		collection = client.create_collection(CollectionId=collectionId)
		print(collectionId + ' is created.')
	else:
		print(collectionId + ' exists.')
##########################################################
##########################################################
def CheckProcessor():

	print('\nListing all processor : ')
	response = client.list_stream_processors()
	isCreated = False
	for processor in response['StreamProcessors']:
		print(processor['Name'] +'->' + processor['Status'])
		if streamProcessorName == processor['Name']:
			isCreated = True

	print('')
	if not isCreated:
		# Create stream processor
		print('Creating streamProcessor...')
		streamProcessorArn = client.create_stream_processor(
		    Input={
		        'KinesisVideoStream': {
		            'Arn': videoStreamArn
		        }
		    },
		    Output={
		        'KinesisDataStream': {
		            'Arn': dataStreamArn
		        }
		    },
		    Name=streamProcessorName,
		    Settings={
		        'FaceSearch': {
		            'CollectionId': collectionId,
		            'FaceMatchThreshold': 90
		        }
		    },
		    RoleArn=IAMRoleArn
		)
		print('Created streamProcessorArn : ' + streamProcessorArn['StreamProcessorArn'])
	else:
		print('Exist ' + streamProcessorName + ', no need for creating.')
##########################################################
##########################################################
def StopProcessor():

	response = client.describe_stream_processor(Name=streamProcessorName)
	if response['Status'] != "Stopped":
		print('Current state of processor->' + response['Status'])
		response = client.stop_stream_processor(Name=streamProcessorName)
		print('Stopping ' + streamProcessorName + '...')
	else:
		print(streamProcessorName + 'is already stopped.')

	# Monitor until it is stopped 
	response = client.describe_stream_processor(Name=streamProcessorName)
	while response['Status'] != "STOPPED":
		print('Current state of processor->' + response['Status'])
		response = client.describe_stream_processor(Name=streamProcessorName)
		time.sleep(5)
##########################################################
##########################################################
def StartProcessor():

	# Start stream processor
	response = client.describe_stream_processor(Name=streamProcessorName)
	if response['Status'] != "RUNNING":
		print('Current state of processor->' + response['Status'])
		response = client.start_stream_processor(Name=streamProcessorName)
		print('Starting ' + streamProcessorName + '...')
	else:
		print(streamProcessorName + 'is already running.')

	# Infinite loop with keyboard interrupt detection
	try:
		while True:
			response = client.describe_stream_processor(Name=streamProcessorName)
			print('		==================================')
			print('		LastUpdateTime : ' + response['LastUpdateTimestamp'].strftime("%Y-%m-%d %H:%M:%S"))
			print('		Status : ' + response['Status'])
			print('		Input  : ' + response['Input']['KinesisVideoStream']['Arn'])
			print('		Output : ' + response['Output']['KinesisDataStream']['Arn'])
			print('		Press ctrl+c to leave')
			time.sleep(3)
	except KeyboardInterrupt:
		print('\nKeyboardInterrupt detected!')
		StopProcessor()
		sys.exit()
##########################################################
##########################################################
if __name__ == '__main__':
	main()