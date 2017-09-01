'''
Created on May 23, 2017

@author: paulu
'''

import time 
import lambda_function



if __name__ == '__main__':
    
	testEvent = {
	  "Records": [
		{
		  "eventVersion": "2.0",
		  "eventTime": "1970-01-01T00:00:00.000Z",
		  "requestParameters": {
			"sourceIPAddress": "127.0.0.1"
		  },
		  "s3": {
			"configurationId": "testConfigRule",
			"object": {
			  "eTag": "0123456789abcdef0123456789abcdef",
			  "sequencer": "0A1B2C3D4E5F678901",
			  "key": "sbs1ToIntermediateKml/2017/08/09/83802028-d6e5-4c14-8673-159e583c1950.csv",
			  "size": 1024
			},
			"bucket": {
			  "arn": "arn:aws:s3:::aws-athena-query-results-786156393318-us-east-1",
			  "name": "aws-athena-query-results-786156393318-us-east-1",
			  "ownerIdentity": {
				"principalId": "EXAMPLE"
			  }
			},
			"s3SchemaVersion": "1.0"
		  },
		  "responseElements": {
			"x-amz-id-2": "EXAMPLE123/5678abcdefghijklambdaisawesome/mnopqrstuvwxyzABCDEFGH",
			"x-amz-request-id": "EXAMPLE123456789"
		  },
		  "awsRegion": "us-east-1",
		  "eventName": "ObjectCreated:Put",
		  "userIdentity": {
			"principalId": "EXAMPLE"
		  },
		  "eventSource": "aws:s3"
		}
	  ]
	}
		
	lambda_function.lambda_handler(testEvent, {})
	pass