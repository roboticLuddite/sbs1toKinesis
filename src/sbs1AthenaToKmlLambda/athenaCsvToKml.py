from __future__ import print_function

import json
import urllib
import boto3
import time
from fastkml import kml, styles, geometry
from fastkml.geometry import Point, LineString, Polygon


s3 = boto3.client('s3')

def ConvertAthenaCsvToKml(outputBucket, outputKey, inputBucket, inputKey):


    print(outputBucket)
    print(outputKey)
    print(inputBucket)
    print(inputKey)

    # Get the object from the event and show its content type
    k = kml.KML()
    ns = '{http://www.opengis.net/kml/2.2}'

    lstyle = styles.LineStyle(color='7dff0000', width=2.0, id='redLine')
    style = styles.Style(styles = [lstyle]) 


    d = kml.Document(ns, 'docid', 'doc name', 'doc description', [style])

    k.append(d)
    shapeFolder = kml.Folder(ns, 'fid', 'f name', 'f description')
    d.append(shapeFolder)
	
    bucket = inputBucket
    key = inputKey
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        csvRows = response['Body'].read().decode('utf-8').splitlines()

        rowIterator = iter(csvRows)
	next(rowIterator)

	for row in rowIterator:
            csvColumns = row.split(',',1)
            icao24 = csvColumns[0].strip('"')
            pathString = csvColumns[1]
            pathString = pathString.strip('""[]')
            pathArray = pathString.split('"", ""')
            tupleArray = [tuple(map(float, x.split(','))) for x in pathArray]           
            tupleArray = [(pos[1], pos[0], pos[2]*.3048) for pos in tupleArray]
             
            
            if len(tupleArray) > 1:             
               placemark = kml.Placemark(ns, icao24, icao24 , 'Flight path for ' + icao24)
               
               lineGeo = LineString(coordinates=tupleArray)
               placemark.geometry = geometry.Geometry(geometry=lineGeo, altitude_mode='absolute')
	       
	       placemark.styleUrl = "#redLine"
               shapeFolder.append(placemark)


        s3.put_object(Body=k.to_string(), ContentType='application/vnd.google-earth.kml+xml', Bucket=outputBucket, Key="AUSAirTraffic-" + time.strftime('%l:%M%p %Z on %b %d, %Y') + ".kml")

        
        #print(k.to_string(prettyprint=True))

        return
    except Exception as e:
        print(e)
        raise e
