# -*- coding: utf-8 -*-
import boto3
import pandas as pd
import os
import os.path
import time
import json
from random import *
from elasticsearch import Elasticsearch, RequestsHttpConnection


"""
#JSON文件格式
{
    thingname:"thingnamexx",
    pubtime:"%Y-%m-%d %I:%M %p", // used for sql select
    pubtimes3:"2007-04-05T14:30Z", //used for s3 select 
    mois:int, // weather.csv - Humidity
    temp:int, // weather.csv - Temp. 0~50
    
    location: "y,x" //from x.json
    
}
"""
walkDir = '/weather-data-dir/'
fieldList = ['Time (CST)','Temp.', 'Windchill', 'Dew Point', 'Humidity', 'Pressure','Visibility',
'Wind Dir', 'Wind Speed', 'Gust Speed', 'Precip', 'Events',  'Conditions' ]

#S3 JSON Single file Dictation
s3json = {}

#Thing Number
thingNumber = 16

#Thing Name prefix
thingNamePrefix = 'Thingname'

#load location to memory
jsonFile = open("coordinates.json","r")
jsonDict = json.load(jsonFile)

#random X generate as below
#thingX = jsonDict['location'][round(166*random())]

#init elasticsearch
#host = 'search-iotes-davdt2utcf4v6lbdbq4zdho5ui.cn-northwest-1.es.amazonaws.com.cn'
host = 'elasticsearch-host-domain-name'
#awsauth = AWS4Auth(YOUR_ACCESS_KEY, YOUR_SECRET_KEY, REGION, 'es')

es = Elasticsearch(
    hosts=[{'host': host, 'port': 443}],
    use_ssl=True,
    verify_certs=True,
    connection_class=RequestsHttpConnection
)
es_id = 1
#open weather file
for parent, dirnames,filenames in os.walk(walkDir):
    #read csv file
    for filename in filenames:
        print "processing file name: %s \n"% filename
        dfWeather= pd.read_csv(walkDir+filename)
        
        for i in dfWeather.index:
            weatherItem = {}
            #Check if Centi Degree or F Degree
            temper = 0
            #Show orginal content
            #print dfWeather.loc[i]
            if dfWeather.loc[i]['Temp.'][-1] == "C":
                temper = float(dfWeather.loc[i]['Temp.'][0:-3])
            
            elif dfWeather.loc[i]['Temp.'][-1] == "F":
                temper = round((float(dfWeather.loc[i]['Temp.'][0:-3]) - 32) / 1.8,2)
                
            try:    
                for field in fieldList:
                    weatherItem[field] = str(dfWeather.loc[i][field])
            except:
                pass
            
            ttime = time.strptime(str(dfWeather.loc[i]['Time (CST)']),"%Y-%m-%d %I:%M %p")
            weatherItem['pubtime'] = time.strftime("%Y-%m-%d %H:%M:%S",ttime)
            weatherItem['pubtimeS3'] = time.strftime('%Y-%m-%dT%H:%M:%SZ', ttime)
            weatherItem['temp'] = temper
            try:
		weatherItem['mois'] = int(dfWeather.loc[i]['Humidity'][:-1])
            except ValueError:
                weatherItem['mois'] = 0
            for j in range(1,thingNumber+1):
                jsonThing = {}
                jsonThing['thingname'] = thingNamePrefix + str(j)
                jsonThing['pubtime'] = weatherItem['pubtime'] 
                jsonThing['pubtimeS3'] = weatherItem['pubtimeS3']
                jsonThing['temp'] = weatherItem['temp'] + float('%0.1f' % random())
                jsonThing['mois'] = weatherItem['mois'] +  randint(1,10)
                jsonThing['location'] = jsonDict['location'][randint(1,166)]
                res = es.index(index="pubiot", doc_type='iot', id=es_id, body=jsonThing)
                es_id = es_id + 1
                # with open(jsonFileName, 'w') as f:
                #     json.dump(jsonThing, f)
                #s3.upload_file('data.json', 'zy-pubiot-1998', jsonFileName)

