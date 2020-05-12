import time
from datetime import datetime

from Neo import Neo

from google.cloud import bigquery 
from google.oauth2 import service_account

#Construct a BigQuery client object.'
key_path= "/root/Desktop/Projects/BigQuery/smartpi-925a18414597.json"
credentials1 = service_account.Credentials.from_service_account_file(
    key_path,
    scopes = ["https://www.googleapis.com/auth/cloud-platform"],    
)
client = bigquery.Client(
    credentials = credentials1,
    project = credentials1.project_id
)


RaspberryPi = Neo('pi')

while True:
  try:
    #get data
    humidity = RaspberryPi.getHumidity()
    temperatureH = RaspberryPi.getTemperatureH()
    temperature = RaspberryPi.getTemperature()

    Now = datetime.now()
    date_time = Now.strftime("%Y-%m-%d %H:%M:%S")
    
    #Inject data into BigQuery. numeor 2
    query1 = (
        'INSERT `smartpi-273316.PiSensors.Sensors` (Date, Temperature, Comment,Humidity) '
        'VALUES  (DATETIME "' + str(date_time) + '",'
        + str(temperature) + ','
        +'"Start.py | tempH: '+ str(temperatureH)+'| temp: '+ str(temperatureH)+'",'
        +str(humidity)+')')


    query_job1 = client.query(query1) #Make an API request
    print ("Query sent :  " + str(query1))    
    
    time.sleep(1) #sleep one second
  except KeyboardInterrupt:
    print('OK')
    
    