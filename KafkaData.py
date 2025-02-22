import json
from datetime import datetime
from time import sleep
from kafka import KafkaProducer
import logging
from alpha_vantage.timeseries import TimeSeries

ALPHA_VANTAGE_SYMBOL = 'AAPL'
ALPHA_VANTAGE_KEY ='OB8J15G4BXTF4X20'
KAFKA_BROKER = 'localhost:9092'
KAFKA_TOPIC_NAME = 'stock'    
INDEX = 0

# Get data from alpha vantage api
def get_data():
    time = TimeSeries(key=ALPHA_VANTAGE_KEY, output_format='json')
    jsondata, _ = time.get_intraday(symbol=ALPHA_VANTAGE_SYMBOL, interval='1min', outputsize='full')
    # with open("DataStockApple.json", 'r') as jsonfile:
    #     jsondata = json.load(jsonfile)
    return jsondata

# json data format to list
def format_data(jsondata):
    data = [{   "time"  :key,
                "open"  :float(value["1. open"]),
                "high"  :float(value["2. high"]),
                "low"   :float(value["3. low"]),
                "close" :float(value["4. close"])} for key, value in jsondata.items()]
        
    data =  sorted(data, key=lambda x: x["time"])
    return data


# send data to kafka
def stream_data(producer, data):
    try:
        producer.send(topic=KAFKA_TOPIC_NAME, value=json.dumps(data).encode('utf-8'), key=b'stock')
        print(f"Sent Message! {INDEX + 1, data}")
    except Exception as e:
        logging.error(f'An error occured: {e}')




if __name__ == "__main__":
    datalist = get_data()
    datalist = format_data(datalist)
    # print(datalist[:5])

    producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER)

    # send data to spark process by kafka
    for data in datalist:
        data["time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        stream_data(producer, data)
        INDEX = INDEX + 1
        sleep(1)

