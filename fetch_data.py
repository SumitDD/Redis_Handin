import json
from flask import Flask, render_template, jsonify
import time
import redis
import datetime
import requests
redis_client = redis.Redis(host='localhost', port=6379, db=0, password="")
app = Flask(__name__)
redis_client.expire('api_data', 3600)

#Use Redis to enhance secondary respond times from these end points.
@app.route('/', methods=['GET'])
def fetch_from_api():
#TO SHOW THE TIME TO GET THE DATA FROM REDIS CACHE 
    if redis_client.get("api_data"):
        print("CACHED IN REDIS")
        start = datetime.datetime.now()
        data = redis_client.get("api_data")
        data_json = json.loads(data)
        response = data_json['data'][2]
        end = datetime.datetime.now()
        total_time = end-start
        execution_time = total_time.total_seconds() * 1000
        return jsonify(f"THE TIME TO EXECUTE: {execution_time} ms to get the following data {response}")
   

#TO SHOW THE TIME TO GET THE DATA FROM THE API 
    else:
        print("GETTING THE DATA FROM THE URL")
        start = datetime.datetime.now()
        data = requests.get(f'https://datausa.io/api/data?University=142832&measures=Total%20Noninstructional%20Employees&drilldowns=IPEDS%20Occupation&parents=true/')
        data_json = data.json()
        response = data_json['data'][2]
        end = datetime.datetime.now()
        total_time = end-start
        execution_time = total_time.total_seconds() * 1000
        redis_client.set("api_data", json.dumps(data_json))
        return jsonify(f"THE TIME TO EXECUTE: {execution_time} ms to get the following data {response}")

#Make use Redis to drill down in to the data and find some interesting point that can be fetched and displayed without the need to re-query the API all the time.
@app.route('/fetchsingle')
def find_value_by_id():
    start = datetime.datetime.now()
    response = redis_client.get("api_data")
    response = json.loads(response)
    uni_id = response['data'][2]['IPEDS Occupation']
    end = datetime.datetime.now()
    total_time = end-start
    execution_time = total_time.total_seconds() * 1000
    return jsonify('IPEDS Occupation found: {} and it took {} ms'.format(uni_id, execution_time))

#Divide the data in to logical divisions using either sets, lists,hashed or one of the other types native to Redis.
@app.route('/dividedata')
def divide_data():
    start = datetime.datetime.now()
    data = requests.get(f'https://datausa.io/api/data?University=142832&measures=Total%20Noninstructional%20Employees&drilldowns=IPEDS%20Occupation&parents=true/')
    data_json = data.json()
    for number, api_object in enumerate(data_json['data']):
        new_object = {x.replace(' ', ''): v for x, v in api_object.items()}
        for key, value in new_object.items():
            redis_client.hset(number, key, value)

        #Specify a retention time for all values (Time to live)
        redis_client.expire(number, 3600)
    end = datetime.datetime.now()
    total_time = end-start
    execution_time = total_time.total_seconds() * 1000
    return jsonify('The time it took to divide all the objects from the data list into hash sets: {} ms'.format(execution_time))

if __name__ == "__main__":
    app.run(debug=True, port=5000)