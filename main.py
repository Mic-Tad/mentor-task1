import pymongo
import requests
import redis
import json

mongourl = "mongodb://mongo:9090"
redis_client=redis.Redis()
client = pymongo.MongoClient("localhost:9090")
db = client.weather_forecast14
coll = db.Kyiv
KYIV_LAT = 50.27
KYIV_LON = 30.3125


def get_forecast(lat=KYIV_LAT, lon=KYIV_LON):
    """Gets the forecast from open-weather13.p.rapidapi.com"""
    url = f"https://open-weather13.p.rapidapi.com/city/fivedaysforcast/{lat}/{lon}"
    headers = {
        "X-RapidAPI-Key": "b7114a70b3msh36cae50bddaa283p1dfaf5jsn68549db2f8c2",
        "X-RapidAPI-Host": "open-weather13.p.rapidapi.com"
    }
    response = requests.request("GET", url, headers=headers)
    return response.json()


lat = KYIV_LAT
lon = KYIV_LON

def insert_new_data():
    redis_client.flushall()
    def store_json(j_file):
        """Writes the json into db, so we do not need to send request more than 1 time"""
        redis_client.set('weather',json.dumps(j_file),500)

        

    js = get_forecast(lat=lat, lon=lon)
    store_json(js)
    print('There is new data')
    return True

def check_for_new_data():
<<<<<<< HEAD
    data1=redis_client.get('weather')
    flag=data1 is not None
    if flag:
        data1=json.loads(redis_client.get('weather'))
    if not (flag and lat == data1['city']['coord']['lat'] and lon == data1['city']['coord']['lon']):
        return insert_new_data()
    return False
=======
    if not (lat == coll1.find_one({})['city']['coord']['lat'] and lon == coll1.find_one({})['city']['coord']['lon']):
        coll1.delete_many({})

        def store_json(j_file):
            """Writes the json into db, so we do not need to send request more than 1 time"""
            coll1.insert_one(j_file)

        js = get_forecast(lat=lat, lon=lon)
        store_json(js)
        print('There is new data')

>>>>>>> 2aec5c6daae93723f2a063839a80598b7ec21f83


def scheme():
    """Gets the important data from all_data"""
<<<<<<< HEAD
    flag = check_for_new_data()
    all_data = json.loads(redis_client.get('weather'))
=======
    check_for_new_data()
    all_data = coll1.find({})
>>>>>>> 2aec5c6daae93723f2a063839a80598b7ec21f83

    coll.delete_many({})
    
    zero_kelvin = -273
 
    for j in all_data['list']:
        data = {
            'date_time': j['dt_txt'],
            'temp': int(j['main']['temp'] + zero_kelvin),
            'humidity': j['main']['humidity'],
            'weather': j['weather'][0]['main'],
            'wind_speed': int(j['wind']['speed'])

        }
        coll.insert_one(data)
    coll.create_index('date_time')
    redis_client.close()


scheme()

print('done')
