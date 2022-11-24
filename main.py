import pymongo
import requests

client = pymongo.MongoClient("localhost:27017")
db = client.weather_forecast
coll = db.Kyiv
coll1 = db.Kyiv_all_in_one
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


def check_for_new_data():
    if not (lat == coll1.find_one({})['city']['coord']['lat'] and lon == coll1.find_one({})['city']['coord']['lon']):
        coll1.delete_many({})
        def store_json(j_file):
            """Writes the json into db, so we do not need to send request more than 1 time"""


            coll1.insert_one(j_file)

        js = get_forecast(lat=lat, lon=lon)
        store_json(js)
        print('There is new data')
        return True
    return False


def scheme():
    """Gets the important data from all_data"""
    flag = check_for_new_data()
    all_data = coll1.find({})


    coll.delete_many({})

    zero_kelvin = -273
    for i in all_data:
        for j in i['list']:
            data = {
                'date_time': j['dt_txt'],
                'temp': int(j['main']['temp'] + zero_kelvin),
                'humidity': j['main']['humidity'],
                'weather': j['weather'][0]['main'],
                'wind_speed': int(j['wind']['speed'])

            }
            coll.insert_one(data)
    coll.create_index('date_time')


scheme()

print('done')
