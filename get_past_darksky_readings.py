import datetime, time
import requests
import json

# Input: Takes a datetime.datetime object
def get_past_darksky_readings(d):

    # Convert to ISO format
    d_iso = int(time.mktime(d.timetuple()))
    api_query = "https://api.darksky.net/forecast/7358efac8f40bf49ceaa4a8c5278bd31/34.4099508,-119.8661304," + str(d_iso)
    r = requests.get(api_query)
    r = json.loads(r.content)
    
    parameters = ['time', 'ozone', 'windGust', 'temperature', 'dewPoint', 'humidity', 'apparentTemperature', 'pressure', 'windSpeed', 'precipProbability', 'visibility', 'cloudCover', 'precipIntensity']
    
    past_data = []
    
    for hour in r["hourly"]["data"]:
        hourly_data = {}
        for item in hour:
            for p in parameters:
                if item == p:
                    hourly_data[item] = hour[item]
                    if p == "time":
                        hourly_data["time"] = datetime.datetime.fromtimestamp(int(hourly_data["time"])).strftime('%Y-%m-%d %H:%M:%S')
                        hourly_data["time"] = datetime.datetime.strptime(hourly_data["time"], '%Y-%m-%d %H:%M:%S')
                        hourly_data["time"] = hourly_data["time"] - datetime.timedelta(hours=8, minutes=0)
        past_data.append(hourly_data)

    print past_data
    
# Example: get_past_darksky_readings(datetime.datetime(2019, 2, 20))

get_past_darksky_readings(datetime.datetime(2019, 2, 20))