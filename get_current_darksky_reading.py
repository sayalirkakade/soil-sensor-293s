import forecastiopy
# from darksky import forecast
import datetime
import inspect

def get_current_darksky_readings():

    apikey = '7358efac8f40bf49ceaa4a8c5278bd31'

    Goleta = [34.4099508, -119.8661304]

    fio = ForecastIO.ForecastIO(apikey,
                                units = ForecastIO.ForecastIO.UNITS_SI,
                                lang = ForecastIO.ForecastIO.LANG_ENGLISH,
                                latitude = Goleta[0], longitude = Goleta[1])

    parameters = ['time', 'ozone', 'windGust', 'temperature', 'dewPoint', 'humidity', 'apparentTemperature', 'pressure', 'windSpeed', 'precipProbability', 'visibility', 'cloudCover', 'precipIntensity']

    if fio.has_currently() is True:
        currently = FIOCurrently.FIOCurrently(fio)
        
        store_values = {}
        
        for item in parameters:
            if item == 'time':
                currently.time = datetime.datetime.fromtimestamp(int(currently.time)).strftime('%Y-%m-%d %H:%M:%S')
                currently.time = datetime.datetime.strptime(currently.time, '%Y-%m-%d %H:%M:%S')
                currently.time = currently.time - datetime.timedelta(hours=8, minutes=0)
                store_values[item] = currently.time
            else:
                store_values[item] = currently.get()[item]
        
        return store_values
                
    else:
        return -1
