"""the gnrl_database_interaction module is part of the dbutility package


Naming Conventions:

Package:            thispackage (short name)
Module:             this_module (short name)
Class:              ThisIsAClass
Function:           this_is_a_function
Public Method:      this_is_a_public_method
Non-Public Method:  _this_is_a_non_public_method
Variables:          thisIsAVariable
Constant:           THIS_IS_A_CONSTANT

"""


__status__  = "development"
__version__ = 'mdl 1.0.1'
__date__    = "26 June 2018"
__author__  = 'Chris Pickford <drchrispickford@gmail.com>'


import pandas as pd
import time as tm
import datetime
import requests




class OpenWeatherMap(object):

    '''
    __status__  = "development"
    __version__ = 'mdl 1.0.1'
    __date__    = "21 June 2018"
    __author__  = 'Chris Pickford <drchrispickford@gmail.com> '

    '''

    def __init__(self):

        import requests
        from pandas.io.json import json_normalize

        from WeatherData.src import config

        self.api_url = config.owm_api_url
        self.appid = config.owm_appid
        self.cityList = config.owmCityList
        self.safetyTimer = 100000
        self.weatherReading = None
        self.weatherDF = pd.DataFrame


        print('Open Weather Map API object created')



    def weather_API_request(self,city):
        '''
            Accepts the path to a script file, then checks / produces a viable SQL script
            TO DO:  write the security check function
            '''

        message_ = {
            'error': None,
            'method': 'apiutility.openweathermap_API_interaction.weather_API_request',
            'description': "cconnects to openweathermap and collects weather data",
            'misc': None
        }

        try:
            if (tm.time() - self.safetyTimer) / 60 < 10:
                print('Cannot request data yet')
                return None
            else:
                self.weatherReading = requests.get(url=self.api_url, params=dict(q=city, APPID=self.appid))

            #self.safetyTimer = tm.time()

        except Exception as e:
            message_['error'] = "Something went wrong collecting data from the API"
            message_['traceback'] = self.config.get_traceback(e)
            # print(message_['error'])
            pass
        return message_

    def process_weather_reading(self, weatherData):
        '''
            Pushes data to the SQL database
            TO DO:  write main code
            '''

        message_ = {
            'error': None,
            'method': 'apiutility.openweathermap_API_interaction.process_weather_reading',
            'description': "What does it do",
            'misc': None
        }

        try:
            columnNames = ['date', 'city', 'description', 'temp', 'max_temp', 'min_temp', 'cloud_coverage',
                           'humidity', 'main', 'cod', 'latitude', 'longitude', 'pressure',
                           'country', 'sys_message', 'sunrise', 'sunset',
                           'visibility', 'wind_speed'
                           ]

            data = pd.DataFrame(columns=columnNames)

            df_All = json_normalize(weatherData.json())

            df_Weather = pd.DataFrame.from_dict(weatherData.json()['weather'])

            dfTemp = pd.concat([df_All, df_Weather], axis=1)

            dfTemp.rename(columns={
                'dt': 'date',
                'clouds.all': 'cloud_coverage',
                'coord.lat': 'latitude',
                'coord.lon': 'longitude',
                'main.humidity': 'humidity',
                'main.pressure': 'pressure',
                'main.temp': 'temp',
                'main.temp_max': 'max_temp',
                'main.temp_min': 'min_temp',
                'sys.country': 'country',
                'sys.id': 'id',
                'sys.message': 'sys_message',
                'sys.sunrise': 'sunrise',
                'sys.sunset': 'sunset',
                'sys.type': 'sys_type',
                'wind.deg': 'wind_direction_degrees',
                'wind.speed': 'wind_speed',
                'name': 'city'
            }, inplace=True)
            dfTemp['date'] = datetime.datetime.fromtimestamp(dfTemp['date'].values).strftime('%Y-%m-%d')

            for eachCol in data.columns:
                data[eachCol] = dfTemp[eachCol]

            data['temp'] = data['temp'] - 273.15
            data['max_temp'] = data['max_temp'] - 273.15
            data['min_temp'] = data['min_temp'] - 273.15

            data.fillna(value='unknown', axis=1, inplace=True)

        except Exception as e:
            message_['error'] = "Something went wrong processing the JSON to a dataframe"
            message_['traceback'] = config.get_traceback(e)
            data = ''
            # print(message_['error'])
            pass

        return message_, data

    def fetch(self):
        '''
            Pushes data to the SQL database
            TO DO:  write main code
            '''

        message_ = {
            'error': None,
            'method': 'apiutility.openweathermap_API_interaction.fetch',
            'description': "calls the functions to collect data and process it for each city passed to the object",
            'misc': None
        }

        try:
            for eachCity in self.cityList:
                print(eachCity)

                message = OpenWeatherMap.weather_API_request(self,city=eachCity)
                config.abort_pass_programme(objMessage=message, logMessage=' Data collected for ' + eachCity)

                message, data = OpenWeatherMap.process_weather_reading(self,self.weatherReading)
                config.abort_pass_programme(objMessage=message, logMessage=' Data processed for ' + eachCity)

                if not self.weatherDF.empty:
                    self.weatherDF = self.weatherDF.append(data, ignore_index=True)
                else:
                    self.weatherDF = data

        except Exception as e:
            message_['error'] = "Something went wrong fetching data for a city"
            message_['traceback'] = self.config.get_traceback(e)
            data = ''
            # print(message_['error'])
            pass

        return message_, self.weatherDF

