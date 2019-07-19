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
__version__ = 'mdl 2.0.1'
__date__    = "24 October 2018"
__author__  = 'Chris Pickford <drchrispickford@gmail.com>', \
                'Alice Oberacker'



import pandas as pd
import numpy as np
import time as tm
import datetime
import os
import sys

import requests
import pandas as pd
from pandas.io.json import json_normalize
from googleads import adwords

sys.path.append(os.path.join('..','..'))

from DataCrane import config
from DataCrane.dbutility import credentials


class ExecuteAPICall(object):
    def __init__(self, connection = None, query = None, placeHolderReplacements= None, outputPath = None):

        self.connection = connection
        self.queryScript = query
        self.placeHolderReplacements = placeHolderReplacements
        self.outputPath = outputPath
        self.connect_to = self.connection.connect_to

        print('Log file name is ',self.connection.logFileName)

        self.config = self.connection.config
        '''
        if self.connection.logFileName is not None:

            self.config = config.config(logFileName=self.connection.logFileName)
        else:
            self.config = config.config()
        '''
        self.connectionApproval = self.prepare_to_execute()


    def prepare_to_execute(self):
        '''

        :return:
        '''

        message_ = {
            'error': None,
            'method': 'dbutility.gnrl_api_interaction.ExecuteAPICall.prepare_to_execute',
            'description': "prepares SQL script for execution",
            'misc': False
        }

        self.config.abort_pass_programme(objMessage=message_, logString='-------Execute APICall object created-------')
        self.config.abort_pass_programme(objMessage=message_, logString='Attempting connection to ' + self.connect_to)

        message_ = self.permissions_check()
        self.config.abort_pass_programme(objMessage=message_, logString='Approval to execute: granted')

        return message_['misc']

    def execute_script(self):

        message_ = {
            'error': None,
            'method': 'dbutility.gnrl_api_interaction.ExecuteAPICall.execute_script',
            'description': "runs the execution function from the connection object",
            'misc': True
        }

        try:

            if self.connectionApproval:
                message_, data = self.connection.execute_script(self.queryScript)

                self.config.abort_pass_programme(objMessage=message_, logString='Data collection complete')
                print(data)

                if (self.outputPath != None) & (message_['error']==None):
                    data.to_csv(self.outputPath)
                    self.config.abort_pass_programme(objMessage=message_, logString='Export to CSV')
                    print('CSV saved to: ' + self.outputPath)

        except Exception as e:
            message_['error'] = "Something went wrong executing the script: "+self.queryScript
            message_['traceback'] = self.config.get_traceback(e)
            data = None
            pass

        self.config.abort_pass_programme(objMessage=message_, logString='-------API data call complete-------')
        return message_, data

    def permissions_check(self):
        '''
            Does something
            TO DO:  something
            '''
        message_ = {
            'error': None,
            'method': 'dbutility.gnrl_api_interaction.ExecuteAPICall.permissions_check',
            'description': "Checks to see if this user has permission to use the function being run",
            'misc': False
        }
        try:
            if self.config.permissions[self.connection.credentials.username][self.connect_to]:
                message_['misc']=True
            else:
                message_['error'] = "User doesn't have permission to connect to  " + self.connect_to


        except Exception as e:
            message_['error'] = "Something went wrong doing the function permissions_check"
            message_['traceback'] = self.config.get_traceback(e)
            # print(message_['error'])
            pass

        return message_

class OpenWeatherMapConnection(object):
    '''
    __status__  = "development"
    __version__ = 'mdl 1.0.1'
    __date__    = "21 June 2018"
    __author__  = 'Chris Pickford <chris.pickford@onthebeach.co.uk> '
    '''
    def __init__(self,dataIn):


        self.connect_to = 'openWeatherMap'

        if 'credentials' in dataIn.keys():
            self.credentials = dataIn['credentials']
        else:
            newUser = credentials.Credentials()
            self.credentials = newUser.capture_credentials()
        self.credentials.username = self.credentials.username

        if 'logFileName' in dataIn.keys():
            self.config = config.config(logFileName=dataIn['logFileName'])
            self.logFileName = dataIn['logFileName']
            #self.connection.config.logPath = self.connection.config.set_logging(dataIn['logFileName'])
        else:
            self.config = config.config()
            self.logFileName = None

        #self.config = config.config()


        self.api_url = self.config.openWeatherMapAPI['owm_api_url']
        self.appid = self.config.openWeatherMapAPI['owm_appid']


        print('Open Weather Map API object created')

    def execute_script(self, query=None):
        '''
            Pushes data to the SQL database
            TO DO:  write main code
            '''

        message_ = {
            'error': None,
            'method': 'apiutility.gnrl_API_interaction.OpenWeatherMapConnection.execute_script',
            'description': "calls the functions to collect data and process it for each city passed to the object",
            'misc': None
        }
        data = pd.DataFrame
        try:

            '''
            print('starting query execution')
            print(query)
            with open(query, 'r') as f:
                cityList = [line.split()[0] for line in f]

            print('city list created')
            print(cityList)
            '''

            for eachCity in query.split():
                print(eachCity)
                try:
                    message_, weatherReading = self.weather_API_request(city= eachCity+ ',UK')
                    self.config.abort_pass_programme(
                        objMessage=message_, logString=' Data collected for ' + eachCity, severity='warning')

                    message_, weatherReading = self.process_weather_reading(weatherData=weatherReading)
                    self.config.abort_pass_programme(
                        objMessage=message_, logString=' Data processed for ' + eachCity, severity='warning')

                    if message_['error'] == None:
                        if not data.empty:
                            data = data.append(weatherReading, ignore_index=True)
                        else:
                            data = weatherReading
                    else:
                        pass
                except:
                    pass

        except Exception as e:
            message_['error'] = "Something went wrong fetching data"
            message_['traceback'] = self.config.get_traceback(e)
            data = None
            print(message_['error'])
            pass

        return message_, data

    def weather_API_request(self,city):
        '''
            Accepts the path to a script file, then checks / produces a viable SQL script
            TO DO:  write the security check function
            '''

        message_ = {
            'error': None,
            'method': 'apiutility.gnrl_API_interaction.OpenWeatherMapConnection.weather_API_request',
            'description': "connects to openweathermap and collects weather data",
            'misc': None
        }

        try:

            print('requesting data')
            data = requests.get(url=self.api_url, params=dict(q=city, APPID=self.appid))


        except Exception as e:
            message_['error'] = "Something went wrong collecting data from the API"
            message_['traceback'] = self.config.get_traceback(e)
            data = None
            print(message_['error'])
            pass
        return message_, data

    def process_weather_reading(self, weatherData):
        '''
            Pushes data to the SQL database
            TO DO:  write main code
            '''

        message_ = {
            'error': None,
            'method': 'apiutility.gnrl_API_interaction.OpenWeatherMapConnection.process_weather_reading',
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
            message_['traceback'] = self.config.get_traceback(e)
            data = None
            print(message_['error'])
            pass

        return message_, data

class AdwordsAPIConnection(object):

    def __init__(self, config, yamlFile, credentials = None, logFileName=None):
        self.connect_to = 'AdwordsAPI'

        if credentials is not None:
            self.credentials = credentials
        else:
            newUser = credentials.Credentials()
            newUser.capture_credentials()
            self.credentials = newUser

        self.config = config

        if logFileName is not None:
            self.logFileName = logFileName
        else:
            self.logFileName = None

        self.yamlFile = yamlFile

        print('API connection object created.  Pass this to a general_database_interaction.executeSQL object for best results')

    def execute_script(self, query=None, **kwargs):
        '''
        Executes SQL script
        '''
        message_ = {
            'error': None,
            'method': 'dbutility.gnrl_api_interaction.AdwordsAPIConnection.execute_script',
            'description': "Executes a SQL script",
            'misc': None
        }

        try:
            timeStart = tm.time()

            clientId = self.config.googleAdwordsAPI['clientId']
            #adwords.AdWordsClient.soap_impl='suds'
            adwords_client = adwords.AdWordsClient.LoadFromStorage(self.yamlFile)
            #adwords_client.soap_impl='suds'
            adwords_client.SetClientCustomerId(clientId)
            report_downloader = adwords_client.GetReportDownloader(version='v201809')
            #print('Executing script:\n', query)

            try:

                f = open('temp.csv', 'wb')
                # load data into file
                report_downloader.DownloadReportWithAwql(
                    query, 'CSV', output=f, skip_report_header=True, skip_column_header=False,
                    skip_report_summary=True, include_zero_impressions=True)
                f.close()

                data = pd.read_csv('temp.csv')
                os.remove('temp.csv')


            except Exception as e:
                print(str(query))
                print('execution failed: \n', e)
                message_['error'] = str(query) + 'was not executed'
                message_['traceback'] = self.config.get_traceback(e)
                data = None
                return message_, data
            finally:
                _ = self.config.set_logging(self.config.logFileName)

            runTime = np.round((tm.time() - timeStart) / 60, 1)
            message_['misc'] = 'Time elapsed for executing script: ' + str(runTime) + 'min'

        except Exception as e:
            message_['error'] = "Something went wrong doing the function execute_script"
            message_['traceback'] = self.config.get_traceback(e)
            # print(message_['error'])
            data = None
            pass

        return message_, data

class GaAPIConnection(object):

    def __init__(self, config, credentials=None, logFileName=None):

        self.connect_to = 'GA_API'
        from googleapiclient.discovery import build
        from oauth2client.service_account import ServiceAccountCredentials

        if credentials is not None:
            self.credentials = credentials
        else:
            newUser = credentials.Credentials()
            newUser.capture_credentials()
            self.credentials = newUser

        if logFileName is not None:
            self.config = config  # config.config(logFileName=dataIn['logFileName'])
            self.logFileName = logFileName
            # self.connection.config.logPath = self.config.set_logging(self.logFileName)
        else:
            self.config = config  # config.config()
            self.logFileName = None

        self.SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']


        self.privateKey = self.config.googleServiceAccountPrivateKey

        GAcredentials = ServiceAccountCredentials.from_json_keyfile_name(
            self.privateKey, self.SCOPES)
        self.analytics = build('analyticsreporting', 'v4', credentials=GAcredentials)


    def execute_script(self, query=None, tableName=None, nbRowInserts=None):
        '''
        Executes SQL script
        '''
        message_ = {
            'error': None,
            'method': 'dbutility.gnrl_api_interaction.GaApiConnection.execute_script',
            'description': "Executes a GA API request",
            'misc': None
        }

        try:
            timeStart = tm.time()
            print('Executing script:\n', query)

            try:
                response = self.analytics.reports().batchGet(body= query).execute()
                #print(response)

                try:
                    #data = self.parse_API_response(response)
                    data = self.GA_API_to_dataframe(response)
                    if response.get('nextPageToken') != None:
                        print('fetching next page')
                        response = self.analytics.reports().batchGet(body=response.get('nexPageToken')).execute()
                        dataPage = self.GA_API_to_dataframe(response)
                        data = pd.concat([data, dataPage],axis=0)

                except:
                    pass

            except Exception as e:
                print(str(query))
                print('execution failed: \n', e)
                message_['error'] = str(query) + 'was not executed (' + str(e) + ')'
                data = None
                return message_, data

            runTime = np.round((tm.time() - timeStart) / 60, 1)
            message_['misc'] = 'Time elapsed for executing script: ' + str(runTime) + 'min'

        except Exception as e:
            message_['error'] = "Something went wrong doing the function execute_script: " + str(e)
            # print(message_['error'])
            data = None
            pass

        return message_, data


    def GA_API_to_dataframe(self,response):
        list = []
        # get report data
        for report in response.get('reports', []):
            # set column headers
            columnHeader = report.get('columnHeader', {})
            dimensionHeaders = columnHeader.get('dimensions', [])
            metricHeaders = columnHeader.get('metricHeader', {}).get('metricHeaderEntries', [])
            rows = report.get('data', {}).get('rows', [])

            for row in rows:
                # create dict for each row
                dict = {}
                dimensions = row.get('dimensions', [])
                dateRangeValues = row.get('metrics', [])

                # fill dict with dimension header (key) and dimension value (value)
                for header, dimension in zip(dimensionHeaders, dimensions):
                    dict[header] = dimension

                # fill dict with metric header (key) and metric value (value)
                for i, values in enumerate(dateRangeValues):
                    for metric, value in zip(metricHeaders, values.get('values')):
                        # set int as int, float a float
                        if ',' in value or '.' in value:
                            dict[metric.get('name')] = float(value)
                        else:
                            dict[metric.get('name')] = int(value)

                list.append(dict)

            df = pd.DataFrame(list)
            return df