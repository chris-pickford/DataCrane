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
__date__    = "10 Oct 2018"
__author__  = 'Chris Pickford <drchrispickford@gmail.com>, ' \
             'Alice Oberacker '


import os
import sys
from copy import deepcopy
from pathlib import Path
import pymssql
import time as tm
import numpy as np
import pandas as pd
from http import HTTPStatus




#from DataCrane import config
from DataCrane.dbutility import credentials



class ExecuteSQL(object):
    '''

    '''

    def __init__(self, connection=None, query=None, sqlReplacements=None, outputPath=None):
        """

        :param connection: The connection object (eg. GBQConnection, MIConnection)
        :param query: The sql script, either the string directly, or a filepath to the sql file location
        :param sqlReplacements: A dictionary containing variable to be replaced and their new value
        :param outputPath: The path to store the output data as csv
        """

        self.connection = connection
        self.queryScript = query
        self.sqlReplacements = sqlReplacements
        self.outputPath = outputPath
        self.connect_to = self.connection.connect_to
        self.config = self.connection.config

        '''
        if 'logFileName' in dataIn:
            print('log file specified')
            self.config = config.config(logFileName=dataIn['logFileName'])
        else:
            print('using default log file')
            self.config = config.config()
        '''
        self.connectionApproval = self.prepare_to_execute()


    def prepare_to_execute(self):
        '''

        :return:
        '''

        message_ = {
            'error': None,
            'method': 'dbutility.gnrldatabase_interaction.ExecuteSQL.prepare_script',
            'description': "prepares SQL script for execution",
            'misc': False
        }


        self.config.abort_pass_programme(objMessage=message_, logString='-------Execute SQL object created-------')
        self.config.abort_pass_programme(objMessage=message_, logString='Attempting connection to ' + self.connect_to)

        if isinstance(self.queryScript, str):
            message_, _ = self.prepare_script(self.queryScript, self.sqlReplacements)

            if message_['error'] is not None:
                return message_
            self.config.abort_pass_programme(objMessage=message_, logString='prepare script')

        message_ = self.permissions_check()
        if message_['error'] is not None:
                return message_
        self.config.abort_pass_programme(objMessage=message_, logString='Approval to execute: granted')


        return message_#['misc']


    def execute_script(self, saveToTable=None, nbRowInserts=None):
        message_ = {
            'error': None,
            'method': 'dbutility.gnrldatabase_interaction.ExecuteSQL.execute_script',
            'description': "runs the execution function from the connection object",
            'misc': True
        }

        try:
            data = None
            if self.connectionApproval:
                #if isinstance(self.connection, MIConnection):

                self.config.abort_pass_programme(objMessage=message_, logString='Executing script')
                message_exec, data = self.connection.execute_script(self.queryScript, saveToTable=saveToTable,
                                                                    nbRowInserts=nbRowInserts)

                '''
                #elif isinstance(self.connection, GBQConnection):
                else:
                    message_, data = self.connection.execute_script(self.queryScript)
                    print(message_)
                '''
                #_ = self.config.set_logging(self.config.logFileName)

                if message_exec['error'] is not None:
                    return message_exec, data
                self.config.abort_pass_programme(objMessage=message_exec, logString='Data collection')


                if (self.outputPath != None) & (message_['error']==None):
                    data.to_csv(self.outputPath)
                    self.config.abort_pass_programme(objMessage=message_, logString='Export to CSV')
                    print('CSV saved to: ' + self.outputPath)

        except Exception as e:
            message_['error'] = "Something went wrong executing the SQL script: "+self.queryScript
            message_['traceback'] = self.config.get_traceback(e)
            pass

        self.config.abort_pass_programme(objMessage=message_, logString='-------Execute SQL script complete-------')

        return message_, data



    def prepare_script(self, scriptName= None, vars=None):
        '''
            Accepts the path to a script file, then checks / produces a viable SQL script
            '''

        message_ = {
            'error': None,
            'method': 'dbutility.gnrldatabase_interaction.ExecuteSQL.prepare_script',
            'description': "converts script into viable sql script",
            'misc': None
        }

        try:
            if '.sql' in scriptName[-4:]:
                # Load SQL template from file
                with open(scriptName, 'r') as scriptFile:
                    queryScript = scriptFile.read()
            else: # queryName is sql script itself
                queryScript = scriptName

            # Replace placeholders with actual values
            if vars != None:
                for key, value in vars.items():
                    queryScript = queryScript.replace(key, value)
            self.queryScript = queryScript
            message_['misc']=True

        except Exception as e:
            message_['error'] = "Something went wrong reading the SQL script: "+scriptName
            message_['traceback'] = self.config.get_traceback(e)
            message_['misc'] = False
            self.queryScript = ''
            # print(message_['error'])
            pass
        return message_, self.queryScript

    def permissions_check(self):
        '''
            Does something
            TO DO:  something
            '''
        message_ = {
            'error': None,
            'method': 'dbutility.gnrldatabase_interaction.ExecuteSQL.permissions_check',
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

class MSSQLConnection(object):

    def __init__(self, config, credentials=None, logFileName=None, server='server_name', database='database_name'):

        self.connect_to = 'MSSQLDB'

        if credentials is not None:
            self.credentials = credentials
        else:
            newUser = credentials.Credentials()
            newUser.capture_credentials()
            self.credentials = newUser
        # self.credentials.username = 'OTB\\' + self.credentials.username
        self.server = server
        self.database = database

        if logFileName is not None:
            self.config = config  # config.config(logFileName=logFileName)
            self.logFileName = logFileName
        else:
            self.config = config  # config.config()
            self.logFileName = None




    def execute_script(self, query=None, saveToTable=None, nbRowInserts=1000):
        """
        Executes custom SQL script or inserts data from pandas DataFrame into defined table.

        :param query: Either Query string or Dataframe with data to push
        :param saveToTable: If query is dataframe, then tableName defines the table to push into
        :param nbRowInserts: If query is dataframe, then nbRowInserts defines how many rows are pushed at onces.
        :return:
        """

        message_ = {
            'error': None,
            'method': 'dbutility.gnrldatabase_interaction.MSSQLConnection.execute_script',
            'description': "Executes a SQL script",
            'misc': None
        }

        if nbRowInserts is None:
            nbRowInserts = 1000
        queries = []
        # in case query is already a valid sql query:
        if isinstance(query, str):
            queries = [query]
        # in case the data comes in a Dataframe transform it into a INSERT query
        if isinstance(query, pd.DataFrame) and saveToTable is not None:
            counter = 0
            data = deepcopy(query)
            try:
                queryMainPart = 'SET ANSI_WARNINGS OFF; INSERT INTO ' + saveToTable + '(' + ', '.join(
                    data.columns) + ')' + 'VALUES {0}; SET ANSI_WARNINGS ON;'
                format_query = queryMainPart.format

                temp = []
                extend_temp = temp.extend
                for i in range(0, len(data), nbRowInserts):
                    #print(counter)
                    # self.config.abort_pass_programme(logString='insert %d rows into query'%(counter))
                    dataChunk = data.iloc[i: i + nbRowInserts]
                    inserts = []
                    for index, row in dataChunk.iterrows():
                        try:
                            temp[:] = []
                            extend_temp(row.values)
                            # remove all " and ' and trim string to 250 characters
                            tmp = [v.replace('"', '').replace("'", '')[:250] if type(v) is str else v for v in temp]
                            inserts.append(tmp)
                            counter += 1
                        except UnicodeEncodeError as e:
                            self.config.abort_pass_programme(logString='Error whilst building query string: %s'%str(e))
                            self.config.abort_pass_programme(logString=','.join([str(t) for t in tmp]))
                            pass

                    queryStr = format_query((str([tuple(v) for v in inserts])[1:-1]))
                    queries.append(queryStr)
            except Exception as e:
                message_['error'] = "Something went wrong transforming the dataframe to a string"
                message_['traceback'] = self.config.get_traceback(e)
                return message_, None

        timeStart = tm.time()
        # print(queries)
        #query is string
        for i, q in enumerate(queries):

            notConnected = True
            nbTries = 0
            while notConnected and nbTries < 3:
                connectionError = None
                try:
                    # print('Executing script:\n', q)
                    with pymssql.connect(server=self.server,
                                         user=self.credentials.username,
                                         password=self.credentials.password,
                                         database=self.database) as dbConnection:
                        try:
                            notConnected = False
                            cursor = dbConnection.cursor()
                            cursor.execute(q)
                            #if cursor.rowcount < nbRowInserts:
                            #    print('rows affected: ', cursor.rowcount)
                            #    self.config.abort_pass_programme(logString='rows affected: ' + str(cursor.rowcount))
                            try:
                                print('Execution complete: ')
                                data = pd.DataFrame(data=cursor.fetchall(), columns= [i[0] for i in cursor.description])
                                print('End of results')
                                dbConnection.commit()
                            except:
                                print('No results to return')
                                dbConnection.commit()
                                data = None
                        except KeyboardInterrupt as interrupt:
                            dbConnection.close()
                            message_['error'] = str(q) + 'was interrupted (' + str(interrupt) + ')'
                            data = None
                            return message_, data
                        except Exception as e:
                            print(str(q))
                            print('execution failed: \n', e)
                            dbConnection.close()
                            message_['error'] = str(q) + 'was not executed'
                            message_['traceback'] = self.config.get_traceback(e)
                            data = None
                            return message_, data

                except Exception as e:
                    connectionError = e
                    connectionTraceback = self.config.get_traceback(e)
                    # connection error, try again
                    nbTries += 1
                    pass
            if notConnected and nbTries==3:
                # connection error 3 times, just leave it be now
                message_['error'] = "Connection to DB failed in execute_script: " + str(connectionError)
                message_['traceback'] = connectionTraceback
                self.config.abort_pass_programme(logString=str(type(connectionError).__name__) + 'Failed to execute this query: ' + q)
                data = None

            runTime = np.round((tm.time() - timeStart) / 60, 1)
            #message_['misc'] = \
            print('Time elapsed for executing script: ' + str(runTime) + 'min')
            # self.config.abort_pass_programme(logString='Time elapsed for executing script: ' + str(runTime) + 'min')
        return message_, data

class GBQConnection(object):

    def __init__(self, config, credentials=None, logFileName =None, useLegacy=False, projectId = None, if_exists='fail'):

        self.connect_to = 'GBQ'


        if credentials is not None:
            self.credentials = credentials
        else:
            newUser = credentials.Credentials()
            newUser.capture_credentials()
            self.credentials = newUser

        if logFileName is not None:
            self.config = config  # config.config(logFileName=dataIn['logFileName'])
            self.logFileName =logFileName
            # self.connection.config.logPath = self.config.set_logging(self.logFileName)
        else:
            self.config = config  # config.config()
            self.logFileName = None

        self.projectId = projectId
        print(self.projectId)
        self.if_exists = if_exists
        self.destinationTable = None


        self.privateKey = self.config.googleServiceAccountPrivateKey


        self.useLegacy = useLegacy

    def execute_script(self, query=None, saveToTable=None, nbRowInserts=None):

        from google.cloud import bigquery
        print(saveToTable)
        print(self.projectId)

        '''
        Executes SQL script
        '''
        message_ = {
            'error': None,
            'method': 'dbutility.gnrldatabase_interaction.GBQConnection.execute_script',
            'description': "Executes a SQL script",
            'misc': None
        }

        try:
            timeStart = tm.time()
            print('Executing script:\n', query)

            try:

                if isinstance(query,str):

                    self.client = bigquery.Client.from_service_account_json(self.privateKey)

                    self.job_config = bigquery.QueryJobConfig()
                    self.job_config.use_legacy_sql = self.useLegacy
                    if saveToTable != None:
                        self.job_config.destination = saveToTable
                    '''
                    query_job = self.client.query(query, job_config=self.job_config)
                    try:
                        data = query_job.to_dataframe()
                    except Exception as e:
                        data = query_job.result()
                    '''
                    # RETRY in case query fails, eg. table not available yet
                    # in case the Constants weren't set in the config, give them default values
                    try:
                        executionTries = self.config.RETRY_COUNT
                        delay = self.config.RETRY_DELAY
                        multiplier = self.config.RETRY_MULTIPLIER
                    except Exception as e:
                        executionTries = 1
                        delay = 0
                        multiplier = 1

                    for ntry in range(executionTries):
                        # use multiplier
                        delay += delay*multiplier
                        # run query
                        try:
                            query_job = self.client.query(query, job_config=self.job_config)
                            try:
                                data = query_job.to_dataframe()
                            except Exception as e:
                                data = query_job.result()
                            break
                        except Exception as bad_request:
                            # sleep for delay in seconds
                            if ntry < executionTries-1:
                                self.config.abort_pass_programme(logString='Job failed, try again in {} seconds'.format(str(delay*60)))
                                tm.sleep(delay*60)
                            else:
                                raise bad_request


                # push DataFrame to big query
                if isinstance(query,pd.DataFrame) and saveToTable is not None:
                    query.to_gbq(
                        destination_table=saveToTable,
                        project_id=self.projectId,
                        if_exists=self.if_exists,
                        chunksize=nbRowInserts
                    )
                    data = None


            except Exception as e:
                print(str(query))
                print('execution failed: \n', e)
                message_['error'] = str(query) + 'was not executed'
                message_['traceback'] = self.config.get_traceback(e)
                data = None
                return message_, data

            runTime = np.round((tm.time() - timeStart) / 60, 1)
            message_['misc'] = 'Time elapsed for executing script: ' + str(runTime) + 'min'

        except Exception as e:
            message_['error'] = "Something went wrong doing the function execute_script"
            message_['traceback'] = self.config.get_traceback(e)
            # print(message_['error'])
            data = None
            pass

        return message_, data