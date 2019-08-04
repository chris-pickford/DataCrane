"""the config module is part of the utilityPackagesV2 package


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
__date__    = "09 June 2019"
__author__  = 'Chris Pickford <drchrispickford@gmail.com>, Alice Oberacker'


import sys
import logging
import os
from importlib import reload
from DataCrane.mt_email import email_


class config(object):

    def __init__(self, logFileName = None):


        ##############################################################################
        ## Permissions ##

        self.permissions = {
            'chris.pickford': {

                'GBQ':True,
                'GA_API':True,
                'MongoDB': True,
                'openWeatherMap': True,
                'MSSQLDB': True
            }

        }

        ##############################################################################
        ## Logging ##

        self.statusLog = {
            'OK ': '[__OK__]',
            'WARN': '[_WARN_]',
            'FAIL': '[_FAIL_]'
            }
        self.format = '%(asctime)s %(status)s %(separator)s %(message)s'

        self.dctLog = {
            'FAIL': {
                'status': '[ FAIL ]',
                'separator': ' - '
            },
            'WARN': {
                'status': '[ WARN ]',
                'separator': ' - '
            },
            'OK': {
                'status': '[      ]',
                'separator': ' - '
            }
        }

        # self.logPath = self.set_logging(logFileName)
        # print('Set log function returned: ', self.logPath)

        ##############################################################################
        ## Private keys and service accounrts ##

        self.googleServiceAccountPrivateKey = None




        self.googleAdwordsAPI = {

        }

        self.openWeatherMapAPI = {
            'owm_api_url' : 'http://api.openweathermap.org/data/2.5/weather',
            'owm_appid' : ''
        }

        ##############################################################################
        ## Email ##

        self.email = {
            'username': 'drchrispickford@gmail.com',
            'password': os.environ.get('datascienceAccount'),
            'my address': 'drchrispickford@gmail.com'
        }

        ##############################################################################
        ## Class variables ##

        self.logger = None
        self.debug = True
        self.dataset = None


    def set_logging(self, logFileName):

        cwd = os.getcwd()
        print(cwd)
        logPath = os.path.join(os.path.dirname(os.path.abspath(cwd)), 'Logs')
        print(logPath)
        if not os.path.exists(logPath):
            os.makedirs(logPath)
        print(logFileName)
        self.logFileName = logFileName

        if logFileName != None:
            print('Log file name specified:', logFileName)
            if not os.path.exists(os.path.join(logPath, logFileName)):
                print('Creating log file')
                with open(os.path.join(logPath, logFileName), 'w') as logFile:
                    #logFile.write('New log')
                    pass
            else:
                pass

            logPath = os.path.join(logPath, logFileName)
            print('Log file set: ', logPath)
            logging.shutdown()
            reload(logging)
            logging.basicConfig(filename=logPath, level=logging.DEBUG, format=self.format,
                                datefmt='%d-%m-%Y %H:%M')
            self.logger = logging.getLogger('utility logger')



            return logPath

        else:
            print('Running program with default log file name')
            if not os.path.exists(os.path.join(logPath, 'Log.log')):
                print('creating log file')
                with open(os.path.join(logPath, 'Log.log'), 'w') as logFile:
                    #logFile.write('New log')
                    pass
            logPath = os.path.join(logPath, 'Log.log')
            print('Log file set: ', logPath)

            logging.basicConfig(filename=logPath, level=logging.DEBUG, format=self.format,
                                datefmt='%d-%m-%Y %H:%M')
            self.logger = logging.getLogger('utility logger')

            return logPath




    def get_platform(self):
        platforms = {
            'linux1': 'Linux',
            'linux2': 'Linux',
            'darwin': 'Mac',
            'win32': 'Windows'
        }
        if sys.platform not in platforms:
            return sys.platform

        return platforms[sys.platform]

    def log_this(self, dataIn):
        logging.info(dataIn['logString'], extra=self.dctLog['OK'])

    def get_traceback(self, exc):
        exc_type, exc_value, exc_traceback = sys.exc_info()  # most recent (if any) by default
        traceback_details = {
            'filename': exc_traceback.tb_frame.f_code.co_filename,
            'lineno': exc_traceback.tb_lineno,
            'name': exc_traceback.tb_frame.f_code.co_name,
            'type': exc_type.__name__,
            'message': str(exc),  # or see traceback._some_str()
        }
        del (exc_type, exc_value, exc_traceback)
        return traceback_details


    def abort_pass_programme(self, objMessage=None, logString=None, severity=None, addressBook=None, emailSubject=None,
                             emailAttachment=None):
        """
        Log programme progress, as well as warnings and fails. If crucial error is detected abort programme.

        :param objMessage: Diciotnary with error, description and misc keys.
        :param logString: String to log specific message
        :param severity: warning or critical, aborts programme when critical
        :param addressBook: string with all email addresses comma separated
        :param emailSubject: subject of email
        :param emailAttachment: list of attachment paths
        :return:
        """

        traceback_template = '''Traceback (most recent call last):
                                File "%(filename)s", line %(lineno)s, in %(name)s
                                %(type)s: %(message)s\n'''  # Skipping the "actual line" item
        if self.dataset is None:
            dataName = ''
        else:
            dataName = self.dataset

        # there was an error, so report it
        if objMessage is not None and objMessage['error'] is not None:
            # send an email out about the error
            if addressBook is not None:
                dataIn = {}
                try:
                    dataIn['html'] = '<html><body><p>' + objMessage['error'] + '<br /><br />' + traceback_template % \
                                     objMessage['traceback'] + '</p></body></html>'
                    dataIn['address book'] = addressBook
                    dataIn['subject'] = emailSubject
                    dataIn['attachment path'] = emailAttachment

                    # create a new email object (package mt_email)
                    emailer = email_.Email(dataIn)

                    # send the report to everyone in the address book (see config file)
                    message_ = emailer.send_mail()

                except Exception as e:
                    self.logger.info(dataName + ' email did not send: '+str(e), extra=self.dctLog['WARN'])
                    self.logger.info(traceback_template % self.config.get_traceback(e), extra=self.dctLog['WARN'])
                    pass
            # handle different severity levels
            if severity is not None:
                if severity == 'warning':
                    self.logger.info(dataName + ' ' + objMessage['error']+' in: '+objMessage['method'], extra=self.dctLog['WARN'])
                    self.logger.info(traceback_template % objMessage['traceback'], extra=self.dctLog['WARN'])
                    pass
                elif severity == 'critical':
                    self.logger.info(dataName + ' ' + objMessage['error']+' in: '+objMessage['method'], extra=self.dctLog['FAIL'])
                    self.logger.info(traceback_template % objMessage['traceback'], extra=self.dctLog['FAIL'])
                    sys.exit(1)
            else:
                self.logger.info(dataName + ' ' + objMessage['error']+' in: '+objMessage['method'], extra=self.dctLog['FAIL'])
                self.logger.info(traceback_template % objMessage['traceback'], extra=self.dctLog['FAIL'])
                print(objMessage['error'])
                sys.exit(1)

        # no error, just report on status
        elif logString is not None:
            if (severity == 'debug') or self.debug:
                self.logger.info(dataName + ' ' + logString, extra=self.dctLog['OK'])

