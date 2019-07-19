from datetime import datetime, timedelta
import os
import logging
import sys

OS = 'Local'

if OS == 'Server':
    sys.path.append(os.path.join(os.path.expanduser('~'), os.path.sep, 'PYTHON','Datascience Packages'))
elif OS == 'Local':
    sys.path.append(os.path.join(os.path.expanduser('~'), 'Documents', 'Coding', 'Python', 'Projects'))

from DataCrane.config import config

SqlVarReplacements = {
    '@StartDate': '2018-09-01',
    '@EndDate': '2018-10-01',

}

class credentials(object):
    def __init__(self):
        self.username = 'datascience'
        self.password = os.environ.get('datascienceAccount')

class privateConfig(config):
    def __init__(self, OS, PROJECT_NAME):
        config.__init__(self)


        if OS == 'Local':
            self.ROOT = os.path.join(os.path.expanduser('~'), 'Documents', 'Coding', 'Python', 'Projects')
            self.PROJECT_ROOT = os.path.join(self.ROOT, PROJECT_NAME)
            self.PACKAGE_ROOT = os.path.join(os.path.expanduser('~'), 'Documents', 'Coding', 'Python', 'Projects')
        elif OS == 'Server':
            self.ROOT = os.path.join(os.path.expanduser('D:'), os.path.sep, 'Chris Pickford', 'Projects')
            self.PROJECT_ROOT = os.path.join(self.ROOT, PROJECT_NAME)
            self.PACKAGE_ROOT = os.path.join(os.path.expanduser('D:'), os.path.sep, 'Datascience Packages')

        # Add the project root and package roots to the path
        sys.path.append(self.ROOT)
        sys.path.append(self.PROJECT_ROOT)
        sys.path.append(self.PACKAGE_ROOT)  ## Project specific imports


        from CodeCandy.haribo import jupyter as candy

        candy.help()
        candy.set_screen_width()## Jupyter Environment setup


    statusLog = {
        'OK ': '[__OK__]',
        'FAIL': '[_FAIL_]'
    }
    format = '%(asctime)s %(status)-8s %(separator)-3s %(message)s'

    dctLog = {
        'FAIL': {
            'status': '[ FAIL ]',
            'separator': ' - '
        },
        'OK': {
            'status': '[      ]',
            'separator': ' - '
        }
    }