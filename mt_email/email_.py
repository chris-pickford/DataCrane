"""the email module is part of the mt_email package


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

__status__ = "development"
__version__ = 'mdl 2.0.0'
__date__ = "24 October 2018"
__author__ = 'Chris Pickford <drchrispickford@gmail.com>, '


import os
import sys
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.image import MIMEImage

sys.path.append(os.path.join('..','..'))

from DataCrane import config
from DataCrane.dbutility import credentials


class Email(object):

    def __init__(self, dataIn):


        if 'logFileName' in dataIn:
            self.config = config.config(logFileName=dataIn['logFileName'])
        else:
            self.config = config.config()

        self.username = self.config.email['username']
        self.password = self.config.email['password']
        self.myAddress = self.config.email['my address']
        self.addressBook = dataIn['address book']
        self.subject = dataIn['subject']
        self.attachmentPath = dataIn['attachment path']
        self.html = dataIn['html']



    def send_mail(self):
        '''
            Does something
            TO DO:  something
            '''
        message_ = {
            'error': None,
            'method': 'mt_email.Email.Email.send_mail',
            'description': "What does it do",
            'misc': None
        }


        try:

            msg = MIMEMultipart()
            msg['Subject'] = self.subject
            msg['From'] = self.myAddress
            msg['To'] = ','.join(self.addressBook)

            html = self.html
            emailText = MIMEText(html, 'html')
            msg.attach(emailText)

            if self.attachmentPath is not None:
                for eachAttachment in self.attachmentPath:
                    img_data = open(eachAttachment, 'rb').read()
                    image = MIMEImage(img_data, name=os.path.basename(eachAttachment))
                    msg.attach(image)

            s = smtplib.SMTP('mail.****.co.uk:***')
            s.ehlo()
            s.starttls()
            s.login(self.username, self.password)

            msg = msg.as_string()
            s.sendmail(self.myAddress, self.addressBook, msg)
            s.quit()

        except Exception as e:
            message_['error'] = "Something went wrong doing the function " + message_['method']+ ": " + str(e)
            message_['traceback'] = self.config.get_traceback(e)
            #print(message_['error'])
            pass

        return message_