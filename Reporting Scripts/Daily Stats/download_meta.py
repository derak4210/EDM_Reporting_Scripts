#!/usr/bin/python27
from download_meta_data import download_meta
import traceback, smtplib
from email.mime.text import MIMEText 
from email.mime.multipart import MIMEMultipart

meta_stages = [
               "agencies","users","products","tiers",
               #"domains",
               "campaigns","adgroups", 
               #"ads",
               #"adgroups_locales", 
               #"adgroups_browsers", "adgroups_keywords", "adgroups_negative_keywords","adgroups_products","adgroups_allowed_domains",
               #"adgroups_banned_domains","adgroups_site_tiers","adgroups_times","adgroups_tiers",
               #"browsers","timezones","countries",
               #"device_types","devices"
               #"keyword_tiers",
               #"keywords",
               #"locales", "platforms","site_tiers","times","units" 
               ]

for stage in meta_stages:
    try:
        download_meta(stage)
    except:
        msg = MIMEMultipart('alternative')
        error = traceback.print_exc()
        content = MIMEText(error,'plain')
        msg.attach(content)
        msg['Subject'] = 'ERROR DOWNLOAD DATABLOCKS ADCENTER META DATA'
        #Connect
        s = smtplib.SMTP()
        s.connect('mail.exciteholidays.com', '25')
        s.ehlo()
        s.sendmail('damir@excitedigitalmedia.com','damir@excitedigitalmedia.com',msg.as_string())
        continue
    print("Successfully updated %s table" % stage)