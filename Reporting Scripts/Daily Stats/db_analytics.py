#!/usr/bin/python27
from download_module import *
from db_upload import upload_data
import sys, traceback
from mail_module import *

#date = '2014-06-27'
date = str(sys.argv[1])

reporting_levels = [
#                    "daily",
#                    "agencies",
#                    "advertisers",
                    

                    "adgroups",
#                   "campaigns"
                    #"ads"
                    ]
reporting_metrics = [
                     "daily"
                     # "countries",
                     ##"device_types",
                     ##"keyword",
                     #"products","tier"
                     ]


for metric in reporting_metrics:
    for level in reporting_levels:
        try:
            for i in range(0,10):
                data = download_report(date,metric, level)

                if len(data)>0:
                    break
            print("Successfully download data for %s for date: %s" % (level,date))
        except:
            print("Download Error")
            traceback.print_exc()
            error = "Download Error for %s level on %s" %(level,date)
            mail(error,level,date)
            break
        try:
            #raw_input("UPLOADING %s ROWS"%len(data))
            error = upload_data(date, metric, level, data)
            mail(error,level,date)
        except:
            #mail out success/failure
            mail(error,level,date)
