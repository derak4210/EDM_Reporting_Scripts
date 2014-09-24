import os, csv, itertools, sys
from upload import upload
import traceback
#basepath = 'C:/cygwin64/home/Damir/ProjectRescue/AdManager/'
basepath = '/tmp/ProjectRescue/AdManager/'
advertisers = os.listdir(basepath)

class MyOutput():
    def __init__(self, logfile):
        self.stdout = sys.stdout
        self.log = open(logfile, 'w')
 
    def write(self, text):
        self.stdout.write(text)
        self.log.write(text)
        self.log.flush()
 
    def close(self):
        self.stdout.close()
        self.log.close()
sys.stdout = MyOutput("AM_Log.txt")
for adv in advertisers:
    print("Now processing %s" %adv)
    try:
        upload_rows = []
        dailyfiles = os.listdir(basepath + adv + '/Data/')
        for file in dailyfiles:
            reader = csv.reader(open(basepath + adv + '/Data/' + file,'rb'))
            date = file[-14:-4]
            for row in reader:
                if ('Advertiser' in row):
                    continue
                else:
                    new_row = [date]
                    for element in row:
                        if element=="n/a":
                            element=0
                        if element == "Unknown Advertiser ":
                            element = adv
                        new_row.append(element)
                upload_rows.append(new_row)
        if not len(upload_rows)==0:
            upload(upload_rows,"advertiser_daily_ad_summary")
    except:
        print(traceback.print_exc())
