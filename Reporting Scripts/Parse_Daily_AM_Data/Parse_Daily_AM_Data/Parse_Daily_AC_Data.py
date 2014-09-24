import os, csv, itertools, traceback
from upload import upload
basepath = 'C:/cygwin64/home/Damir/ProjectRescue/AdConnect/Publishers/'

publishers = os.listdir(basepath)
#writer = csv.writer(open('C:/cygwin64/home/Damir/Publisher_Source_Daily.csv','wb'))
#header = 0

for pub in publishers:
    upload_rows = []
    try:
        sources = os.listdir(basepath + pub + '/Sources/')
        for source in sources:
            dailyfiles = os.listdir(basepath + pub + '/Sources/' + source)
            for file in dailyfiles:
                if "Daily" in file:
                    reader = csv.reader(open(basepath + pub + '/Sources/' + source + '/' + file,'rb'))
                    for row in reader:
                        if ('Date' in row):
                            continue
                            
                        else:                
                            new_row = [pub, source]
                            for element in row:
                                new_row.append(element)
                            upload_rows.append(new_row)
    except:
        print("ERROR")
        traceback.print_exc()
    #upload rows
    upload(upload_rows,"publisher_source_daily_summary")