#!/usr/bin/python27
import json, csv, urllib, os, urllib2, sys, time, datetime
from logger import *
from datetime import timedelta

def create_map(curr_day):

    url = 'http://login.db.excitedigitalmedia.com/detailedstats.php?auth=db73407306496d474232319f175bdf0bfd2ee360&start_date=' + curr_day + '&end_date=' + curr_day

    resp = urllib.urlopen(url)

    reader = csv.reader(resp)

    #initialize dictionary
    sources = set()
    for row in reader:
        if 'date' in row:
            continue
        sourceID = row[1]
        sourceName = row[2]
        pubID = row[3]
        pubName = row[4]
        feedID = row[5]
        feedName = row[6]
        new_row = sourceID,sourceName#,feedID,feedName
        sources.add(new_row)

    return (sources)

#curr_day = '2014-06-01'
curr_day = str(sys.argv[1])

source_feed_map = create_map(curr_day)

click_types = ["raw","final"]
click_origin = ["feed"]
start = datetime.datetime.now()
for row in source_feed_map:
    sid = row[0]
    #fid = row[2]
    #iterate through campaign and feed click logs
    for origin in click_origin:
        #iterate through raw and final click logs
        for type in click_types:
            #create directory 
            if not os.path.isdir('/home/ec2-user/DB_logs/' + curr_day + '/'):
                os.makedirs('/home/ec2-user/DB_logs/' + curr_day + '/')
            log = init_logger(sid,type,curr_day)
            log.info("Downloading %s Click Log for %s" % (type, sid))
            log_start = datetime.datetime.now()
            log.info("Start Time: %s PDT" % str(log_start))
            requests = []
        
            #build command to execute
            #url = 'http://login.db.excitedigitalmedia.com/parse_raw_data.php?auth=db73407306496d474232319f175bdf0bfd2ee360&request=feed&date=' + curr_day + '&click_type=' + type + '&id=' + fid + '&sid=' + sid
            #filename = '/home/ec2-user/DB_logs/' + curr_day + '/excitedigitalmedia_' + type + '_click_logs_' + sid + '_' + fid + '_' + curr_day +  '.j'
            #filename = 'C:/Users/Damir/Desktop/DB Logs' + '/excitedigitalmedia_' + type + '_click_logs_' + sid + '_' + fid + '_' + curr_day + '_' + '.j'
            command = 'curl \'http://login.db.excitedigitalmedia.com/parse_raw_data.php?auth=db73407306496d474232319f175bdf0bfd2ee360&request=' + origin + '&date=' + curr_day + '&click_type=' + type + '&sid=' + sid + '\' --compressed > /home/ec2-user/DB_logs/' + curr_day + '/excitedigitalmedia_' + origin + '_' + type + '_click_logs_' + sid + '_' + curr_day + '.j'
            print("Command to be executed:\n%s" % command)
            #requests.append((url,filename))
            log.info("----- Requesting log file -----")
            log.info("Command: %s" % command)
            os.system(command)
            #download_logs(requests)
            log_end = datetime.datetime.now()
            log.info("End Time: %s PDT" % str(log_end))
            time_diff = log_end - log_start
            log.info("Total Running time: %s" %str(time_diff))

end = datetime.datetime.now()
print("IT TOOK %s TO DOWNLOAD ALL CLICK LOGS" % str(end-start))
