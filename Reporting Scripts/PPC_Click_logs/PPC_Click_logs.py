from download_module import *
import sys, traceback, urllib, csv, datetime

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
        new_row = sourceID,sourceName,pubID,pubName
        sources.add(new_row)

    return (sources)
def mail_error_report(global_error_count):
    msg = MIMEMultipart('alternative')
    msg['Subject'] = 'PPC CLICK LOG PROCESSING TOO MANY ERROR ROWS: %s' %global_error_count
    #Connect
    s = smtplib.SMTP()
    s.connect('mail.exciteholidays.com', '25')
    s.ehlo()
    s.sendmail('reporter@excitedigitalmedia.com','damir@excitedigitalmedia.com',msg.as_string())


def main(date):
    print("Parsing Click Log terms for %s" %(date))
    global global_error_count 
    global_error_count = 0
    id_map = create_map(date)
    global_start = datetime.datetime.now()
    for sourceID,sourceName,pubID,pubName in id_map:
        start = datetime.datetime.now()
        print("%s-%s - [%s]" %(pubName,sourceName, start))
        try:
            errors = download_parse_raw_click_log(date,sourceID,sourceName,pubID,pubName)
            global_error_count = global_error_count + errors
            errors = download_parse_final_click_log(date,sourceID,sourceName,pubID,pubName)
            global_error_count = global_error_count + errors
            #errors = download_parse_campaign_logs(date,sourceID,sourceName,pubID,pubName)
            #global_error_count = global_error_count + errors
        except:
            print("ERROR PARSING CLICK LOG")
            traceback.print_exc()
        end = datetime.datetime.now()
        print("END [%s]" % end)
        time_diff = end-start
        print("IT TOOK %s to process %s"%(time_diff,sourceName))

    global_end = datetime.datetime.now()
    print("ENTIRE PROCESS TOOK %s AND THERE WAS %s ERROR ROWS"%(global_end-global_start,global_error_count))
    if global_error_count>100:
        mail_error_report(global_error_count)

if __name__ == '__main__':
  main(sys.argv[1])
  #main("")