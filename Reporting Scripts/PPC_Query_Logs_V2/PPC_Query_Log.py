from download_module import download_parse_query_log
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

def main(date):
    print("Parsing Query terms for %s" %(date))

    id_map = create_map(date)
    global_start = datetime.datetime.now()
    for sourceID,sourceName,pubID,pubName in id_map:
        start = datetime.datetime.now()
        print("%s-%s - [%s]" %(pubName,sourceName, start))
        try:
            download_parse_query_log(date,sourceID,sourceName,pubID,pubName)
        except:
            print("ERROR PARSING QUERY LOG")
            traceback.print_exc()
        end = datetime.datetime.now()
        print("END [%s]" % end)
        time_diff = end-start
        print("IT TOOK %s to process %s"%(time_diff,sourceName))
    global_end = datetime.datetime.now()
    print("ENTIRE PROCESS TOOK %s"%(global_end-global_start))
if __name__ == '__main__':
  main(sys.argv[1])
  #main("")