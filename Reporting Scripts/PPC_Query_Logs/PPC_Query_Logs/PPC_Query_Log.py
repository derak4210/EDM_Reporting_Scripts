from download_module import download_parse_query_log
import sys, traceback

def main(sid,date):
    print("Parsing Query terms for Source ID %s for %s" %(sid,date))

    try:
        download_parse_query_log(date,sid)
    except:
        print("ERROR PARSING QUERY LOG")
        traceback.print_exc()

if __name__ == '__main__':
  main(sys.argv[1],sys.argv[2])
  #main("")