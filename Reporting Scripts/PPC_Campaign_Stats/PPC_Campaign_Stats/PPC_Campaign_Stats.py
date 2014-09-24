from download_module import *
import sys, traceback, urllib, csv, datetime

def create_map(curr_day):

    campaigns = {
                 "23201 AdMedia Ask" : "23",
                 "edmv3 mars Europe" : "21",
                 "23200 Ask AU" : "19",
                 "edmv3 Searchatomic Rakuten Australia" : "15",
                 "edmv73 Exciterewards Global" : "13",
                 "edmv72 Australia" : "11",
                 "edmv7 searchatomic Argentina" : "7",
                 "excitecsr exciterewards USA" : "5",
                 "edmv2 Searchsuggests" : "1",
                 "edmv71 searchatomic Sterkly" : "3",
                 "edmv71 searchatomic Sterkly international" : "9",
                 "23205 Ask AU" : "17",
                 "23210 Ask AU Advertise" : "25"


                 }

    return (campaigns)

def main(date):

    print("Processing campaign level click logs for %s" %date)

    camp_id_map = create_map(date)
    for camp_name,camp_id in camp_id_map.iteritems():
        print("Running for %s"%camp_name)
        try:
            download_parse_raw_logs(date,camp_name,camp_id)
            download_parse_final_logs(date,camp_name,camp_id)
        except:
            traceback.print_exc()



if __name__ == '__main__':
  main(sys.argv[1])
  #main("")
