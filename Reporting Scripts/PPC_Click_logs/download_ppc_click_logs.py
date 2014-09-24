import json,urllib,urllib2,os,sys,traceback
def re_download_log(date,hour,camp_id,type):
    url = 'http://login.db.excitedigitalmedia.com/parse_raw_data.php?auth=db73407306496d474232319f175bdf0bfd2ee360&request=campaign&click_type=' + str(type)
    date_params = '&date=' + date
    hour_param = '&hour=' + hour
    camp_id_param = '&id=' + camp_id

    file_name = '/home/ec2-user/DB_logs/Campaign_logs/' + date + '/' + 'excitedigitalmedia_campaign_' + str(type) + '_click_logs_' + camp_id + '_' + date + '_' + hour + '.j'

    full_url = url + date_params + camp_id_param + hour_param
    command = 'curl \'' + full_url + '\' --compressed > ' + file_name
    print("Command to be executed:\n%s" % command)
    os.system(command)
    
def download_logs(date,hour,camp_id,type):

    url = 'http://login.db.excitedigitalmedia.com/parse_raw_data.php?auth=db73407306496d474232319f175bdf0bfd2ee360&request=campaign&click_type=' + str(type)
    date_params = '&date=' + date
    hour_param = '&hour=' + hour
    camp_id_param = '&id=' + camp_id
    
    #file_name = 'C:/Users/Damir/SkyDrive/Visual Studio Projects/PPC_Campaign_Stats/PPC_Campaign_Stats/' + date + '/excitedigitalmedia_campaign_final_click_logs_' + camp_id + '_' + date + '.j'
    file_name = '/home/ec2-user/DB_logs/Campaign_logs/' + date + '/' + 'excitedigitalmedia_campaign_' + str(type) + '_click_logs_' + camp_id + '_' + date + '_' + hour + '.j'
    if not os.path.exists(file_name):
        if not os.path.exists('/home/ec2-user/DB_logs/Campaign_logs/' + date + '/'):
            os.makedirs('/home/ec2-user/DB_logs/Campaign_logs/' + date + '/')
        re_download_log(date,hour,camp_id,type)

    #check if log is populated
    open_file = open(file_name,'rb')
    while 'Please check back shortly' in open_file.read():
        open_file.close()
        print("Waiting for file to be done processing on their end")
        time.sleep(300)
        re_download_log(date,hour,camp_id,type)
        open_file = open(file_name,'rb')

    print("Successfully download file")
    open_file.close()

def main(date,hour):
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
    types = ["raw","final"]
    for camp_name,camp_id in campaigns.iteritems():
        for type in types:
            print("Running %s log for %s"%(type,camp_name))
            try:
                download_logs(date,hour,camp_id,type)
            except:
                traceback.print_exc()

if __name__ == '__main__':
  main(sys.argv[1],sys.argv[2])
  #main("")