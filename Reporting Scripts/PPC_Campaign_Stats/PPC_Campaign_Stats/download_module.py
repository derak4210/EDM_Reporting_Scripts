import json, urllib, urllib2, os,csv, traceback, string, itertools, datetime, time
from urlparse import urlparse
from db_upload import upload_data

def download_log(date,camp_id, type):
    url = 'http://login.db.excitedigitalmedia.com/parse_raw_data.php?auth=db73407306496d474232319f175bdf0bfd2ee360&request=campaign&click_type=' + type
    date_params = '&date=' + date
    camp_id_param = '&id=' + camp_id

    file_name = '/home/ec2-user/DB_logs/Campaign_logs/' + date + '/' + 'excitedigitalmedia_campaign_' + type + '_click_logs_' + camp_id + '_' + date + '.j'
    #file_name = 'C:/Users/Damir/SkyDrive/Visual Studio Projects/PPC_Campaign_Stats/PPC_Campaign_Stats/' + date + '/excitedigitalmedia_campaign_' + type + '_click_logs_' + camp_id + '_' + date + '.j'

    full_url = url + date_params + camp_id_param
    command = 'curl \'' + full_url + '\' --compressed > ' + file_name
    print("Command to be executed:\n%s" % command)
    os.system(command)

def download_parse_raw_logs(date,camp_name,camp_id):
    url = 'http://login.db.excitedigitalmedia.com/parse_raw_data.php?auth=db73407306496d474232319f175bdf0bfd2ee360&request=campaign&click_type=raw'
    date_params = '&date=' + date
    camp_id_param = '&id=' + camp_id
    data = []
    
    file_name = '/home/ec2-user/DB_logs/Campaign_logs/' + date + '/' + 'excitedigitalmedia_campaign_raw_click_logs_' + camp_id + '_' + date + '.j'
    #file_name = 'C:/Users/Damir/SkyDrive/Visual Studio Projects/PPC_Campaign_Stats/PPC_Campaign_Stats/' + date + '/excitedigitalmedia_campaign_' + type + '_click_logs_' + camp_id + '_' + date + '.j'

    if not os.path.exists(file_name):
        if not os.path.exists('/home/ec2-user/DB_logs/Campaign_logs/' + date + '/'):
            os.makedirs('/home/ec2-user/DB_logs/Campaign_logs/' + date + '/')
        full_url = url + date_params + camp_id_param
        command = 'curl \'' + full_url + '\' --compressed > ' + file_name
        print("Command to be executed:\n%s" % command)
        os.system(command)

    #check if log is populated
    open_file = open(file_name,'rb')
    while 'Please check back shortly' in open_file.read():
        open_file.close()
        download_log(date,camp_id,'raw')
        open_file = open(file_name,'rb')
    
    open_file = open(file_name,'rb')
    count = 0
    for row in open_file:
        #Check if file is fully downloaded or contains error message that it is queued
        print("ROW: %s" %row)
        row = row.replace('\\','')
        try:
            row = unicode(row,'utf-8')
        except:
            traceback.print_exc()
            row = row.decode('utf-8','ignore')
        try:
            json_row = json.loads(row,encoding='utf-8')
            pid = str(json_row['pid'])
            uuid = str(json_row['uuid'])
            sid = str(json_row['sid'])
            query = (json_row['keyword']).encode('utf-8')
            kw = str(json_row['search_keyword'])
            subid = str(json_row['subid'])
            cid = str(json_row['cid'])
            adv_id = str(json_row['aid'])
            ip = str(json_row['ip_addr'])
            country = str(json_row['country'])
            ua = str(json_row['user_agent'])
            ref = str(json_row['ref_url'])
            ref_parse = urlparse(ref)
            if ref=='':
                ref = ''
            else:
                ref = '{uri.scheme}://{uri.netloc}/'.format(uri=ref_parse)
	    gross_bid = str(json_row['gross_bid'])
            net_bid = str(json_row['bid'])
            disp_url = str(json_row['display_url'])
            search_time = datetime.datetime.fromtimestamp(int(json_row['search_time']))   
            search_time =  search_time.strftime('%Y-%m-%d %H:%M:%S')                                                  
            click_time = datetime.datetime.fromtimestamp(int(json_row['click_time']))
            click_time =  click_time.strftime('%Y-%m-%d %H:%M:%S')
            new_row = [date,sid,pid,cid,subid,kw,query,adv_id,ip,country,ua,ref,gross_bid,net_bid, disp_url,click_time,search_time,uuid,camp_name]
            #new_row = [date,camp_id,camp_name,kw,query,ref]
            data.append(new_row)
        except:
            traceback.print_exc()
            count = count +1
            continue


    print("%s ERRORS"%count)
    ##write report
    #file = open("Adlux_Campaign_Raw_Click_Log_" + camp_name + "_" + date + ".csv","wb")
    #wr = csv.writer(file)
    #header = ["date","Campaign ID","Campaign Name","keyword","query_term","referrer"]
    #wr.writerow(header)
    #print("About to write %s rows to the file" %len(data))
    #wr.writerows(data)

    upload_data(data,'raw',date,camp_id)
    print("Remove daily click log")
    os.remove(file_name)

def download_parse_final_logs(date,camp_name,camp_id):
    url = 'http://login.db.excitedigitalmedia.com/parse_raw_data.php?auth=db73407306496d474232319f175bdf0bfd2ee360&request=campaign&click_type=final'
    date_params = '&date=' + date
    camp_id_param = '&id=' + camp_id
    data = []
    
    #file_name = 'C:/Users/Damir/SkyDrive/Visual Studio Projects/PPC_Campaign_Stats/PPC_Campaign_Stats/' + date + '/excitedigitalmedia_campaign_final_click_logs_' + camp_id + '_' + date + '.j'
    file_name = '/home/ec2-user/DB_logs/Campaign_logs/' + date + '/' + 'excitedigitalmedia_campaign_final_click_logs_' + camp_id + '_' + date + '.j'
    if not os.path.exists(file_name):
        if not os.path.exists('/home/ec2-user/DB_logs/Campaign_logs/' + date + '/'):
            os.makedirs('/home/ec2-user/DB_logs/Campaign_logs/' + date + '/')
        full_url = url + date_params + camp_id_param
        command = 'curl \'' + full_url + '\' --compressed > ' + file_name
        print("Command to be executed:\n%s" % command)
        os.system(command)

    #check if log is populated
    open_file = open(file_name,'rb')
    while 'Please check back shortly' in open_file.read():
        open_file.close()
        download_log(date,camp_id,'final')
        #raw_input("download")
        open_file = open(file_name,'rb')

    open_file = open(file_name,'rb')
    count = 0
    for row in open_file:
        #Check if file is fully downloaded or contains error message that it is queued
        while 'Please check back shortly' in row:
            print("Sleeping for 5 minutes for file to be ready")
            time.sleep(300)
            command = 'curl \'' + full_url + '\' --compressed > ' + file_name
            print("Command to be executed:\n%s" % command)
            os.system(command)

        print("ROW: %s" %row)
        row = row.replace('\\','')
        try:
            row = unicode(row,'utf-8')
        except:
            traceback.print_exc()
            row = row.decode('utf-8','ignore')
        try:
            json_row = json.loads(row,encoding='utf-8')
            uuid = str(json_row['uuid'])
            pid = str(json_row['pid'])
            sid = str(json_row['sid'])
            query = (json_row['keyword']).encode('utf-8')
            kw = str(json_row['search_keyword'])
            subid = str(json_row['subid'])
            cid = str(json_row['cid'])
            adv_id = str(json_row['aid'])
            ip = str(json_row['ip_addr'])
            country = str(json_row['country'])
            ua = str(json_row['user_agent'])
            ref = str(json_row['ref_url'])
            ref_parse = urlparse(ref)
            if ref=='':
                ref = ''
            else:
                ref = '{uri.scheme}://{uri.netloc}/'.format(uri=ref_parse)
	    gross_bid = str(json_row['gross_bid'])
            net_bid = str(json_row['bid'])
            disp_url = str(json_row['display_url'])
            search_time = datetime.datetime.fromtimestamp(int(json_row['search_time']))   
            search_time =  search_time.strftime('%Y-%m-%d %H:%M:%S')                                                  
            click_time = datetime.datetime.fromtimestamp(int(json_row['click_time']))
            click_time =  click_time.strftime('%Y-%m-%d %H:%M:%S')
            click_url = str(json_row['click_url'])
            click_type = str(json_row['click_type'])
            new_row = [date,sid,pid,cid,subid,kw,query,adv_id,ip,country,ua,ref,gross_bid,net_bid, disp_url,click_time,search_time,uuid,camp_name]
            #new_row = [date,camp_id,camp_name,kw,query,ref]
            data.append(new_row)
        except:
            traceback.print_exc()
            count = count +1
            continue


    print("%s ERRORS"%count)
    ##write report
    #file = open("Adlux_Campaign_Raw_Click_Log_" + camp_name + "_" + date + ".csv","wb")
    #wr = csv.writer(file)
    #header = ["date","Campaign ID","Campaign Name","keyword","query_term","referrer"]
    #wr.writerow(header)
    #print("About to write %s rows to the file" %len(data))
    #wr.writerows(data)

    upload_data(data,'final',date,camp_id)
    print("Remove daily click log")
    os.remove(file_name)


