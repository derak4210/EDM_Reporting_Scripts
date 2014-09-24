import json, urllib, urllib2, os,csv, traceback, string, itertools, datetime
from db_upload import upload_data
from urlparse import urlparse
def removeNonAscii(s): return "".join(i for i in s if ord(i)<128)

def groupdata(data):
    lmb = lambda d: d[3]
    sorted_data = sorted(data, key = lmb)

    final = []
    for k, g in itertools.groupby(sorted_data,key = lmb):
        count = 0
        for row in g:
            count = count + 1
        #DUE TO SIZE ONLY CARE ABOUT KWS with COUNT > 100
        if count>100:
            final_row = [row[0],row[1],row[2],row[3],row[4],count]
            final.append(final_row)
    return final
def download_parse_raw_click_log(date,source_id,sourceName,pubID,pubName):
    grouped_data = []
    url = 'http://login.db.excitedigitalmedia.com/parse_raw_data.php?auth=db73407306496d474232319f175bdf0bfd2ee360&request=feed&click_type=raw'
    date_params = '&date=' + date
    sid_param = '&sid=' + source_id
    data = []
    count = 0

    #check if click logs exist
    file_name = '/home/ec2-user/DB_logs/' + date + '/' + 'excitedigitalmedia_feed_raw_click_logs_' + source_id + '_' + date + '.j'
    #file_name = 'C:/Users/Damir/SkyDrive/Visual Studio Projects/PPC_Click_Logs/PPC_Click_Logs/' + date + '/excitedigitalmedia_feed_raw_click_logs_' + source_id + '_' + date + '.j'
    if not os.path.exists(file_name):
        if not os.path.exists('/home/ec2-user/DB_logs/' + date + '/'):
            os.makedirs('/home/ec2-user/DB_logs/' + date + '/')
        full_url = url + date_params + sid_param + hour_param + str(x)
        command = 'curl \'' + full_url + '\' --compressed > ' + file_name
        print("Command to be executed:\n%s" % command)
        os.system(command)
        
    #parse click log
    open_file = open(file_name,'rb')

    for row in open_file:
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
            kw = str(json_row['keyword'])
            subid = str(json_row['subid'])
            fid = str(json_row['fid'])
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
            new_row = [date,source_id,pid,sourceName,pubName,subid,kw,fid,ip,country,ua,ref,gross_bid,net_bid, disp_url,click_time,search_time]
            data.append(new_row)
        except UnicodeDecodeError:
            json_row = json.loads(row,encoding='latin-1')
            pid = str(json_row['pid'])
            kw = str(json_row['keyword'])
            subid = str(json_row['subid'])
            fid = str(json_row['fid'])
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
            new_row = [date,source_id,pid,sourceName,pubName,subid,kw,fid,ip,country,ua,ref,gross_bid,net_bid, disp_url,click_time,search_time]
            data.append(new_row)
        except:
            try:
                row = filter(string.printable.__contains__, row)
                #row = strip_control_characters(row)
                json_row = json.loads(row,encoding='latin-1')
                pid = str(json_row['pid'])
                kw = str(json_row['keyword'])
                subid = str(json_row['subid'])
                fid = str(json_row['fid'])
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
                new_row = [date,source_id,pid,sourceName,pubName,subid,kw,fid,ip,country,ua,ref,gross_bid,net_bid, disp_url,click_time,search_time]
                data.append(new_row)
            except:
                try:
                    row = row.replace('\\','')
                    row = removeNonAscii(row).replace("X\"MT","").replace("X\"","")
                    json_row = json.loads(row,encoding='latin-1')
                    pid = str(json_row['pid'])
                    kw = str(json_row['keyword'])
                    subid = str(json_row['subid'])
                    fid = str(json_row['fid'])
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
                    new_row = [date,source_id,pid,sourceName,pubName,subid,kw,fid,ip,country,ua,ref,gross_bid,net_bid, disp_url,click_time,search_time]
                    data.append(new_row)
                except:
                    traceback.print_exc()
                    count = count +1 
                    continue
    #grouped_data = groupdata(data)

    #UPLOAD DATA
    upload_data(data,'raw',date,source_id,pubID)
    return(count)

def download_parse_final_click_log(date,source_id,sourceName,pubID,pubName):
    grouped_data = []
    url = 'http://login.db.excitedigitalmedia.com/parse_raw_data.php?auth=db73407306496d474232319f175bdf0bfd2ee360&request=feed&click_type=final'
    date_params = '&date=' + date
    sid_param = '&sid=' + source_id
    data = []
    count = 0

    #check if click logs exist
    file_name = '/home/ec2-user/DB_logs/' + date + '/' + 'excitedigitalmedia_feed_final_click_logs_' + source_id + '_' + date + '.j'
    #file_name = 'C:/Users/Damir/SkyDrive/Visual Studio Projects/PPC_Click_Logs/PPC_Click_Logs/' + date + '/excitedigitalmedia_feed_final_click_logs_' + source_id + '_' + date + '.j'
    if not os.path.exists(file_name):
        if not os.path.exists('/home/ec2-user/DB_logs/' + date + '/'):
            os.makedirs('/home/ec2-user/DB_logs/' + date + '/')
        full_url = url + date_params + sid_param + hour_param + str(x)
        command = 'curl \'' + full_url + '\' --compressed > ' + file_name
        print("Command to be executed:\n%s" % command)
        os.system(command)
        
    #parse click log
    open_file = open(file_name,'rb')

    for row in open_file:
        print("ROW: %s" %row)
        row = row.replace('\\','')
        try:
            row = unicode(row,'utf-8')
        except:
            traceback.print_exc()
            row = row.decode('utf-8','ignore')
        try:
            json_row = json.loads(row,encoding='utf-8')
            click_type = str(json_row['click_type'])
            pid = str(json_row['pid'])
            kw = str(json_row['keyword'])
            subid = str(json_row['subid'])
            fid = str(json_row['fid'])
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
            new_row = [date,source_id,pid,sourceName,pubName,subid,kw,fid,ip,country,ua,ref,gross_bid,net_bid, disp_url,click_time,search_time,click_type]
            data.append(new_row)
        except UnicodeDecodeError:
            json_row = json.loads(row,encoding='latin-1')
            click_type = str(json_row['click_type'])
            pid = str(json_row['pid'])
            kw = str(json_row['keyword'])
            subid = str(json_row['subid'])
            fid = str(json_row['fid'])
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
            new_row = [date,source_id,pid,sourceName,pubName,subid,kw,fid,ip,country,ua,ref,gross_bid,net_bid, disp_url,click_time,search_time,click_type]
            data.append(new_row)
        except:
            try:
                row = filter(string.printable.__contains__, row)
                #row = strip_control_characters(row)
                json_row = json.loads(row,encoding='latin-1')
                click_type = str(json_row['click_type'])
                pid = str(json_row['pid'])
                kw = str(json_row['keyword'])
                subid = str(json_row['subid'])
                fid = str(json_row['fid'])
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
                new_row = [date,source_id,pid,sourceName,pubName,subid,kw,fid,ip,country,ua,ref,gross_bid,net_bid, disp_url,click_time,search_time,click_type]
                data.append(new_row)
            except:
                try:
                    row = removeNonAscii(row).replace("X\"MT","").replace("X\"","")
                    json_row = json.loads(row,encoding='latin-1')
                    click_type = str(json_row['click_type'])
                    pid = str(json_row['pid'])
                    kw = str(json_row['keyword'])
                    subid = str(json_row['subid'])
                    fid = str(json_row['fid'])
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
                    new_row = [date,source_id,pid,sourceName,pubName,subid,kw,fid,ip,country,ua,ref,gross_bid,net_bid, disp_url,click_time,search_time,click_type]
                    data.append(new_row)
                except:
                    traceback.print_exc()
                    count = count +1 
                    continue
    #grouped_data = groupdata(data)

    #UPLOAD DATA
    upload_data(data,'final',date,source_id,pubID)
    return(count)

def download_parse_campaign_logs(date,source_id,sourceName,pubID,pubName):
    url = 'http://login.db.excitedigitalmedia.com/parse_raw_data.php?auth=db73407306496d474232319f175bdf0bfd2ee360&request=campaign&click_type=raw'
    date_params = '&date=' + date
    sid_param = '&sid=' + source_id
    data = []
    count = 0
    #file_name = '/home/ec2-user/DB_logs/' + date + '/' + 'excitedigitalmedia_campaign_raw_click_logs' + source_id + '_' + date + '.j'
    file_name = 'C:/Users/Damir/SkyDrive/Visual Studio Projects/PPC_Click_Logs/PPC_Click_Logs/' + date + '/excitedigitalmedia_campaign_raw_click_logs_' + source_id + '_' + date + '.j'

    #parse click log
    open_file = open(file_name,'rb')

    for row in open_file:
        print("ROW: %s" %row)
        row = row.replace('\\','')
        try:
            row = row.encode('utf-8','ignore')
            #row = unicode(row,'utf-8')
        except:
            traceback.print_exc()
            row = row.decode('utf-8','ignore')
        try:
            json_row = json.loads(row,encoding='utf-8')
            pid = str(json_row['pid'])
            kw = (json_row['keyword']).encode('utf-8')
            query = str(json_row['search_keyword'])
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
            new_row = [date,source_id,pid,cid,sourceName,pubName,subid,kw,query,adv_id,ip,country,ua,ref,gross_bid,net_bid, disp_url,click_time,search_time]
            data.append(new_row)
        except UnicodeDecodeError:
            try:

                json_row = json.loads(row,encoding='latin-1')
                pid = str(json_row['pid'])
                kw = str(json_row['keyword'])
                query = str(json_row['search_keyword'])
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
                new_row = [date,source_id,pid,cid,sourceName,pubName,subid,kw,query,adv_id,ip,country,ua,ref,gross_bid,net_bid, disp_url,click_time,search_time]
                data.append(new_row)
            except:
                traceback.print_exc()
        except:
            count = count + 1
            continue

    ##write report
    #file = open("ADLUX_CLICK_CAMPAIGN_LOGS_" + date + ".csv","wb")
    #wr = csv.writer(file)
    #header = ["date","sid","pid","cid","source_name","pub_name","subid","keyword","query_term","adv_id","ip","country","user_agent","referrer","gross_bid","net_bid","display_url","click_time","search_time"]
    #wr.writerow(header)

    #wr.writerows(data)

    upload_data(data,'campaign_raw',date,source_id,pubID)
    return(count)
