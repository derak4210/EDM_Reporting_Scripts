#!/usr/bin/python27
dlux/excitedigitalmedia/g/
import urllib, sys, traceback, json, collections
dlux/excitedigitalmedia/g/
import MySQLdb as mdb
dlux/excitedigitalmedia/g/
from email.mime.text import MIMEText 
from email.mime.multipart import MIMEMultipart
from OrderedSet import *
#from ordereddict import OrderedDict

def chunk(l,n):
    if n<1:
        n=1
    return [l[i:i+n] for i in range(0, len(l),n)]

def download_meta(stage):
dlux/excitedigitalmedia/g/

    prefix_url = "https://api.adcenter.excitedigitalmedia.com/v2/"
dlux/excitedigitalmedia/g/
    suffix_url = "?apiKey=4d28cf3ed125ed03&pageSize=1000"
dlux/excitedigitalmedia/g/
    page_param = "&currentPage="
    full_url = prefix_url + stage + suffix_url
dlux/excitedigitalmedia/g/

    resp = urllib.urlopen(full_url)
dlux/excitedigitalmedia/g/

    json_resp = json.load(resp)
dlux/excitedigitalmedia/g/

    data = []
    keys = OrderedSet()
dlux/excitedigitalmedia/g/
    for row in json_resp.itervalues():
dlux/excitedigitalmedia/g/
        total_pages = int(row['totalPages'])
dlux/excitedigitalmedia/g/
        total_items = int(row['totalItems'])
dlux/excitedigitalmedia/g/
        if total_items==0:
dlux/excitedigitalmedia/g/
            continue
        for x in range(1,total_pages + 1):
dlux/excitedigitalmedia/g/
            paged_url = full_url + page_param + str(x)
dlux/excitedigitalmedia/g/
            paged_resp = json.load(urllib.urlopen(paged_url),object_pairs_hook=collections.OrderedDict())
dlux/excitedigitalmedia/g/
            for item_row in paged_resp['data']['items']:
dlux/excitedigitalmedia/g/
                new_row = []
                try:
                    if str(item_row['active'])=='Yes':
dlux/excitedigitalmedia/g/
                        item_row['active'] = '1'
                    elif str(item_row['active'])=='No':
dlux/excitedigitalmedia/g/
                        item_row['active'] = '0'
                except:
                    print("No active")
                
                for key,value in item_row.items():
dlux/excitedigitalmedia/g/
                    
                    if 'date' in key:
                        value = str(value).replace('.000Z','').replace('T',' ')
dlux/excitedigitalmedia/g/
                    try:
                        if value.encode('utf-8')=="None":
                            value == ''
                        new_row.append(value.encode('utf-8'))
                    except AttributeError:
                        if str(value)=="None":
dlux/excitedigitalmedia/g/
                            value == ''
                        new_row.append(str(value))
dlux/excitedigitalmedia/g/
                    keys.add(key)
dlux/excitedigitalmedia/g/
                    
                #data_row = new_row.split(',') 
dlux/excitedigitalmedia/g/
                data.append(new_row)
    #Upload data to DB
    upload(data,keys,stage)
dlux/excitedigitalmedia/g/

def upload(data,keys,stage):
dlux/excitedigitalmedia/g/

    #build query based off of keys/stage
dlux/excitedigitalmedia/g/
    columns = []
dlux/excitedigitalmedia/g/
    for key in keys:
dlux/excitedigitalmedia/g/
        if str(key) == "users_id":
dlux/excitedigitalmedia/g/
            key = "advertiser_id"
dlux/excitedigitalmedia/g/
        columns.append('`'+str(key)+'`')
dlux/excitedigitalmedia/g/

    #determine table name from stage
dlux/excitedigitalmedia/g/
    if stage=="users":
dlux/excitedigitalmedia/g/
        table_name = "advertisers"
dlux/excitedigitalmedia/g/
    else:
dlux/excitedigitalmedia/g/
        table_name = stage
dlux/excitedigitalmedia/g/

    
    #try to do exdcutemany
    try:
        #determine if we need to split the bulk into chunks
dlux/excitedigitalmedia/g/
        string_format_values = ''
dlux/excitedigitalmedia/g/
        for key in keys:
dlux/excitedigitalmedia/g/
            if string_format_values=='':
dlux/excitedigitalmedia/g/
                string_format_values = "%s"
dlux/excitedigitalmedia/g/
            else:
dlux/excitedigitalmedia/g/
                string_format_values = string_format_values + ",%s"
dlux/excitedigitalmedia/g/

        query = "REPLACE INTO `datablocks_adcenter`.`" + table_name + "` " + str(columns).replace('\'','').replace('[','(').replace(']',')') + " VALUES (" + string_format_values + ")"
dlux/excitedigitalmedia/g/
        db = mdb.connect(host="localhost", user="root", db='datablocks_adcenter', port = 2000)
dlux/excitedigitalmedia/g/
        cur = db.cursor()
dlux/excitedigitalmedia/g/
        if len(data)>1000:
            chunked = chunk(data,1000)
            for row in chunked:
                cur.executemany(query,row)
        else:
dlux/excitedigitalmedia/g/
            #No need to split
dlux/excitedigitalmedia/g/
            cur.executemany(query,data)
        db.commit()
    except:
        #bulk insert failed for some reason so we need to do 1-1
dlux/excitedigitalmedia/g/
        traceback.print_exc()
        for row in data:
            try:
                query = "REPLACE INTO `datablocks_adcenter`.`" + table_name + "` " + str(columns).replace('\'','').replace('[','(').replace(']',')') + " VALUES (" + str(row).replace('[','').replace(']','') + ")"
dlux/excitedigitalmedia/g/
                db = mdb.connect(host="localhost", user="root", db='datablocks_adcenter', port = 2000)
dlux/excitedigitalmedia/g/
                cur = db.cursor()
dlux/excitedigitalmedia/g/
                print("Query to be executed: %s" %query)
dlux/excitedigitalmedia/g/
                cur.execute(query)
                db.commit()
            except:
                traceback.print_exc()


