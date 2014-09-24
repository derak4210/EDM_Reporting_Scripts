#!/usr/bin/python27
import urllib, sys, traceback, json, collections
import MySQLdb as mdb
from email.mime.text import MIMEText 
from email.mime.multipart import MIMEMultipart

def chunk(l,n):
    if n<1:
        n=1
    return [l[i:i+n] for i in range(0, len(l),n)]

def download_meta(stage):

    prefix_url = "https://api.adcenter.excitedigitalmedia.com/v2/"
    suffix_url = "?apiKey=4d28cf3ed125ed03&pageSize=1000"
    page_param = "&currentPage="
    full_url = prefix_url + stage + suffix_url

    resp = urllib.urlopen(full_url)

    json_resp = json.load(resp)

    data = []
    keys = ""
    for row in json_resp.itervalues():
        total_pages = int(row['totalPages'])
        total_items = int(row['totalItems'])
        if total_items==0:
            continue
        for x in range(1,total_pages + 1):
            paged_url = full_url + page_param + str(x)
            paged_resp = json.load(urllib.urlopen(paged_url),object_pairs_hook=collections.OrderedDict)
            for item_row in paged_resp['data']['items']:
                new_row = ""
                for key,value in item_row.items():
                    #build dynamic list of item attributes
                    if str(value) =='None':
                        value = 'NULL'
                    if new_row == "":
                        new_row = str(value)
                        keys = key
                    else:
                        new_row = new_row + "," + str(value)
                        keys = keys + "," + str(key)
                data_row = new_row.split(',')
                data.append(data_row)
    #Upload data to DB
    upload(data,keys,stage)

def upload(data,keys,stage):

    #build query based off of keys/stage
    columns = []
    for key in keys.split(','):
        columns.append('`'+str(key)+'`')

    #determine table name from stage
    if stage=="users":
        table_name = "advertisers"
    else:
        table_name = stage

    string_format_values = ''
    for key in keys.split(','):
        if string_format_values=='':
            string_format_values = "%s"
        else:
            string_format_values = string_format_values + ",%s"

    #query = "REPLACE INTO `datablocks_adcenter`.`" + table_name + "` " + str(columns).replace('\'','').replace('[','(').replace(']',')') + " VALUES (" + string_format_values + ")"

    #executemany isnt working, looping over data
    for row in data:
        try:
            query = "REPLACE INTO `datablocks_adcenter`.`" + table_name + "` " + str(columns).replace('\'','').replace('[','(').replace(']',')') + " VALUES (" + str(row).replace('[','').replace(']','') + ")"
            db = mdb.connect(host="localhost", user="root", db='datablocks_adcenter', port = 2000)
            cur = db.cursor()
            cur.execute(query)
        except:
            traceback.print_exc()


stage = str(sys.argv[1])
download_meta("users")