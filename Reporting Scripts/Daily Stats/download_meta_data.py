#!/usr/bin/python27
import urllib, sys, traceback, json, collections
import MySQLdb as mdb
from email.mime.text import MIMEText 
from email.mime.multipart import MIMEMultipart
from OrderedSet import *

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
    #keys = OrderedSet()
    for row in json_resp.itervalues():
        total_pages = int(row['totalPages'])
        total_items = int(row['totalItems'])
        if total_items==0:
            continue
        for x in range(1,total_pages + 1):
            paged_url = full_url + page_param + str(x)
            paged_resp = json.load(urllib.urlopen(paged_url))
            for item_row in paged_resp['data']['items']:
                new_row = []
                try:
                    if str(item_row['active'])=='Yes':
                        item_row['active'] = '1'
                    elif str(item_row['active'])=='No':
                        item_row['active'] = '0'
                except:
                    print("No active")
                
                for key,value in item_row.items():
                    
                    if 'date' in key:
                        value = str(value).replace('.000Z','').replace('T',' ')
                    try:
                        if value.encode('utf-8')=="None":
                            value == ''
                        new_row.append(value.encode('utf-8'))
                    except AttributeError:
                        if str(value)=="None":
                            value == ''
                        new_row.append(str(value))
                    
                #data_row = new_row.split(',') 
                data.append(new_row)
    #Upload data to DB
    upload(data,stage)

def upload(data,stage):

    ##build query based off of keys/stage
    #columns = []
    #for key in keys:
    #    if str(key) == "users_id":
    #        key = "advertiser_id"
    #    columns.append('`'+str(key)+'`')

    #determine table name from stage
    if stage=="agencies":
        query = "REPLACE INTO `datablocks_adcenter`.`agencies` (`id`, `name`,`address`,`address2`,`city`,`state`,`zip`,`country_code`,`phone`,`comments`,`active`) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    elif stage =="users":
        query = "REPLACE INTO `datablocks_adcenter`.`advertisers` (`id`,`agencies_id`,`timezones_id`,`username`,`first_name`,`last_name`,`address`,`address2`,`city`,`state`,`zip`,`country_code`,`email`,`phone`,`active`,`homepage`,`balance`,`balancetype`,`currency`,`api_key`) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    elif stage =="products":
        query = "REPLACE INTO `datablocks_adcenter`.`products` (`id`,`name`)VALUES (%s,%s)"
    elif stage =="tiers":
        query = "REPLACE INTO `datablocks_adcenter`.`tiers` (`id`,`name`) VALUES (%s,%s)"
    elif stage =="campaigns":
        query = "REPLACE INTO `datablocks_adcenter`.`campaigns` (`id`,`advertiser_id`,`name`,`dailybudget`,`overallbudget`,`deliverytype`,`active`,`agencies_id`) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)"
    elif stage =="adgroups":
        query = "REPLACE INTO `datablocks_adcenter`.`adgroups` (`id`,`advertiser_id`,`campaigns_id`,`agencies_id`,`name`,`startdate`,`enddate`,`adrotation`,`bid`,`clicks_per_user`,`targeting_type`,`active`) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s"
    
    #try to do exdcutemany
    try:
        #determine if we need to split the bulk into chunks
        db = mdb.connect(host="localhost", user="root", db='datablocks_adcenter', port = 2000)
        cur = db.cursor()
        if len(data)>1000:
            chunked = chunk(data,1000)
            for row in chunked:
                cur.executemany(query,row)
        else:
            #No need to split
            cur.executemany(query,data)
        db.commit()
    except:
        #bulk insert failed for some reason so we need to do 1-1
        traceback.print_exc()
        for row in data:
            try:
                db = mdb.connect(host="localhost", user="root", db='datablocks_adcenter', port = 2000)
                cur = db.cursor()
                print("Query to be executed: %s" %query)
                cur.execute(query)
                db.commit()
            except:
                traceback.print_exc()


