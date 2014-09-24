import sys, urllib, json, traceback
import MySQLdb as mdb

def get_accounts():

    #query database for source map
    #db = mdb.connect(host="localhost", user="root", db='datablocks_adcenter')
    db = mdb.connect(host="localhost", user="root", db='datablocks_adcenter', port=2000)
    cur = db.cursor()
    cur.execute("SELECT id,username from `datablocks_adcenter`.`advertisers`")
    accounts = cur.fetchall()
    return (accounts)

def download_report(date,id,acc_name):
    url = 'https://api.adcenter.excitedigitalmedia.com/v2/stats/users/' + str(id) + '/products?apiKey=4d28cf3ed125ed03&pageSize=5000&timezone=10&date_from=' + date + '&date_to=' + date

    opener = urllib.urlopen(url)
    resp = json.load(opener)
    upload_data = []
    for item_row in resp.itervalues():
        total_items = str(item_row['totalItems'])
        
        if total_items=='0':
            continue
        for item_row in resp['data']['items']:
            advertiser_id = id
            advertiser_name = acc_name
            day = date
            product_id = item_row['id']
            product_name = unicode(item_row['name'])
            clicks = item_row['clicks']
            searches = item_row['searches']
            impressions = item_row['impressions']
            conversions = item_row['conversions']
            spend = item_row['spend']
            ctr = item_row['ctr']
            conversion_rate = item_row['conversion_rate']
            avg_ecpm = item_row['avg_ecpm']
            avg_cpc = item_row['avg_cpc']

            upload_row = [day,advertiser_id,advertiser_name,product_id,product_name,clicks,searches,impressions,conversions,spend,ctr,conversion_rate,avg_ecpm,avg_cpc]
            upload_data.append(upload_row)

    return upload_data

def upload(data):
    query = "REPLACE INTO `datablocks_adcenter`.`daily_adv_product_summary` (`date`,`adv_id`,`adv_name`,`product_id`,`product_name`,`clicks`,`searches`,`impressions`,`conversions`,`spend`,`ctr`,`conversion_rate`,`avg_ecpm`,`avg_cpc`) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"

    try:
        db = mdb.connect(host="localhost", user="root", db='datablocks_adcenter', port = 2000)
        cur = db.cursor()
        cur.executemany(query,data)
        db.commit()
    except:
        traceback.print_exc()
def main(date):
    accounts = get_accounts()

    for id,acc_name in accounts:
        print("Running daily product performance for %s" %acc_name)
        data = download_report(date,id,acc_name)
        if len(data)>0:
            upload(data)
if __name__ == '__main__':
    main(sys.argv[1])
    #main('2014-08-01')
