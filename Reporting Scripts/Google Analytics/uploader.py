import MySQLdb as mdb
import traceback

def upload_data(data,date,name):


    query = "REPLACE INTO `third_party_data`.`google_analytics_ad_content_daily` (`date`,`advertiser`,`ad_content`,`users`,`new_users`,`bounce_rate`,`bounces`,`pageviews`,`pageviews_per_session`,`avg_time_on_page`,`avg_session_duration`,`tier`,`product`,`pub_id`,`source_id`,`sub_id`) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"

    try:
        db = mdb.connect(host="localhost", user="root", db='datablocks_adcenter', port = 2000)
        cur = db.cursor()

        #first delete rows from table
        del_query = "DELETE FROM `third_party_data`.`google_analytics_ad_content_daily` where date='"+date+"' and advertiser='" + name + "'"
        cur.execute(del_query)
        db.commit()

        #check if more than 1000 rows then split up
        if len(data)>1000:
            chunked = chunk(data,1000)
            for row in chunked:
                cur.executemany(query,row)
        else:
            #No need to split
            cur.executemany(query,data)

        msg = ("EXECUTED INSERT SUCCESSFULLY FOR %s" %date)
        db.commit()
    except:
        print("ERRORRRR!!!")
        msg = traceback.print_exc()
