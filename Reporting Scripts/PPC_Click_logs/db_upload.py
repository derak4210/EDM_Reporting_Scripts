import traceback
import MySQLdb as mdb

def chunk(l,n):
    if n<1:
        n=1
    return [l[i:i+n] for i in range(0, len(l),n)]

def upload_data(data, type,date,sid,pid):
    
    if type=='raw':
        del_query = "DELETE FROM `datablocks_ppc_center`.`raw_feed_clicks` WHERE (pid=" + pid + " and sid=" + sid + " and date='" + date + "');"
        query = "INSERT INTO `datablocks_ppc_center`.`raw_feed_clicks` (`date`,`sid`,`pid`,`source_name`,`pub_name`,`subid`,`keyword`,`feed_id`,`ip`,`country`,`user_agent`,`referrer_domain`,`gross_bid`,`net_bid`,`display_url`,`click_time`,`search_time`) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    elif type=='final':
        del_query = "DELETE FROM `datablocks_ppc_center`.`final_feed_clicks` WHERE (pub_id=" + pid + " and source_id=" + sid + " and date='" + date + "');"
        query = "INSERT INTO `datablocks_ppc_center`.`final_feed_clicks` (`date`,`source_id`,`pub_id`,`source_name`,`pub_name`,`subid`,`keyword`,`fid`,`ip`,`country`,`user_agent`,`referrer`,`gross_bid`,`net_bid`,`display_url`,`click_time`,`search_time`,`click_type`) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    elif type=='campaign_raw':
        del_query = "DELETE FROM `datablocks_ppc_center`.`raw_campaign_clicks` WHERE (pid=" + pid + " and sid=" + sid + " and date='" + date + "');"
        query = "INSERT INTO `datablocks_ppc_center`.`raw_campaign_clicks` (`date`,`sid`,`pid`,`cid`,`source_name`,`pub_name`,`subid`,`keyword`,`query_term`,`adv_id`,`ip`,`country`,`user_agent`,`referrer`,`gross_bid`,`net_bid`,`display_url`,`click_time`,`search_time`) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    #for row in data:
    try:
        db = mdb.connect(host="localhost", user="root", db='datablocks_adcenter', port = 2000)
        cur = db.cursor()
        #run deletion script for current pid,sid,date
        cur.execute(del_query)
        #check if more than 1000 rows then split up
        if len(data)>1000:
            chunked = chunk(data,1000)
            for row in chunked:
                cur.executemany(query,row)
        else:
            #No need to split
            cur.executemany(query,data)

        msg = ("EXECUTED INSERT SUCCESSFULLY")
        db.commit()
    except:
        print("ERRORRRR!!!")
        msg = traceback.print_exc()

    return msg