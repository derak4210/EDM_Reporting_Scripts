import MySQLdb as mdb
import traceback

def chunk(l,n):
    if n<1:
        n=1
    return [l[i:i+n] for i in range(0, len(l),n)]

def upload_conversions(curr_date,conversions):
    print("INSERTING ROWS INTO DB")
    
    try:
        
        db = mdb.connect(host="localhost", user="root", db='datablocks_adcenter', port=2000)
        cur = db.cursor()
        #first delete rows from table
        del_query = "DELETE FROM `datablocks_adcenter`.`conversion_details_v2` where date='"+curr_date+"'"
        cur.execute(del_query)
        db.commit()
        print("Successfully deleted same day data before inserting new rows")
        #now insert new rows
        query = "INSERT INTO `datablocks_adcenter`.`conversion_details_v2` (`ref_url`,	`camp_id`,	`adv`,	`ad_desc2`,	`product`,	`ad_name`,	`conv_domain`,	`click_time`,	`adg_id`,	`ad_title`,	`adv_locale`,	`click_domain`,	`camp_name`,	`disp_url`,	`click_url`,	`bid`,	`adv_conv_domain`,	`conv_id`,	`device_type`,	`date`,	`search_time`,	`adv_id`,	`agency_id`,	`ad_desc1`,	`kw`,	`ads_id`,	`tiers_id`,	`agency`,	`cost`,	`tier`,	`param1`,	`source_id`,	`adgroup`,	`param2`,	`type`) VALUES (%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s)"
        if len(conversions)>1000:
            chunked = chunk(conversions,1000)
            for row in chunked:
                cur.executemany(query,row)
                db.commit()
        else:
            #No need to split
            cur.executemany(query,conversions)

        #cur.executemany(query,conversions)
        print("EXECUTED INSERT SUCCESSFULLY")
        db.commit()
    except:
        print("ERRORRRR!!!")
        traceback.print_exc()
