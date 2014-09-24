import traceback
import MySQLdb as mdb

def chunk(l,n):
    if n<1:
        n=1
    return [l[i:i+n] for i in range(0, len(l),n)]

def upload_data(date, metric, level, data):

    if "daily" in level:
        query = "REPLACE INTO `datablocks_adcenter`.`daily_performance_summary` (`date`,`searches`,`impressions`,`clicks`,`conversions`,`spend`,`ctr`,`conversion_rate`,`avg_ecpm`,`avg_cpc`) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    elif "agencies" in level:
        query = "REPLACE INTO `datablocks_adcenter`.`daily_agencies_performance_summary` (`date`,`agency_id`,`searches`,`impressions`,`clicks`,`conversions`,`spend`,`ctr`,`conversion_rate`,`avg_ecpm`,`avg_cpc`,`name`) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    elif "advertisers" in level:
        query = "REPLACE INTO `datablocks_adcenter`.`daily_advertiser_performance_summary` (`date`,`advertiser_id`,`agency_id`,`searches`,`impressions`,`clicks`,`conversions`,`spend`,`ctr`,`conversion_rate`,`avg_ecpm`,`avg_cpc`,`name`,`currency`,`balancetype`) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    elif "campaigns" in level:
        query = "REPLACE INTO `datablocks_adcenter`.`daily_campaign_performance_summary`  (`date`,`campaigns_id`,`advertiser_id`,`agency_id`,`searches`,`impressions`,`clicks`,`conversions`,`spend`,`ctr`,`conversion_rate`,`avg_ecpm`,`avg_cpc`,`name`, `dailybudget`, `overallbudget`, `deliverytype`) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    elif "adgroups" in level:
        query = "REPLACE INTO `datablocks_adcenter`.`daily_adgroup_performance_summary` (`date`,`adgroups_id`,`campaigns_id`,`advertiser_id`,`agency_id`,`searches`,`impressions`,`clicks`,`conversions`,`spend`,`ctr`,`conversion_rate`,`avg_ecpm`,`avg_cpc`,`name`,`startdate`,`enddate`,`adrotation`,`bid`,`clicks_per_user`,`targeting_type`) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    elif "ads" in level:
        query = "REPLACE INTO `datablocks_adcenter`.`daily_ad_performance_summary` (`date`,`ads_id`,`adgroups_id`,`campaigns_id`,`advertiser_id`,`agency_id`,`searches`,`impressions`,`clicks`,`conversions`,`spend`,`ctr`,`conversion_rate`,`avg_ecpm`,`avg_cpc`,`type`,`title`,`description`,`desc2`,`display_url`,`click_url`,`tracking_code`,`units_id`,`mime_type`,`javascript`) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"

    #for row in data:
    try:
        db = mdb.connect(host="localhost", user="root", db='datablocks_adcenter', port = 2000)
        cur = db.cursor()
        #check if more than 1000 rows then split up
        if len(data)>1000:
            chunked = chunk(data,1000)
            for row in chunked:
                cur.executemany(query,row)
        else:
            #No need to split
            cur.executemany(query,data)

        msg = ("EXECUTED INSERT SUCCESSFULLY FOR %s" %level)
        db.commit()
    except:
        print("ERRORRRR!!!")
        msg = traceback.print_exc()

    return msg