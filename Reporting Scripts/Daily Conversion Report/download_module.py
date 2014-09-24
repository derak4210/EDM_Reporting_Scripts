import urllib, urllib2, json, csv
from datetime import datetime, timedelta
import MySQLdb as mdb

def download_conversion_report(curr_day):
    url = 'https://api.adcenter.excitedigitalmedia.com/v2/stats/conversion_details/?apiKey=4d28cf3ed125ed03&pageSize=1000&order=-date&timezone=Australia/Sydney'
    #date_time = datetime.strptime(curr_day,"%Y-%m-%d")
    #prev_day = date_time - timedelta(days=1)
    date_param = '&date:=' + curr_day + ',' + curr_day#&type=Last_Click'
    page_param = '&currentPage='
    print("Download report: %s" %(url+date_param))
    resp = urllib2.urlopen(url+date_param)

    json_resp = json.load(resp)

    #print(json_resp)

    todays_conversions = []
    

    for row in json_resp.itervalues():
        total_pages = int(row['totalPages'])
        total_items = int(row['totalItems'])
        if total_items==0:
            continue
        for x in range(1,total_pages + 1):
            #while (no_more_conversions_for_today == 0):
            paged_url = url + date_param + page_param + str(x)
            paged_resp = json.load(urllib2.urlopen(paged_url))
            for item_row in paged_resp['data']['items']:
                new_row = []
                print(item_row)
                date = curr_day

                #if(date == curr_day):
                ref_url = unicode(item_row['ref_url'])
                camp_id = int(item_row['campaigns_id'])
                adv = unicode(item_row['advertisers_name'])
                ad_desc2 = unicode(item_row['ads_description2'])
                product = unicode(item_row['products_name'])
                ad_name = unicode(item_row['ads_name'])
                conv_domain = unicode(item_row['conversion_domain'])
                click_time = unicode(item_row['click_time'])
                adg_id = int(item_row['adgroups_id'])
                ad_title = unicode(item_row['ads_title'])
                adv_locale = unicode(item_row['advertisers_locale'])
                click_domain = unicode(item_row['click_domain'])
                camp_name = unicode(item_row['campaigns_name'])
                disp_url = unicode(item_row['ads_display_url'])
                click_url = unicode(item_row['ads_click_url'])
                bid = unicode(item_row['adgroups_bid'])
                adv_conv_domain = unicode(item_row['ads_conversion_domain'])
                conv_id = unicode(item_row['conversions_id'])
                device_type = unicode(item_row['device_types_name'])
                
                search_time = unicode(item_row['search_time'])
                adv_id = int(item_row['advertisers_id'])
                agency_id = int(item_row['agencies_id'])
                ad_desc1 = unicode(item_row['ads_description'])
                kw = unicode(item_row['keyword'])
                ads_id = int(item_row['ads_id'])
                tiers_id = int(item_row['tiers_id'])
                agency = unicode(item_row['agencies_name'])
                cost = unicode(item_row['cost'])
                tier = unicode(item_row['tiers_name'])
                param1 = unicode(item_row['param1'])
                source_id = unicode(item_row['source_id'])
                adgroup = unicode(item_row['adgroups_name'])
                param2 = unicode(item_row['param2'])
                type = unicode(item_row['type'])
                new_row = [ref_url ,	camp_id ,	adv ,	ad_desc2 ,	product ,	ad_name ,	conv_domain ,	click_time ,	adg_id ,	ad_title ,	adv_locale ,	click_domain ,	camp_name ,	disp_url ,	click_url ,	bid ,	adv_conv_domain ,	conv_id ,	device_type ,	date ,	search_time ,	adv_id ,	agency_id ,	ad_desc1 ,	kw ,	ads_id ,	tiers_id ,	agency ,	cost ,	tier ,	param1 ,	source_id ,	adgroup ,	param2  , type]
                
                todays_conversions.append(new_row)
                    #elif date < curr_day:
                    #    since we obtain all conversions ordered by date descending, once we see a date which is less than date we are running for, safe to stop looking
                    #    no_more_conversions_for_today = 1
                    #    break

        ##strip out only date part of string to compare against the day the script is running for, if dates equal add to a dictionary of conversions which occurred on that day
        #date_time = str(conversion_row['date_time'])[:10]
        #if date_time == curr_day:
        #    todays_conversions.append(conversion_row)

        ##convert string date from conversion report to a date time object so we can compare dates and stop processing if we see a conversion older than date we are processing for
        #date_time_frmt = datetime.strptime(str(conversion_row['date_time']),'%Y-%m-%dT%H:%M:%S.000Z')
        #curr_day_frmt = datetime.strptime(curr_day,'%Y-%m-%d') #not necessary once we cron using a date time object as input


    return todays_conversions


def build_source_map(curr_day):
    #query database for source map
    db = mdb.connect(host="localhost", user="root", db='datablocks_adcenter')
    #db = mdb.connect(host="localhost", user="root", db='datablocks_adcenter', port=2000)
    cur = db.cursor()
    cur.execute("SELECT pub_id,pub_name,source_id,source_name from `datablocks_adcenter`.`publisher_sources`")
    sources = cur.fetchall()
    return (sources)

def get_names(type, id):
    url = 'https://api.adcenter.excitedigitalmedia.com/v1/' + type + '/' + str(id) + '?apiKey=4d28cf3ed125ed03'

    resp = urllib.urlopen(url)

    json_resp = json.load(resp)

    for row in json_resp['data']['items']:
        if type == "keywords":
            return row['keyword']
        else:
            return row['name']
