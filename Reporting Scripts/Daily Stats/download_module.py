import urllib,urllib2, json, itertools,traceback

def download_meta_data(stage):
    prefix_url = "https://api.adcenter.excitedigitalmedia.com/v2/"
    suffix_url = "?apiKey=4d28cf3ed125ed03&pageSize=1000"
    page_param = "&currentPage="
    full_url = prefix_url + stage + suffix_url

    resp = urllib2.urlopen(full_url)

    json_resp = json.load(resp)

    data = []

    if stage == 'agencies':
        etl = ["id","name","address","address2","city","state","zip","country_code","phone","comments","active"]

    for row in json_resp['data']:
        total_items = str(row['totalItems'])
        total_pages = str(row['totalPages'])
        current_page = str(row['currentPage'])

        for x in range(current_page,total_pages):
            print(x)

        
def download_report(date, metric, level):
    pre_url = "https://api.adcenter.excitedigitalmedia.com/v2/stats/"
    suffix_url = "?apiKey=4d28cf3ed125ed03&order=-date_from&pageSize=100&timezone=Australia/Sydney" #timezone
    date_params = "&date_from=" + date + "&date_to=" + date
    page_param = "&currentPage="

    full_url = pre_url + level + suffix_url + date_params
    opener = urllib2.urlopen(full_url)
    print("Downloading: %s" %full_url)
    #raw_input(opener.read())
    resp = json.load(opener)
    
    try:
        data = []

        for row in resp.itervalues():
            total_items = str(row['totalItems'])
            total_pages = str(row['totalPages'])
            current_page = str(row['currentPage'])
            if total_items=='0':
                continue
            #page through json response if necessary
            num_pages = total_items
            for x in range(1,int(total_pages)+1):
                paged_url = full_url + page_param + str(x)
                resp = urllib2.urlopen(paged_url)

                json_resp = json.load(resp)
                for item_row in json_resp['data']['items']:
                    #set common values
                    searches = str(item_row['searches'])
                    conversions = str(item_row['conversions'])
                    avg_cpc = str(item_row['avg_cpc'])
                    ctr = str(item_row['ctr'])
                    clicks = str(item_row['clicks'])
                    avg_ecpm = str(item_row['avg_ecpm'])
                    conversion_rate = str(item_row['conversion_rate'])
                    try:
                        impressions = str(item_row['impressions'])
                    except:
                        impressions = 0
                    spend = str(item_row['spend'])

                    #determine incommonalities
                    if "daily" in level:
                        item_data_row = [date, searches, impressions, clicks, conversions, spend, ctr, conversion_rate, avg_ecpm, avg_cpc ]
                    elif "agencies" in level:
                        id = str(item_row['id'])
                        #Other potential not essential metrics returned:
                        name = str(item_row['name'])
                        #address = str(item_row['address'])
                        #address2 = str(item_row['address2'])
                        #city = str(item_row['city'])
                        #state = str(item_row['state'])
                        #zip = str(item_row['zip'])
                        #country_code = str(item_row['country_code'])
                        #phone = str(item_row['phone'])
                        #comments = str(item_row['comments'])
                        #active = str(item_row['active'])
                        item_data_row = [date, id, searches, impressions, clicks, conversions, spend, ctr, conversion_rate, avg_ecpm, avg_cpc,name ]
                    elif "advertisers" in level:
                        id = str(item_row['id'])
                        agency_id = str(item_row['agencies_id'])
                        #timezones_id=str(item_row['timezones_id'])
                        username=str(item_row['name'])
                        #first_name=str(item_row['first_name'])
                        #last_name=str(item_row['last_name'])
                        #email=str(item_row['email'])
                        #homepage=str(item_row['homepage'])
                        #balance=str(item_row['balance'])
                        balancetype=str(item_row['invoice_type'])
                        currency=str(item_row['currency'])
                        #api_key=str(item_row['api_key'])
                        item_data_row = [date, id, agency_id,searches, impressions, clicks, conversions, spend, ctr, conversion_rate, avg_ecpm, avg_cpc, username,currency, balancetype ]
                    elif "campaigns" in level:
                        id = str(item_row['id'])
                        agency_id = str(item_row['agencies_id'])
                        adv_id = str(item_row['advertisers_id'])
                        name = str(item_row['name'])
                        dailybudget = float(item_row['dailybudget'])
                        overallbudget = float(item_row['overallbudget'])
                        deliverytype = str(item_row['deliverytype'])
                        active = str(item_row['active'])
                        item_data_row = [date, id, adv_id,agency_id, searches, impressions, clicks, conversions, spend, ctr, conversion_rate, avg_ecpm, avg_cpc, name, dailybudget, overallbudget, deliverytype ]
                    elif "adgroups" in level:
                        #print(item_row)
			id = str(item_row['id'])
                        agency_id = str(item_row['agencies_id'])
                        adv_id = str(item_row['advertisers_id'])
                        camp_id = str(item_row['campaigns_id'])
                        name = str(item_row['name'])
                        startdate = str(item_row['startdate'])
                        enddate = str(item_row['enddate'])
                        adrotation = str(item_row['adrotation'])
                        bid = float(item_row['bid'])
                        clicks_per_user = int(item_row['clicks_per_user'])
                        targeting_type = str(item_row['targeting_type'])
                        active = str(item_row['active'])
                        item_data_row = [date, id, camp_id, adv_id,agency_id,searches, impressions, clicks, conversions, spend, ctr, conversion_rate, avg_ecpm, avg_cpc,name,startdate,enddate,adrotation,bid,clicks_per_user,targeting_type ]
                    elif "ads" in level:
                        id = str(item_row['id'])
                        agency_id = str(item_row['agencies_id'])
                        adv_id = str(item_row['users_id'])
                        camp_id = str(item_row['campaigns_id'])
                        adgroup_id = str(item_row['adgroups_id'])
                        type = str(item_row['type'])
                        title = item_row['title'].encode('utf-8')
                        description = item_row['description'].encode('utf-8')
                        desc2 = item_row['desc2'].encode('utf-8')
                        display_url = str(item_row['display_url'])
                        click_url = str(item_row['click_url'])
                        tracking_code = str(item_row['tracking_code'])
                        units_id = str(item_row['units_id'])
                        mime_type = str(item_row['mime_type'])
                        javascript = str(item_row['javascript'])
                        active = str(item_row['active'])
                        item_data_row = [date, id, adgroup_id,camp_id, adv_id,agency_id, searches, impressions, clicks, conversions, spend, ctr, conversion_rate, avg_ecpm, avg_cpc,type,title,description,desc2,display_url,click_url,tracking_code,units_id,mime_type,javascript ]
                    data.append(item_data_row)
    except:
        print("Something went wrong with this row: %s" %item_row)
        traceback.print_exc()
    return data

