from processor import process_report
from uploader import upload_data
from mail_module import mail
import hello_analytics_api_v3_auth
import traceback,sys,os

def main(curr_day):
    # Step 1. Get an analytics service object.
    service = hello_analytics_api_v3_auth.initialize_service()
    #logfile = open("log.log",'wb')
    #accounts_map = { 
    #    "Daily Life" : "22700242", #AKA Fairfax Media - Metro
    #    "Drive" : "22516094", 
    #    "TP New Car Showroom" : "2019139", 
    #    "Kidspot" : "225257", 
    #    "Living Social" : "3767395", 
    #    "Mazda" : "10349309", 
    #    } 
    
    ##accounts = service.management().accounts().list().execute()

    #for account_name,account_id in accounts_map.iteritems():
    #    logfile.write(str('\n'+account_name+'\n'))
    #    #Grab web properties and their IDS
    #    logfile.write("\nWEB PROPERTIES\n")
    #    webproperties = service.management().webproperties().list(accountId=account_id).execute()

    #    for web_p in webproperties.get('items'):
    #        logfile.write('\n'+str(web_p))
    #        #obtain profile ID
    #        profiles = service.management().profiles().list(
    #        accountId=account_id,
    #        webPropertyId=web_p['id']).execute()
    #        try:
    #            for profile in profiles['items']:
    #                logfile.write("\nPROFILES\n")
    #                logfile.write('\n'+str(profile))
    #        except:
    #            continue
    #        #logfile.writelines(profiles)

    #logfile.close()
    profile_map = {
        "Daily Life" : "54624166", #AKA Fairfax Media - Metro
#        "Drive" : "44394715", 
        "TP New Car Showroom" : "69339994", 
        "Kidspot" : "299451", 
        "Living Social" : "85692242", 
#        "Mazda" : "10349309", 
	"Ausvance" : "65347538",
        } 
    for profile_name, profile_id in profile_map.iteritems():
        writer = open(profile_name+'.txt','wb')
        try:
            print("Pulling results for %s"%profile_name)
            results = get_results(service, profile_id, curr_day)
            #DO SOMETHING WITH RESULTS
            upload_rows = []
            if results:
                print("Processing Results")
                for row in results['rows']:
                    print(row)
                    try:
                        split_ad_content = str(row[0]).split('_')
                        pub_id=split_ad_content[2]
                        source_id = split_ad_content[3]
                        sub_id = split_ad_content[4]
                        tier = split_ad_content[0].replace('DB-','')
                        product = split_ad_content[1]
                        
                    except:
                        pub_id=''
                        source_id=''
                        sub_id=''
                        tier=''
                        product=''

                    new_row = [curr_day,profile_name,str(row[0]),int(row[1]),int(row[2]),float(row[3]),int(row[4]),int(row[5]),float(row[6]),float(row[7]),float(row[8]),tier,product,pub_id,source_id,sub_id ]
                    upload_rows.append(new_row)

                upload_data(upload_rows,curr_day,profile_name)
        except:

            error = traceback.format_exc()
            mail(error,profile_name)
            continue


def get_results(service, profile_id, date):
    return service.data().ga().get(
      ids='ga:' + profile_id,
      start_date=date,
      end_date=date,
      metrics='ga:users,ga:newUsers,ga:bounceRate,ga:bounces,ga:pageviews,ga:pageviewsPerSession,ga:avgTimeOnPage,ga:avgSessionDuration',
      dimensions='ga:adContent',
      sort='-ga:users',
      max_results=10000,
      filters='ga:sourceMedium=~excitedigitalmedia').execute()

if __name__ == '__main__':
  main(sys.argv[1])
  #main("2014-09-02")
