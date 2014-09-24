import csv, os, traceback, sys
import MySQLdb as mdb

def get_campaigns():

    campaigns = {
                 "23201 AdMedia Ask" : "23",
                 "edmv3 mars Europe" : "21",
                 "23200 Ask AU" : "19",
                 "edmv3 Searchatomic Rakuten Australia" : "15",
                 "edmv73 Exciterewards Global" : "13",
                 "edmv72 Australia" : "11",
                 "edmv7 searchatomic Argentina" : "7",
                 "excitecsr exciterewards USA" : "5",
                 "edmv2 Searchsuggests" : "1",
                 "edmv71 searchatomic Sterkly" : "3",
                 "edmv71 searchatomic Sterkly international" : "9",
                 "23205 Ask AU" : "17",
                 "23210 Ask AU Advertise" : "25"
                 }

    return (campaigns)

def pull_db_data(camp_name,id,date):
    
    db = mdb.connect(host="localhost", user="root", db='datablocks_adcenter', port=2000)
    cur = db.cursor()
    query = "SELECT date as Date,camp_name as Campaign,query_term as Search_KW,`raw_campaign_clicks`.`referrer` as Referrer, pid as Pub_Id,sid as Source_ID,count(*) as Count FROM `datablocks_ppc_center`.`raw_campaign_clicks` where date ='" + date + "' and cid = " + id + " group by date,camp_name,query_term,`raw_campaign_clicks`.`referrer`,pid,sid order by date,camp_name,query_term,pid,sid,count(*) desc;"
    cur.execute(query)
    data = cur.fetchall()

    return (data)
def main(date):
    
    campaigns = get_campaigns()

    for camp_name,id in campaigns.iteritems():
        print("Running daily PPC KW/Ref Reports for %s" %camp_name)
        try:
            data = pull_db_data(camp_name,id,date)
            #Write CSV and push to FileShare
            if len(data)>0:
                base_path = '/home/ec2-user/Reports/PPC_Campaign_Reports/' + date + '/'
                #For testing
                #base_path = 'C:/Users/Damir/SkyDrive/Visual Studio Projects/test/test/2014-08-24/'
                if not os.path.exists(base_path):
                    os.makedirs(base_path)
                file_name = base_path + camp_name + '_' + date + '.csv'
                writer = csv.writer(open(file_name,'wb'))
                #write header and data
                header = ["Date","Campaign","Search KW","Referrer","Pub ID","Source ID","Count"]
                writer.writerow(header)
                writer.writerows(data)
        except:
            traceback.print_exc()
if __name__ == '__main__':
    main(sys.argv[1])
    #main('2014-08-01')
