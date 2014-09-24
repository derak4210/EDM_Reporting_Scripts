import urllib, json, itertools,traceback,csv,sys
import MySQLdb as mdb
def download(date,stage,level,id):

    pre_url = "https://api.adcenter.excitedigitalmedia.com/v2/stats/"
    suffix_url = "?apiKey=4d28cf3ed125ed03&order=-date_from&pageSize=50000&timezone=10" #timezone
    date_params = "&date_from=" + date + "&date_to=" + '2014-07-07'#date
    data = []

    url = pre_url + level + "/" + str(id) + "/" + stage + suffix_url + date_params
    resp = json.load(urllib.urlopen(url))
    try:
        for item_row in resp['data']['items']:
            product= str(item_row['name'])
            searches = str(item_row['searches'])

    except:
        print(traceback.print_exc())

def build_id_map(date):
    query = "select adgroups_id, advertiser_id,campaigns_id from datablocks_adcenter.daily_adgroup_performance_summary where date='" + date + "'"

    db = mdb.connect(host="localhost", user="root", db='datablocks_adcenter', port = 2000)
    cur = db.cursor()
    cur.execute(query)
    ids = cur.fetchall()
    return (ids)


def main(curr_day):
    stages = [
              "products",
              "countries"
              ]

    levels = [
                "agencies",
                "users",
                #"ads",
                "adgroups",
                "campaigns"
              ]

    for stage in stages:
        ids = build_id_map(curr_day)
        for id in ids:
            #Download Advertiser level
            download(curr_day,stage, "users", id[1])
            #Download Campaign level
            download(curr_day,stage, "campaigns", id[2])
            #Download Adgroups level
            download(curr_day,stage, "adgroups", id[0])


if __name__ == '__main__':
  #main(sys.argv[1])
  main("2014-07-01")
