import csv

def process_report(account,file_name):
    #need to remove header from file before reading it

    f =  open(file_name,'rb')
    lines = f.readlines()[6:]
    f.close()

    f = open(file_name,'wb')

    for line in lines:
        f.write(line)

    f.close()

    open_file = open(file_name,'rb')
    reader = csv.DictReader(open_file)

    for row in reader:
        
        ad_content = row['Ad Content']
        #parse ad content variable
        split_ac = ad_content.split('_').replace('DB-','')
        tier = split_ac[0]
        product = split_ac[1]
        pid = split_ac[2]
        sid = split_ac[3]
        users = row['Users']
        new_users = row['New Users']
        avg_time_page = float(row['Avg. Time on Page'])
        bounce_rate = float(row['Bounce Rate'])
        pageviews = row['Pageviews']
        avgt_session_duration = float(row['Avg. Session Duration'])
        pages_session = float(row['Pages / Session'])
        bounces = row['Bounces']
