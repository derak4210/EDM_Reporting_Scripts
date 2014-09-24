#!/usr/bin/python27
import urllib, urllib2, os, csv, itertools, json, types, smtplib, traceback, sys
from download_module import *
from email.mime.text import MIMEText 
from email.mime.multipart import MIMEMultipart
from datetime import datetime, timedelta
from db_upload import upload_conversions
import MySQLdb as mdb

#curr_day = '2014-06-23'
today = datetime.strptime(str(sys.argv[1]),"%Y-%m-%d")
tomorrow = (today + timedelta(days=1)).strftime("%Y-%m-%d")

#raw_input(curr_day)

accounts = {
            "73" : "WebJet",
            "25" : "Mazda" ,
            "168" : "Insuranceline",
            "137" : "Domain",
            "99" : "Yellow Advertiser",
	        "173" : "Living Social",
            "162" : "Reply! - iMotors"
            }


conversions = download_conversion_report(tomorrow)

#upload to database

upload_conversions(tomorrow,conversions)

#create and send report
 
try:
    db = mdb.connect(host="localhost", user="root", db='datablocks_adcenter', port=2000)
    cur = db.cursor()
    query = """ select date,adv as "Advertiser",
                camp_name as "Campaign Name",cd.adgroup as AdGroup, 
                cd.ad_name as Ad, count(*) as Conversion,product as Product,tier as Tier,
                (case when product like '%SEM%' then "DB" else "Network" end) as "Traffic Origin", 
                ps.pub_name as Publisher,ps.pub_id as "Publisher ID", 
                ps.source_name as Source,ps.source_id as "Source ID", 
                SUBSTRING_INDEX(cd.source_id,'_',-1) as Sub_ID, 
                cd.source_id as "{Source_ID}",
                cd.search_time as "Search Time",cd.click_time as "Click Time",
                cd.kw as Keyword,cd.ref_url as "Referrer", cd.click_url as "Click_Url"
                from datablocks_adcenter.conversion_details_v2 cd
                left join datablocks_adcenter.publisher_sources ps on (ps.pub_id=substring_index(cd.source_id,'_',1) and ps.source_id=SUBSTRING_index(substring_index(cd.source_id,'_',2),'_',-1))
                where cd.date>='""" + tomorrow + """' and type='Last_Click'
                group by date,adv,camp_name,cd.adgroup,cd.ad_name,product,tier,
                (case when product like '%SEM%' then "DB" else "Network" end),
                ps.pub_name,ps.pub_id, 
                ps.source_name,cd.source_id, 
                SUBSTRING_INDEX(cd.source_id,'_',-1), 
                cd.search_time,cd.click_time,
                cd.kw,cd.ref_url,cd.click_url;"""

    cur.execute(query)
    data = cur.fetchall()
except:
    print("Error fetching conversions")
    error = traceback.format_exc()

try:
    summary_query = """
                    select 
                    ifnull(adv,'Total') as Advertiser,count(*) as Conversions from datablocks_adcenter.conversion_details_v2 
                    where date = '""" + tomorrow + """' and type = 'Last_Click'
                    group by adv WITH ROLLUP;

                    """
    cur.execute(summary_query)
    summary = cur.fetchall()
except:
    print("Error fetching conversions")
    error = traceback.format_exc()
#raw_input(len(summary))
#raw_input(len(data))
try:
    if (summary and data):
        subject = "Today's Conversion Report - " + tomorrow
        email_msg = subject + " <br> <table border = \"1\"> "
        recepients = ['damir@adlux.com','lara@adlux.com', 'evan@adlux.com', 'kulsuma@adlux.com', 'jeron@adlux.com', 'tom.g@excitedigitalmedia.com', 'maree@adlux.com' ]
        #format body of email with top level summary
        header = 0
        for row in summary:
            print row
            if header == 0:
                email_msg = email_msg + "<tr><td>Advertiser</td><td>Conversions</td></tr>"
                header = 1
            email_msg = email_msg + '<tr>'
            for element in row:
                print(element)
                email_msg = email_msg + '<td>' + str(element) + '</td>'
            email_msg = email_msg + '</tr>'
        email_msg = email_msg + " </table> "

        #mail message
        msg = MIMEMultipart('alternative')
        sender = "damir@excitedigitalmedia.com"
        msg['Subject'] = subject
        msg['To'] =  ", ".join(recepients)
        msg['From'] = sender

        s = smtplib.SMTP()
        s.connect('mail.exciteholidays.com','25')
        s.ehlo()

        #write conversions to csv and attach
        try:
            file_name = "/home/ec2-user/Reports/Daily Conversion Report/Daily_Conversion_Report_" + tomorrow + ".csv"
            #file_name = "Daily_Conversion_Report_" + curr_day + ".csv"
            out_file = open(file_name,"wb")
            writer = csv.writer(out_file)
            header = ['date',	'Advertiser',	'Campaign Name',	'AdGroup',	'Ad',	'Conversion',	'Product',	'Tier',	'Traffic Origin',	'Publisher',	'Publisher ID',	'Source',	'Source ID',	'Sub_ID',	'{Source_ID}',	'Search Time',	'Click Time',	'Keyword',	'Referrer',	'Click_Url' ]
            writer.writerow(header)
            for row in data:
                print(row)
                date = tomorrow #curr_day
                adv = str(row[1])
                camp_name = str(row[2])
                adgroup = str(row[3])
                ad = str(row[4])
                conversion = str(row[5])
                product = str(row[6])
                tier = str(row[7])
                traffic_origin = str(row[8])
                pub = str(row[9])
                pub_id = str(row[10])
                source = str(row[11])
                source_id = str(row[12])
                sub_id = str(row[13])
                source_source_id = str(row[14])
                search_time = str(row[15])
                click_time = str(row[16])
                keyword = str(row[17])
                ref = str(row[18])
                click_url = str(row[19])
                write_row = [date ,	adv ,	camp_name ,	adgroup ,	ad ,	conversion ,	product ,	tier ,	traffic_origin ,	pub ,	pub_id ,	source ,	source_id ,	sub_id ,	source_source_id ,	search_time ,	click_time ,	keyword ,	ref ,	click_url ]
                writer.writerow(write_row)
        except:
            traceback.print_exc()
        
    
        #close file
        out_file.close()

        f = file(file_name)
        attachment = MIMEText(f.read())
        attachment.add_header('Content-Disposition', 'attachment', filename = "Daily_Conversion_Report_%s.csv" %tomorrow )
        msg.attach(attachment)


        #attach body
        content = MIMEText(email_msg,'HTML')
        msg.attach(content)

        try:
            s.sendmail('reporter@excitedigitalmedia.com',recepients,msg.as_string())
        except:
            traceback.print_exc()
        finally:
            s.quit()
except:
    try:
        s.sendmail('reporter@excitedigitalmedia.com','damir@excitedigitalmedia.com',traceback.print_exc())
    except:
        traceback.print_exc()
    finally:
        s.quit()
    
#delete current day report
print("About to remove this file %s" %file_name)
os.remove(file_name)
