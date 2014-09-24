#!/usr/bin/python27
import urllib, csv, traceback, sys
import MySQLdb as mdb
import smtplib
from email.mime.text import MIMEText 
from email.mime.multipart import MIMEMultipart

def upload(data):
    try:
        db = mdb.connect(host="localhost", user="root", db='datablocks_adcenter', port = 2000)
        cur = db.cursor()
        query = "REPLACE INTO `datablocks_adcenter`.`publisher_sources` (`pub_id`,`source_id`,`pub_name`,`source_name`) VALUES (%s,%s,%s,%s)"
        cur.executemany(query,data)
        msg = ("EXECUTED INSERT SUCCESSFULLY")
        db.commit()
    except:
        msg = traceback.print_exc()

    return msg

def mail(report, date):

    msg = MIMEMultipart('alternative')
    sender = "damir@excitedigitalmedia.com"
    recepients = ['damir@excitedigitalmedia.com']

    if "SUCCESS" in report:
        msg['Subject'] = 'SUCCESSFUL UPDATED PUBLISHER SOURCES FOR %s' % date
    else:
        msg['Subject'] = 'UPDATING PUBLISHER SOURCES FOR %s FAILED' % date
        email_msg = report
        content = MIMEText(email_msg, 'plain')
        msg.attach(content)

    msg['From'] = sender
    msg['To'] = ", ".join(recepients)

    #Connect
    s = smtplib.SMTP()

    s.connect('mail.exciteholidays.com', '25')
    s.ehlo()


    try:
        s.sendmail(sender,recepients,msg.as_string())
    except Exception:
        traceback.print_exc()
        msg['Subject'] = 'SENDING CONFIRMATION EMAIL FOR PUBLISHER SOURCES JOB FAILED FOR SOME REASON'
        msg['To'] = 'damir@excitedigitalmedia.com'
        content = MIMEText(traceback.print_exc(), 'plain')
        msg.attach(content)
        s.sendmail('damir@excitedigitalmedia.com', 'damir@excitedigitalmedia.com', msg.as_string())
    finally:
        s.quit()

#date = '2014-06-01'
date = str(sys.argv[1])

print("Updating publisher sources for %s" %date)
url = 'http://login.db.excitedigitalmedia.com/detailedstats.php?auth=db73407306496d474232319f175bdf0bfd2ee360&start_date=' + date + '&end_date=' + date

reader = csv.reader(urllib.urlopen(url))
pub_source_map = set()

for row in reader:
    if 'date' in row:
            continue
    sourceID = row[1]
    sourceName = row[2]
    pubID = row[3]
    pubName = row[4]
    new_row = pubID,sourceID,pubName,sourceName
    pub_source_map.add(new_row)

msg = upload(pub_source_map)

mail(msg,date)
