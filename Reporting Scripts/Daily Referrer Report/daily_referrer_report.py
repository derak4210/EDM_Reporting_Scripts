#!/usr/bin/python27
import csv, urllib, operator, itertools, smtplib, sys, traceback
from email.mime.text import MIMEText 
from email.mime.multipart import MIMEMultipart
from datetime import datetime, timedelta

def mail(report,message, type):

    if type == "normal":
        msg = MIMEMultipart('alternative')
        sender = "reporter@excitedigitalmedia.com"
        recepients = ['damir@excitedigitalmedia.com', 'evan@excitedigitalmedia.com', 'kulsuma@excitedigitalmedia.com' ]

        msg['Subject'] = 'DAILY REFERRER REPORT FOR %s' % curr_day
        msg['From'] = sender
        msg['To'] = ", ".join(recepients)


        #attach report
        f = file(report)
        attachment = MIMEText(f.read())
        attachment.add_header('Content-Disposition', 'attachment', filename = "Daily_Referrer_Report_%s.csv" %curr_day )
        msg.attach(attachment)
        #Connect
        s = smtplib.SMTP()

        s.connect('mail.exciteholidays.com', '25')
        s.ehlo()

        email_msg = '<table border="1"> <thead> <tr> <th>PubID</th><th>PubName</th><th>Ref</th><th>count</th></tr></thead><tbody>'
        for line in message:
            email_msg = email_msg + '<tr><td>' + str(line[0]) + '</td><td>' + str(line[1]) + '</td><td>' + str(line[2]) + '</td><td>' + str(line[3]) + '</td></tr>'

        email_msg = email_msg + '</tbody></table>'

        content = MIMEText(email_msg, 'html')
        msg.attach(content)
        try:
            s.sendmail(sender,recepients,msg.as_string())
        except Exception:
            traceback.print_exc()
            msg['Subject'] = 'REFERRER REPORT FAILED'
            msg['To'] = 'damir@excitedigitalmedia.com'
            content = MIMEText(traceback.print_exc(), 'plain')
            msg.attach(content)
            s.sendmail('reporter@excitedigitalmedia.com', 'damir@excitedigitalmedia.com', msg.as_string())
        finally:
            s.quit()
    elif type == "error":
        msg = MIMEMultipart('alternative')
	msg['Subject'] = 'REFERRER REPORT FAILED'
        msg['To'] = 'damir@excitedigitalmedia.com'
        content = MIMEText(message, 'plain')
        msg.attach(content)
        s.sendmail('damir@excitedigitalmedia.com', 'damir@excitedigitalmedia.com', msg.as_string())




def download_ref_report(curr_day,pubID):
    
    url = 'http://login.db.excitedigitalmedia.com/referrers.php?auth=db73407306496d474232319f175bdf0bfd2ee360&date=' + curr_day +'&id=' + pubID

    resp = urllib.urlopen(url)

    reader = csv.DictReader(resp)
    data = []
    for row in reader:
        try:

            dict_row = {}
            dict_row['date'] = row['date']
            dict_row['pid'] = row['pid']
            dict_row['referrer'] = row['referrer']
            dict_row['count'] = row['count']
            data.append(dict_row)
        except:
            #retry again
            global retry
            retry = retry + 1
            if retry > 10:
                error_msg = "Exception occurred for publisher %s - %s. \n This is the error: %s" % (pubID,url, traceback.print_exc())
                print(error_msg)
                mail("n/a",error_msg, "error")
            else:
                download_ref_report(curr_day,pubID)
    #group data by pub
    lmb = lambda d: d["referrer"]

    sorted_reader = sorted(data, key = lmb)
    final_data = []
    for k, g in itertools.groupby(sorted_reader, key = lmb):
        count = 0
        for data in g:
            try:
                count = count + int(data['count'])
            except:
                continue
        final_row = [data['pid'],data['referrer'],count]
        final_data.append(final_row)

    #sort data
    final_data = sorted(final_data,key = lambda i: i[2], reverse=True)
    return final_data



def map_pubs(curr_day):
    #create a 3 day buffer around the day we are running for since this data is in EST and our conversion data varies depending on advertiser
    
    url = 'http://login.db.excitedigitalmedia.com/detailedstats.php?auth=db73407306496d474232319f175bdf0bfd2ee360&start_date=' + curr_day + '&end_date=' + curr_day

    resp = urllib.urlopen(url)

    reader = csv.reader(resp)

    #initialize dictionary
    sources = set()
    for row in reader:
        if 'date' in row:
            continue
        pubID = row[3]
        pubName = row[4]
        new_row = pubID,pubName
        sources.add(new_row)

    return (sources)

#curr_day = '2014-06-01'
today = datetime.strptime(str(sys.argv[1]),"%Y-%m-%d")
curr_day = (today - timedelta(days=1)).strftime("%Y-%m-%d")

#raw_input(curr_day)

retry = 0
pubs = map_pubs(curr_day)
#report = 'C:/Users/Damir/Desktop/Daily_Referrer_Report_' + curr_day + '.csv'
report = '/home/ec2-user/Reports/Daily Referrer Report/Daily_Referrer_Report_' + curr_day + '.csv'

open_file = open(report, 'wb')

writer = csv.writer(open_file)
header = ["Publisher ID","Publisher Name","Referrer","Count"]
writer.writerow(header)

leader_board = []

for pubId, pubName in pubs:
    leader_flag = 0
    #skip Excite publisher
    if pubId == '55':
        continue
    data = download_ref_report(curr_day, pubId)

    for row in data:
        write_row = [row[0],pubName,row[1],row[2]]
        if leader_flag == 0:
            leader_board.append(write_row)
            leader_flag = 1
        writer.writerow(write_row)

open_file.close()

#Email report out
mail(report,leader_board, "normal")
 
