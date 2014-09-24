import MySQLdb as mdb
import traceback, sys, smtplib,csv, types, os
import ConfigParser

from email.mime.text import MIMEText 
from email.mime.multipart import MIMEMultipart

def run_query(query):
    try:
        data = []
        db = mdb.connect(host="localhost", user="root", db='datablocks_adcenter', port = 2000)
        cur = db.cursor()
        cur.execute(query)
        #grab column names of query
        col_names = [i[0] for i in cur.description]
        data.append(col_names)
        data.append(cur.fetchall())
        msg = ("EXECUTED QUERY SUCCESSFULLY")
        return ('success',data)
    except:
        msg = traceback.print_exc()
        return ('failed',msg)

def mail(recepients,data,subject):
    

    msg = MIMEMultipart('alternative')
    sender = "damir@excitedigitalmedia.com"
    msg['Subject'] = subject
    msg['To'] = ", ".join(recepients.split(","))
    msg['From'] = sender

    s = smtplib.SMTP()
    s.connect('mail.exciteholidays.com','25')
    s.ehlo()

    if 'ERROR' in subject:
        #something went wrong send regular error message
        content = MIMEText(data,'plain')
        msg.attach(content)
        try:
            s.sendmail(msg['From'],msg['To'],msg.as_string())
        except:
            traceback.print_exc()
        finally:
            s.quit()
    else:
        #create a CSV to attach
        of = open("temp.csv",'wb')
        writer = csv.writer(of)

        email_msg = subject + "<br>Top 100 Rows: <br> <table border = \"1\"> "
        header = 0
        row_count = 0
        for row in data:
            if header == 0:
                email_msg = email_msg + " <tr> "
                header_csv_row = row
                writer.writerow(header_csv_row)
            for element in row:
                if isinstance(element,types.StringTypes):
                    email_msg = email_msg + " <th align=\"left\"> " + element + " </th> "
                    header = 1
                else:
                    if row_count<100:
                        email_msg = email_msg + " <tr> "
                    csv_row = []
                    for el in element:
                        if row_count<100:
                            email_msg = email_msg + " <td> " + str(el) + " </td> "
                        csv_row.append(str(el))
                        
                    if row_count<100:		    
                        email_msg = email_msg + " </tr> "
                        row_count = row_count + 1
                    writer.writerow(csv_row)
            
            if header == 0:
                    email_msg = email_msg + " </tr> "
        email_msg = email_msg + " </table> "

        #close CSV and attach both it and the body table
        of.close()
        f = file("temp.csv")
        attachment = MIMEText(f.read())
        attachment.add_header('Content-Disposition', 'attachment', filename = subject + '.csv' )
        msg.attach(attachment)

        #attach body
        content = MIMEText(email_msg,'HTML')
        msg.attach(content)

        try:
            s.sendmail(msg['From'],recepients.split(','),msg.as_string())
        except:
            traceback.print_exc()
        finally:
            s.quit()

        #delete the file
        print("Removing temporary file")
        os.remove("temp.csv")



def main(file):
    print("SQL_Reporter initialized for SQL Query %s" %file)
    file_path = "configs/" + file
    #parse out various configurations from file
    Config = ConfigParser.ConfigParser()
    Config.read(file_path)

    try:
        subject = Config.get('REPORT CONFIG','SUBJECT')
        recepients = Config.get('REPORT CONFIG','RECEPIENTS')
        query = Config.get('REPORT SQL','QUERY')
        
        try:
            data = run_query(query)
            if 'success' in data:
                mail(recepients,data[1],subject)
            else:
                mail('damir@excitedigitalmedia.com',data[1],'ERROR RUNNING SQL QUERY')
        except:
            traceback.print_exc()

    except:
        error = traceback.format_exc()  
        mail('damir@excitedigitalmedia.com',error,'ERROR READING CONFIG FILE ' + file)
    
if __name__ == '__main__':
  main(sys.argv[1])
  #main("")
