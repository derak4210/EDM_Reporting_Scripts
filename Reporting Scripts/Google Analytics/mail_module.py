#!/usr/bin/python27
import smtplib
from email.mime.text import MIMEText 
from email.mime.multipart import MIMEMultipart

def mail(report, name):

    msg = MIMEMultipart('alternative')
    sender = "damir@excitedigitalmedia.com"
    recepients = ['damir@excitedigitalmedia.com']

    if "SUCCESS" in report:
        msg['Subject'] = 'SUCCESSFUL RUN OF DAILY GOOGLE ANALYTICS JOB FOR %s' % name
    else:
        msg['Subject'] = 'DAILY GOOGLE ANALYTICS JOB FOR %s FAILED' % name
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
        msg['Subject'] = 'SENDING CONFIRMATION EMAIL FOR DAILY GOOGLE ANALYTICS FAILED FOR SOME REASON'
        msg['To'] = 'damir@excitedigitalmedia.com'
        content = MIMEText(traceback.print_exc(), 'plain')
        msg.attach(content)
        s.sendmail('damir@excitedigitalmedia.com', 'damir@excitedigitalmedia.com', msg.as_string())
    finally:
        s.quit()
