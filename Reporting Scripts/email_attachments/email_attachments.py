import imaplib,email,os,sys
import datetime


def main(datum):
    yest = datetime.datetime.strptime(datum,'%d-%b-%Y') - datetime.timedelta(days=2)
    print "Downloading Emails for %s" %yest.strftime('%Y-%m-%d')
    #save_dir = 'C:/Users/Damir/SkyDrive/Visual Studio Projects/email_attachments/email_attachments/' + yest.strftime('%Y-%m-%d') + '/'
    save_dir = '/home/ec2-user/Email_Reports/'+ yest.strftime('%Y-%m-%d') + '/'
    if not os.path.exists(save_dir):
        os.makedirs(save_dir)

    m = imaplib.IMAP4_SSL('mail.exciteholidays.com')
    m.login('damir','DEls2012')
    m.select('Inbox/GA')

    resp, items = m.search(None, '(SENTON "' + datum + '")', 'SUBJECT', "Google Analytics:")
    items = items[0].split()

    for emailid in items:
        resp, data = m.fetch(emailid, "(RFC822)") # fetching the mail, "`(RFC822)`" means "get the whole stuff", but you can ask for headers only, etc
        email_body = data[0][1] # getting the mail content
        mail = email.message_from_string(email_body) # parsing the mail content to get a mail object

        #Check if any attachments at all
        if mail.get_content_maintype() != 'multipart':
            continue

        print "From:"+mail["From"]+" - Subject:" + mail["Subject"]

        # we use walk to create a generator so we can iterate on the parts and forget about the recursive headach
        for part in mail.walk():
            # multipart are just containers, so we skip them
            if part.get_content_maintype() == 'multipart':
                continue

            # is this part an attachment ?
            if part.get('Content-Disposition') is None:
                continue

            filename = part.get_filename()
            filename = filename.replace('\r\n','')

            att_path = os.path.join(save_dir, filename)

            if os.path.exists(att_path):
                print "File Exists - skipping"
                continue
            #Check if its already there
            if not os.path.isfile(att_path) :
                # finally write the stuff
                print "Saving file: %s" %att_path
                fp = open(att_path, 'wb')
                fp.write(part.get_payload(decode=True))
                fp.close()


if __name__ == '__main__':
  main(sys.argv[1])
  #main("")