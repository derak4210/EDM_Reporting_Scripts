import os, csv, itertools, traceback

basepath = 'C:/cygwin64/home/Damir/ProjectRescue/AdConnect/Feeds'

feeds = os.listdir(basepath)
writer = csv.writer(open('C:/cygwin64/home/Damir/Feed_Source_Daily.csv','wb'))
header = 0
for fd in feeds:
    try:
        files = os.listdir(basepath + fd +'/')
        for file in files:
            if "Source_Daily" in file:
                    reader = csv.reader(open(basepath + fd + '/' + file,'rb'))
                    for row in reader:
                        if ('DATE' in row and header == 0):
                            header = 1
                            print("Writing Header")
                            writer.writerow(row)
                        elif ('DATE' in row and header == 1):
                            continue
                        elif ('Oversee' in row[0]):                
                            print("Writing row")
                            writer.writerow(row)
    except:
        print("ERROR")
        traceback.print_exc()