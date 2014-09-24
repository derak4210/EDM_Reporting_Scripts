import os, csv, itertools

basepath = 'C:/cygwin64/home/Damir/PubSources/'

publishers = os.listdir(basepath)
writer = csv.writer(open('C:/cygwin64/home/Damir/Publisher_Source_Daily.csv','wb'))
for pub in publishers:
    dailyfiles = os.listdir(basepath + pub + '/Data/')
    for file in dailyfiles:
        reader = csv.reader(open(basepath + adv + '/Data/' + file,'rb'))
        for row in reader:
            if ('Advertiser' in row and header == 0):
                new_row = ['Date']
                header = 1
            elif ('Advertiser' in row and header == 1):
                continue
            else:
                date = file[-14:-4]
                new_row = [date]
            for element in row:
                if 'Unknown Advertiser' in element:
                    element = adv
                new_row.append(element)
            writer.writerow(new_row)