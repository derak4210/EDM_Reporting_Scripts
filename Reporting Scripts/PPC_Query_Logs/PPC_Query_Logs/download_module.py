import json, urllib, urllib2, os,csv, traceback, string, itertools

def removeNonAscii(s): return "".join(i for i in s if ord(i)<128)

def groupdata(data):
    lmb = lambda d: d[2]
    sorted_data = sorted(data, key = lmb)

    final = []
    for k, g in itertools.groupby(sorted_data,key = lmb):
        count = 0
        for row in g:
            count = count + 1
        final_row = [row[0],row[1],row[2],count]
        final.append(final_row)

    return final
def download_parse_query_log(date,source_id):
    grouped_data = []
    url = 'http://login.db.excitedigitalmedia.com/parse_raw_data.php?auth=db73407306496d474232319f175bdf0bfd2ee360&request=search'
    date_params = '&date=' + date
    hour_param = '&hour='
    sid_param = '&sid=' + source_id
    data = []
    for x in range(0,23):
        #check if query logs exist
        file_name = '/home/ec2-user/DB_logs/Search_Logs/' + 'excitedigitalmedia_search_logs_' + source_id + '_' + date + '_' + str(x) +  '.j'
        #file_name = 'C:/Users/Damir/SkyDrive/Visual Studio Projects/PPC_Query_Log/PPC_Query_Log/excitedigitalmedia_search_logs_' + source_id + '_' + date + '_' + str(x) +  '.j'
        if not os.path.exists(file_name):
            full_url = url + date_params + sid_param + hour_param + str(x)
            command = 'curl \'' + full_url + '\' --compressed > ' + file_name
            print("Command to be executed:\n%s" % command)
            os.system(command)
        
        #parse query log
        open_file = open(file_name,'rb')

        #json_data = json.loads(open_file.read())

        #json_dump = json.dumps(open_file.read())
        ##json_file = csv.reader(open_file)
        #json_file = json.loads(json_dump)
        for row in open_file:
            print("ROW: %s" %row)
            try:
                json_row = json.loads(row,encoding='utf-8')
                pid = str(json_row['pid'])
                kw = str(json_row['keyword'])
                new_row = [source_id,pid,kw]
                data.append(new_row)
            except UnicodeDecodeError:
                json_row = json.loads(row,encoding='latin-1')
                pid = str(json_row['pid'])
                kw = str(json_row['keyword'])
                new_row = [source_id,pid,kw]
                data.append(new_row)
            except ValueError:
                try:
                    row = filter(string.printable.__contains__, row)
                    #row = strip_control_characters(row)
                    json_row = json.loads(row,encoding='latin-1')
                    pid = str(json_row['pid'])
                    kw = str(json_row['keyword'])
                    new_row = [source_id,pid,kw]
                    data.append(new_row)
                except:
                    try:
                        row = removeNonAscii(row).replace("X\"MT","").replace("X\"","")
                        json_row = json.loads(row,encoding='latin-1')
                        pid = str(json_row['pid'])
                        kw = str(json_row['keyword'])
                        new_row = [source_id,pid,kw]
                        data.append(new_row)
                    except:
                        traceback.print_exc()
                        continue
    grouped_data = groupdata(data)

    #write report
    file = open("Adlux_Search_Log_" + source_id + "_" + date + ".csv","wb")
    wr = csv.writer(file)
    header = ["Source ID", "Publisher ID", "Query Term", "Count"]
    wr.writerow(header)

    wr.writerows(grouped_data)
