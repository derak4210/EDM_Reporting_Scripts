import traceback
import MySQLdb as mdb

def chunk(l,n):
    if n<1:
        n=1
    return [l[i:i+n] for i in range(0, len(l),n)]

def upload_data(data):

    query = "REPLACE INTO `datablocks_ppc_center`.`source_query_terms` (`date`,`sid`,`pid`,`subid`,`query`,`count`) VALUES (%s,%s,%s,%s,%s,%s)"

    #for row in data:
    try:
        db = mdb.connect(host="localhost", user="root", db='datablocks_adcenter', port = 2000)
        cur = db.cursor()
        #check if more than 1000 rows then split up
        if len(data)>1000:
            chunked = chunk(data,1000)
            for row in chunked:
                cur.executemany(query,row)
        else:
            #No need to split
            cur.executemany(query,data)

        msg = ("EXECUTED INSERT SUCCESSFULLY")
        db.commit()
    except:
        print("ERRORRRR!!!")
        msg = traceback.print_exc()

    return msg