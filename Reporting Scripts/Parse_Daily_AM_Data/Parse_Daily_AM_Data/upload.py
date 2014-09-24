import MySQLdb as mdb
import traceback, sys
from datetime import time, datetime

def chunk(l,n):
    if n<1:
        n=1
    return [l[i:i+n] for i in range(0, len(l),n)]

def upload(upload_rows,table):
    try:
        
        if "adconnect" in table:
            db = mdb.connect(host="localhost", user="root", db='nami_adconnect_backup')
            cur = db.cursor()
            query = "REPLACE INTO `nami_adconnect_backup`.`" + table + """`(`DATE`,`PUB_NAME`,`SOURCE_NAME`,`BUDGET DEPLETED / MISCONFIGURATION`,`DAYPART BLOCK`,`CPIP FILTER`,`GEO FILTER`,`IP BLOCK`,`KEYWORD BLOCK`,`REFERER BLOCK`,`MISSING UA/REF`,`IE USER AGENT FILTER`,`CHROME USER AGENT FILTER`,`FIREFOX USER AGENT FILTER`,`SAFARI USER AGENT FILTER`,`OTHER USER AGENT FILTER`,`MAXQUERY THROTTLE`,`FEED FILTERS/TOLERANCES`,`FEED CALLS`,`TIMEOUTS`,`VALID FEED CALLS`,`CALLS PER QUERY`,`FEED COVERAGE`,`VALID FEED COVERAGE`,`PUBLISHER REQUEST RATIO`,`AD RETURN RATIO`,`AD AVAILABLE RATIO`,`CPC CLIPPED`,`EXCESS ADS`,`PUBLISHER USED LISTINGS`,`PUBLISHER COVERAGE`,`AVG PUBLISHER LISTINGS`,`AVG USED LISTINGS`,`GROSS CLICKS`,
                    `INCOMPLETE URL`,`CLICK TIME FILTER`,`INTERASP FILTER`,`IP MISMATCH`,`UA MISMATCH`,`REFERER MISMATCH`,`CPL FILTER`,`CPQ FILTER`,`CPIP FILTER 2`,`CLICK CAP FILTER`,`COOKIE FILTERED`,`DOMAIN FILTER`,`BACKBUTTON FILTER`,`NO FLASH FILTER`,`OFFSCREEN FILTERED`,`ADVANCED JS FILTERED`,`IFRAME FILTERED`,`TOTAL FILTERED CLICKS`,`ROLLOVER REDIRECT DROPS`,`COOKIE REDIRECT DROPS`,`REFPAGE REDIRECT DROPS`,`JS REDIRECT DROPS`,`TOTAL DROPPED`,`SYSTEM ERRORS`,`TOTAL ERRORS AND DROPS`,`OFFSCREEN`,`ONSCREEN`,`NOSCREEN`,`ROLLOVER CLICKS`,`NON-ROLLOVER CLICKS`,`COOKIE PASS`,`COOKIE FAIL`,`IFRAME PASS`,`IFRAME FAIL`,`ADVANCED JS PASS`,`NATURALPLUS COMPLETE`,`REF ASSIGN JS`,
                    `NATURAL JS`,`COMPLETED REFPAGE`,`FINAL REDIRECT`,`MAX JS NEG PLUS`,`ESTIMATED NET CLICKS`,`GOOD CLICK RATIO`,`ESTIMATED GROSS REVENUE`,`AVG ESTIMATED GROSS CPC`,`ASP ESTIMATED REVENUE`,`ESTIMATED PUBLISHER REVENUE`,`AVG ESTIMATED PUBLISHER CPC`,`ESTIMATED PUBLISHER ECPM`,`VALID NET CLICKS`,`VALID CLICKS PER PUBLISHER USED LISTINGS`,`VALID CLICK RATIO`,`VALID GROSS REVENUE`,`VALID ASP NET REVENUE`,`VALID PUBLISHER NET REVENUE`,`VALID REVENUE RATIO`,`VALID AVG GROSS CPC`,`VALID AVG PUBLISHER CPC`,`VALID PUBLISHER ECPM`) VALUES (%s,
                    %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
        elif "advertiser" in table:
            db = mdb.connect(host="localhost", user="root", db='nami_admanager_backup', port=2000)
            cur = db.cursor()
            query = "REPLACE INTO `nami_admanager_backup`.`" + table + "`(`Date`,`Advertiser`,`Campaign`,`Adgroup`,`Ad`,`Matches`,`Impressions`,`Clicks`,`Conversions`,`Spend`,`Cost`,`CPC`,`Matches_CTR`,`Impressions_CTR`) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"

        if len(upload_rows)>1000:
            chunked = chunk(upload_rows,1000)
            for id,row in enumerate(chunked):
                cur.executemany(query,row)
                db.commit()
                print("%s - SUCCESSFULLY INSERTED %s OUT OF %s ROWS INTO DATABASE"%(datetime.now(),len(row),len(upload_rows)))
        else:
            cur.executemany(query,upload_rows)
            db.commit()

        print("%s - SUCCESSFULLY INSERTED %s ROWS INTO DATABASE"%(datetime.now(),len(upload_rows)))

    except:
        print(traceback.print_exc())

