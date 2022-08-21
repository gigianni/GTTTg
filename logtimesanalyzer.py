import mysql.connector
import time as unixtime
from datetime import datetime, timedelta

db = mysql.connector.connect(
        host="gtttg.mysql.database.azure.com",
        user="azureuser",
        password=">{uG8^eLnZP63$A:",
        database="gtfs"
    )
cr = db.cursor(dictionary=True)
starttime = unixtime.time()
rettime = eltime = 0
totalrows = validcount = 0
routeAvgs = {}
# retrieves the average from previous measured times
if input("Use averages from weekly_timetable? [Y/n] ").lower() == 'y':
    sql = "SELECT route_id, timestamp_week, avg(time) as avg FROM weekly_timetable GROUP BY route_id, timestamp_week"
    cr.execute(sql)
    ravgs = cr.fetchall()
    for el in ravgs:
        routeAvgs[el["route_id"]+'-'+str(el["timestamp_week"])] = el["avg"]


sql = "SELECT route_id,MAX(stop_sequence) AS maxSeq FROM logtimes GROUP BY route_id;"

cr.execute(sql)
routes = cr.fetchall()
insVals = []

for route in routes:
    print(f"Route {route['route_id']}")
    unixt = unixtime.time()
    sql = "SELECT * FROM logtimes WHERE route_id=%s AND timestamp>%s ORDER BY trip_id,timestamp;"
    adr = (route["route_id"], int(starttime-(60*60*24*28)))
    cr.execute(sql,adr)
    trips = cr.fetchall()

    sql = "SELECT trip_id, count(*) as len FROM logtimes WHERE route_id=%s AND timestamp>%s  GROUP BY trip_id ORDER BY trip_id"
    adr = (route["route_id"], int(starttime-(60*60*24*28)))
    cr.execute(sql, adr)
    tripsLen = cr.fetchall()

    vals = {}
    # {
    #   key = [ stop_id, stop_name, stop_code, stop_sequence, route_id, route_short_name, trip_id, time]...
    # }
    k = 0
    rettime += unixtime.time() - unixt
    print("\tRetrieve:\t%s seconds" % (unixtime.time() - unixt))
    unixt = unixtime.time()
    for t in tripsLen:
        seq = -1
        if t["len"] < route["maxSeq"]*0.70:
            k += t["len"]
        else:
            totalrows += t["len"]
            for i in range(t["len"]):
                stop = trips[k]
                k += 1
                if seq == -1:   #primo del trip
                    wt = 1
                    time = stop['timestamp']
                    seq = stop['stop_sequence']
                elif seq == stop["stop_sequence"]:  #stesso stopseq
                    time = (stop['timestamp'] + time*wt) / (wt+1)
                    wt += 1
                else:   #nuovo stopseq
                    wt = 1
                    tm = stop['timestamp'] - time
                    tm /= stop["stop_sequence"] - seq
                    time = stop['timestamp']
                    seq = stop["stop_sequence"]

                    if tm < 35:
                        tm = 35

                    hourTs = (stop['timestamp'] // 3600) * 3600
                    dt = datetime.fromtimestamp(hourTs)
                    monday = dt - timedelta(days=dt.weekday())
                    monday = monday.replace(hour=0)
                    key = int(dt.timestamp() - monday.timestamp())

                    if route["route_id"]+'-'+str(key) in routeAvgs:
                        avg = routeAvgs[route["route_id"]+'-'+str(key)]
                    else:
                        avg = -1
                    # absolute deviation
                    ad = abs(tm - avg)
                    if (avg == -1 and tm < 5000) or (ad < avg * 0.5):
                        validcount += 1
                        num = 1
                        if key in vals and seq in vals[key]:
                            oldtm = vals[key][seq][5]
                            num = vals[key][seq][8] + 1
                            if tm > oldtm * 2: #tm sbagliato
                                tm = oldtm
                            elif not (tm * 2 < oldtm):
                                tm = round((oldtm*(num-1) + tm) / num)
                        elif key not in vals:
                            vals[key] = {}

                        vals[key][seq] = [route["route_id"], key, seq, stop["stop_id"], stop["stop_code"], tm,
                                          dt.isocalendar()[1],  dt.strftime('%Y-%m-%d %H:%M:%S'), num]

    for key, value in vals.items():
        for k, v in value.items():
            insVals.append(v[0:8])
    eltime += unixtime.time() - unixt
    print("\tElaborate:\t%s seconds" % (unixtime.time() - unixt))

cr.execute("TRUNCATE TABLE weekly_timetable")
db.commit()

sql = "INSERT INTO weekly_timetable (route_id,timestamp_week,stop_sequence,stop_id,stop_code,time,week,date)" \
          " VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"

cr.executemany(sql, insVals)
db.commit()

print(f"Elaborated {totalrows} rows in {(unixtime.time() - starttime)} seconds")
print(f"\tUsed {validcount}/{totalrows} elements, {validcount/totalrows*100} %")
print(f"\tTotal retrieve: {rettime}\n\tTotal elaborate: {eltime}")
