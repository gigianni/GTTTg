# TO-DO: refactor code, save trips when they are not updated, if getTimetable() keep crashing if executed with getRT
# iterate a timer of 5 secs if runningRT is ON, transform test_table in realtime delta of ttimes for traffic
# sql is the cause of the crashes, maybe safe on file then write every hour

import pandas as pd
import gtfs_kit as gk
from google.transit import gtfs_realtime_pb2
import mysql.connector
import requests
import threading
import datetime
import time
import matplotlib.pyplot as plt

global db, cr, logger, test, stopTdict, stopsRT, routesRT, ttable, runningRT, runningGetTT, logtimesVals, testtableVals
global stops, routes, trips, stop_times


def getDatetimeNowStr():
    """
        Return current datetime as a string in the
        "%d/%m/%y %H:%M:%S" format.
    """
    return datetime.datetime.now().strftime("%d/%m/%y %H:%M:%S")


def getGTFS():
    """
        Retrieve GTFS data from "https://www.gtt.to.it/open_data/gtt_gtfs.zip"
        and populates the following global variables
         ``stops``, ``routes``, ``trips``, ``stop_times``
         indexed by ``stop_id``, ``route_id``, ``trip_id``, (``trip_id``, ``stop_sequence``)
    """
    t = time.time()
    p = 'https://www.gtt.to.it/open_data/gtt_gtfs.zip'

    try:
        feed = (gk.read_feed(p, dist_units='km'))
    except Exception as err:
        logger(f'{getDatetimeNowStr()} <b>getGTFS()<\b>\ngk.read_feed() raised: {repr(err)},'
               f'\n calling getGTFS()')
        getGTFS()
        return 0

    print("--- retrieveGTFS: %s seconds ---" % (time.time() - t))
    t = time.time()

    global stops, routes, trips, stop_times, stopTdict
    stopTdict = {}

    df = feed.get_stops()
    stops = df.set_index(['stop_id'])

    df = feed.get_routes()
    routes = df.set_index(['route_id'])

    df = feed.get_trips()
    trips = df.set_index(['trip_id'])

    df = feed.get_stop_times()
    stop_times = df.set_index(['trip_id','stop_sequence'])
    print("--- getGTFS: %s seconds ---" % (time.time() - t))


def getStopbyId(stopId):
    """
    :param stopId: string
    :return: pandas.Series, columns: 'stop_id', 'stop_code', 'stop_name', 'stop_desc',
        'stop_lat', 'stop_lon', 'zone_id', 'stop_url', 'location_type', 'parent_station',
        'stop_timezone', 'wheelchair_boarding'
    """
    try:
        r = stops.loc[stopId]
        return r
    except KeyError as err:
        logger(f'{getDatetimeNowStr()} <b>getStopbyId({str(stopId)})<\b>\nlen is 0,\ncalling getGTFS()')
        getGTFS()
        return getStopbyId(stopId)


def getStopByTripSequence(tripId, stopSequence, trial=0):
    """
    :param tripId: str
    :param stopSequence: int
    :param trial: [Optional] int 0 or 1, if 1 getGTFS will called on KeyError
    :return: if found a dict: {'stop_id', 'stop_name', 'stop_code'}, if not found None
    """
    global stopTdict
    stopSequenceStr = str(stopSequence)
    if tripId + '-' + stopSequenceStr in stopTdict:
        return stopTdict[tripId + '-' + stopSequenceStr]
    else:
        try:
            stopId = stop_times.at[(tripId, stopSequence), 'stop_id']
        except KeyError as err:
            if trial == 0:
                logger(
                    f'{getDatetimeNowStr()} <b>getStopTimeByTripSequence({str(tripId)},{stopSequenceStr})<\b>\nlen is 0,\ncalling getGTFS()')
                getGTFS()
                return getStopByTripSequence(tripId, stopSequence)
            else:
                stopTdict[tripId + '-' + stopSequenceStr] = None
                return None

        s = getStopbyId(stopId)
        stop = {
            "stop_id": stopId,
            "stop_name": s["stop_name"],
            "stop_code": s["stop_code"]
        }
        stopTdict[tripId + '-' + stopSequenceStr] = stop
        return stop


def getRouteIdbyTrip(tripId):
    """
    :param tripId: str
    :return: RouteId as a str
    """
    try:
        r = trips.loc[tripId, ['route_id', 'direction_id']]
        return r
    except KeyError as err:
        logger(
            f'{getDatetimeNowStr()} <b>getRouteIdbyTrip({tripId})<\b>\ntrips.at[] raised KeyError: {repr(err)},\ncalling getGTFS()')
        getGTFS()
        return getRouteIdbyTrip(tripId)


def getRouteNamebyId(routeId):
    """
    :param routeId: str
    :return: route_short_name as a str
    """
    try:
        r = routes.at[routeId, 'route_short_name']
        return r
    except KeyError as err:
        logger(
            f'{getDatetimeNowStr()} <b>getRouteNamebyId({routeId})<\b>\nroutes.at[] raised KeyError: {repr(err)},\ncalling getGTFS()')
        getGTFS()
        return getRouteNamebyId(routeId)


def getDBconnection():
    global db,cr
    #f = open("mysql.txt", "r")
    #d = f.read().splitlines()
    #f.close()

    db = mysql.connector.connect(
        host='gtttg.mysql.database.azure.com',
        user='azureuser',
        password='>{uG8^eLnZP63$A:',
        database="gtfs"
    )
    cr = db.cursor(dictionary=True)



def getTtimes(r):
    # r: {"stop_id", "stop_name", "stop_code", "stop_sequence", "route_id", "direction", "route_short_name", "trip_id", "timestamp"}
    # ttable[route_id][stop_seq] = ["stop_id", "stop_code", "tm"]
    ret = []
    seq = r["stop_sequence"] + 1
    tm = r["timestamp"]
    br = 0
    cont = 0
    cont2 = 0

    if r["route_id"] in ttable:
        while br == 0 and cont < 5:
            cont2 += 1
            if seq in ttable[r["route_id"]]:
                stop = getStopByTripSequence(r['trip_id'], seq, 1)

                if stop is not None:
                    cont = 0
                    ttablerow = ttable[r["route_id"]][seq]
                    tm += ttablerow[2]
                    ret.append({"stop_id": stop["stop_id"], "stop_name": stop['stop_name'],
                                "stop_code": stop["stop_code"], "stop_sequence": seq, "route_id": r['route_id'],
                                "direction": r["direction"], "route_short_name": r['route_short_name'],
                                "trip_id": r['trip_id'], "timestamp": tm})
                else:
                    #end of route
                    br = 1
            else:
                cont += 1
            seq += 1

    return ret


def RTmanager(i):
    if runningRT or runningGetTT:
        if i != 0 and i % 3 == 0:
            logger(f'{getDatetimeNowStr()} <b>RTmanager()<\b>\ngetRT didn\'t run for: {i*5+15} seconds')
        threading.Timer(5, RTmanager, [i + 1]).start()
    else:
        getRT()


def getRT():
    global test, runningRT, stopsRT, routesRT, logtimesVals, testtableVals, posRT
    runningRT = 1
    #threading.Timer(15, RTmanager, [0]).start()
    t = time.time()
    rt = gtfs_realtime_pb2.FeedMessage()
    try:
        response = requests.get('http://percorsieorari.gtt.to.it/das_gtfsrt/trip_update.aspx')
        rt.ParseFromString(response.content)
    except ConnectionError as err:
        logger(f'{getDatetimeNowStr()} <b>getRT()<\b>\nrequest.get(), rt.ParseFromString(response.content) raised ConnectionError: {repr(err)},\naborting')
        return 0
    print(getDatetimeNowStr())
    print(f"--- retrieveRT ({len(rt.entity)} items):\t{time.time() - t} seconds\t---")
    t = time.time()

    #For better performance I chose to create a list then converting it in a dataframe
    # https://stackoverflow.com/a/47979665

    rows = []
    vals = []
    testvals = []
    tr = {}

    for entity in rt.entity:
        if entity.HasField('trip_update'):
            routeId, direction = getRouteIdbyTrip(entity.trip_update.trip.trip_id)
            routeShortName = getRouteNamebyId(routeId)
            print(trips.loc[entity.trip_update.trip.trip_id])
            input("")
            for stopt in entity.trip_update.stop_time_update:
                stop = getStopByTripSequence(entity.trip_update.trip.trip_id, stopt.stop_sequence)

                if float(stopt.departure.time) >= datetime.datetime.timestamp(datetime.datetime.now()):
                    if entity.trip_update.trip.trip_id not in tr:
                        r = {"stop_id": stop["stop_id"], "stop_name": stop['stop_name'],
                            "stop_code": stop['stop_code'], "stop_sequence": stopt.stop_sequence, "route_id": routeId,
                            "direction": direction, "route_short_name": routeShortName,
                            "trip_id": entity.trip_update.trip.trip_id, "timestamp": stopt.departure.time}
                        rows.append(r)
                        tr[entity.trip_update.trip.trip_id] = 1
                        r = getTtimes(r)
                        rows.extend(r)

                        if len(r) > 0:
                            index = len(r)//2
                            test[entity.trip_update.trip.trip_id+'_'+r[index]["stop_id"]] = [t, r[index]["timestamp"],stopt.stop_sequence]
                else:
                    if entity.trip_update.trip.trip_id in tr:
                        del tr[entity.trip_update.trip.trip_id]
                    stop_trip_ts_id = stop['stop_id']+'_'+entity.trip_update.trip.trip_id+'_'+str(stopt.departure.time)
                    if stop_trip_ts_id not in logtimesVals["set"]:
                        logtimesVals["set"].add(stop_trip_ts_id)
                        vals.append((stop['stop_id'], stop['stop_name'], stop['stop_code'], stopt.stop_sequence,
                                 routeId, routeShortName, entity.trip_update.trip.trip_id, stopt.departure.time, stop_trip_ts_id))
                    if entity.trip_update.trip.trip_id+'_'+stop['stop_id'] in test:
                        testvals.append((stop['stop_id'],stop['stop_code'],stopt.stop_sequence,
                                         routeId,entity.trip_update.trip.trip_id, stopt.departure.time,
                                         test[entity.trip_update.trip.trip_id+'_'+stop['stop_id']][1],test[entity.trip_update.trip.trip_id+'_'+stop['stop_id']][1]-stopt.departure.time,
                                         entity.trip_update.trip.trip_id+'_'+stop['stop_id'],test[entity.trip_update.trip.trip_id+'_'+stop['stop_id']][2],
                                         test[entity.trip_update.trip.trip_id+'_'+stop['stop_id']][0]))
                        del test[entity.trip_update.trip.trip_id+'_'+stop['stop_id']]

    # stopsRT is assigned only now, getStopRT calls from telegram can be executed simultaneously with getRT using
    # the old data
    if len(rows) > 0:
        stopsRT = pd.DataFrame(rows).set_index(['stop_code'])
        routesRT = pd.DataFrame(rows).set_index(['route_short_name'])

    logtimesVals["list"].extend(vals)
    testtableVals.extend(testvals)
    t = time.time() - t
    print("\033[96m", end="")
    if t > 10:
        print("\u001b[7m", end="")
        logger(f"{getDatetimeNowStr()}--- getRT ({len(rows)} items):\t{t} seconds\t---")
    print(f"--- getRT ({len(rows)} items)"+' '*(8-len(str(len(rows))))+f":\t{t} seconds\t---\u001b[0m")
    runningRT = 0
    t = time.time()
    try:
        response = requests.get('http://percorsieorari.gtt.to.it/das_gtfsrt/vehicle_position.aspx')
        rt.ParseFromString(response.content)
    except ConnectionError as err:
        logger(
            f'{getDatetimeNowStr()} <b>getRT()<\b>\nrequest.get(), rt.ParseFromString(response.content) raised ConnectionError: {repr(err)},\naborting')
        return 0

    print(f"--- retrievePos ({len(rt.entity)} items):\t{time.time() - t} seconds\t---")
    t = time.time()
    posRT = {}
    for el in rt.entity:
        if el.HasField('vehicle'):
            v = el.vehicle
            posRT[v.trip.trip_id] = {
                                    "route_id": v.trip.route_id,
                                    "latitude": v.position.latitude,
                                    "longitude": v.position.longitude,
                                    "bearing": v.position.bearing,
                                    "timestamp": v.timestamp,
                                    "label": v.vehicle.label }
    print(f"--- getPos ({len(rt.entity)} items):\t{time.time() - t} seconds\t---")


def getStopRT(stopCode):
    """
        :param stopCode: str
        :return: tuple: tuple[0] -1 no data,
                0 tuple[1] is a Series, 1 tuple[1] is a DataFrame
    """
    try:
        s = (0, stopsRT.loc[stopCode])
        if isinstance(s[1], pd.DataFrame):
            s = (1, s[1].sort_values(by=['route_short_name', 'timestamp']))
            s[1].reset_index(drop=True, inplace=True)
    except KeyError:
        s = (-1, pd.DataFrame())
    return s


def getRouteRT(routeShortName):
    """
        :param routeName: str
        :return: tuple: tuple[0] -2 route not found, -1 route found but no data,
                0 tuple[1] is a Series, 1 tuple[1] is a DataFrame
    """
    if routeShortName + 'U' not in routes.index:
        return -2, pd.DataFrame()
    try:
        s = (0, routesRT.loc[routeShortName])
        if isinstance(s[1], pd.DataFrame):
            s = (1, s[1].assign(route_short_name=s[1].index))
            s = (1, s[1].sort_values(by=['trip_id', 'stop_sequence']))
            s[1].reset_index(drop=True, inplace=True)
    except KeyError:
        s = -1, pd.DataFrame()
    return s

def getMapRT(tripId):
    BBox = ((7.6267, 7.7070,
             45.0864, 45.0454))
    ruh_m = plt.imread("C:\\Users\\giann\\Downloads\\map.png")
    fig, ax = plt.subplots(figsize=(8, 7))

    pos = posRT[tripId]

    ax.scatter(pos["longitude"], pos["latitude"], zorder=1, alpha=1, c='b', s=4, label=pos["label"])
    ax.annotate(pos["label"], (pos["longitude"], pos["latitude"]))
    ax.set_xlim(BBox[0], BBox[1])
    ax.set_ylim(BBox[2], BBox[3])
    plt.axis('off')
    ax.imshow(ruh_m, zorder=0, extent=BBox, aspect='equal')
    plt.savefig("C:\\Users\\giann\\Downloads\\plot.png", dpi=600, bbox_inches='tight', pad_inches=0)

def getTimetable():
    if runningRT == 1:
        print(f"--- getTimetable waiting getRT to stop")
        #threading.Timer(5, getTimetable).start()
        return 0
    global ttable, logtimesVals, testtableVals, runningGetTT
    runningGetTT = 1
    t = time.time()
    dt = datetime.datetime.now()
    if dt.minute > 50:
        dt = dt.replace(minute=0, second=0, microsecond=0) + datetime.timedelta(hours=1)
    else:
        dt = dt.replace(minute=0, second=0, microsecond=0)
    secs = dt.timestamp() - time.time() + 3600
    print(f"---getTimetable next call in {round(secs)} secs")
    #threading.Timer(round(secs), getTimetable).start()

    hour = dt   #17:14 -> 17:00
    dt = datetime.date.today()
    start = dt - datetime.timedelta(days=dt.weekday())  #this monday
    dt = datetime.time(hour=0, minute=0, second=0)      #00:00:00.0
    timestamp = int(hour.timestamp() - datetime.datetime.combine(start, dt).timestamp())

    sql = "SELECT route_id,stop_sequence,stop_id,stop_code,AVG(time) as tm " \
          "FROM weekly_timetable " \
          "WHERE timestamp_week IN (%s,%s,%s) " \
          "GROUP BY route_id,stop_sequence,stop_id,stop_code;"
    adr = (timestamp, timestamp + 3600, timestamp - 3600)

    i = 0
    while i < 3:
        try:
            cr.execute(sql, adr)
            times = cr.fetchall()
        except mysql.connector.Error as err:
            logger(
                f'{getDatetimeNowStr()} <b>getTimetable()<\b>\nSELECT FROM weekly_timetable' +
                f' raised: {repr(err)},\ncalling getDBconnection() and retrying, attempt: {i+1}')
            getDBconnection()
        else:
            break
        i += 1
    if i == 3:
        logger(
            f'{getDatetimeNowStr()} <b>getTimetable()<\b>\nvals cr.execute(), db.commit()' +
            f' FAILED 3 times aborting')
        return 0

    dic = {}
    for el in times:
        routeId = el["route_id"]
        stopSequence = el["stop_sequence"]
        if routeId in dic:
            if stopSequence in dic[routeId]:
                #possibili dati su una fermata diversa, ma stesso stop seq per deviazione
                dic[routeId][stopSequence] = [el["stop_id"], el["stop_code"], (el["tm"]+dic[routeId][stopSequence][2])/2]
            else:
                dic[routeId][stopSequence] = [el["stop_id"], el["stop_code"], el["tm"]]
        else:
            dic[routeId] = {}
            dic[routeId][stopSequence] = [el["stop_id"], el["stop_code"], el["tm"]]

    print(f"--- {hour}-{timestamp} : Loaded {len(dic)}/115 routes in ttable in {time.time() - t} seconds")
    t = time.time()

    ttable = dic
    logs = logtimesVals["list"]
    logtimesVals = {"set": set(), "list": []}
    tests = testtableVals
    testtableVals = []
    runningGetTT = 0


#def init(l):
global logger
test = {}
ttable = None
stopsRT = None
db = None
runningRT = 0
runningGetTT = 0
stopTdict = {}
logtimesVals = {"set": set(), "list": []}
testtableVals = []
logger = print
getDBconnection()
getGTFS()
getTimetable()
print("--- getGTFS: %s seconds ---" % (time.time()))
getRT()

getMapRT(next(iter(posRT)))