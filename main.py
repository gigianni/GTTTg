# TO-DO:

import collections
import csv
import datetime
import io
import math
import threading
import time
import zipfile
import requests
from google.transit import gtfs_realtime_pb2

global logger, runningRT, runningGTFS, RT


class RealTimeData:
    """RT contains all the info got by every poll to the GTFSrt source"""

    def __init__(self):
        self.trips = {}
        self.stops = {}
        self.routes = {}
        self.stopcodes = {}

    def add_stop(self, data):
        """Adds a stop, stop["stop_times"] is a dict with trip_id+"-"+stop_id as keys"""
        self.stops[data["stop_id"]] = {
            "stop_name": data["stop_name"],
            "stop_code": data["stop_code"],
            "stop_desc": data["stop_desc"],
            "stop_lat": data["stop_lat"],
            "stop_lon": data["stop_lon"],
            "stop_times": {}
        }
        self.stopcodes[data["stop_code"]] = data["stop_id"]

    def add_route(self, data):
        self.routes[data["route_id"]] = {
            "route_short_name": data["route_short_name"],
            "trips": {},
            "timetable": collections.OrderedDict()
        }

    def delete_trip(self, trip_id):
        for stop_time in self.trips[trip_id]["stop_times"]:
            del self.stops[stop_time["stop_id"]]["stop_times"][trip_id + "-" + stop_time["stop_id"]]

        del self.routes[self.trips[trip_id]["route_id"]]["trips"][trip_id]
        del self.trips[trip_id]

    def check_trip(self, trip_id):
        """Checks if trip exists (returns 1), or not (returns 0)"""
        if trip_id in self.trips:
            return 1
        else:
            return 0

    def get_trip(self, trip_id):
        if trip_id in self.trips:
            return self.trips[trip_id]
        return None

    def add_trip(self, data):
        self.trips[data["trip_id"]] = {
            "route_id": data["route_id"],
            "route_short_name": self.routes[data["route_id"]]["route_short_name"],
            "direction": data["direction_id"],
            "headsign": data["trip_headsign"],
            "limited": data["limited_route"],
            "position": {
                "latitude": None,
                "longitude": None,
                "bearing": 0,
                "timestamp": 0
            },
            "stop_times": collections.OrderedDict(),
            "stop_times_count": 0,
            "recent_arrivals": {},
            "timetable_version": ""
        }
        self.routes[data["route_id"]]["trips"][data["trip_id"]] = self.trips[data["trip_id"]]

    def update_position_trip(self, trip_id, latitude, longitude, bearing, timestamp):
        self.trips[trip_id]["position"]["latitude"] = latitude
        self.trips[trip_id]["position"]["longitude"] = longitude
        self.trips[trip_id]["position"]["bearing"] = bearing
        self.trips[trip_id]["position"]["timestamp"] = timestamp

    def set_stop_time(self, trip_id, stop_sequence, timestamp, std_dev):
        self.trips[trip_id]["stop_times_count"] += 1
        version = self.trips[trip_id]["timetable_version"]
        route_id = self.trips[trip_id]["route_id"]
        stop_id = self.routes[route_id]["timetable"][version][stop_sequence]["stop_id"]
        self.trips[trip_id]["stop_times"][stop_id] = {
            "stop_sequence": stop_sequence,
            "timestamp": timestamp,
            "std_dev": std_dev
        }
        if route_id not in self.stops[stop_id]["stop_times"]:
            self.stops[stop_id]["stop_times"][route_id] = {
                "route_short_name": self.routes[route_id]["route_short_name"],
                "times": collections.OrderedDict()
            }

        self.stops[stop_id]["stop_times"][route_id]["times"][trip_id + "-" + stop_id] = {
            "timestamp": timestamp,
            "std_dev": std_dev
        }

    def add_timetable(self, route_id, stops_dic, version):
        self.routes[route_id]["timetable"][version] = {}
        i = 1
        count = 0
        while count < len(stops_dic):
            if i in stops_dic:
                self.routes[route_id]["timetable"][version][i] = {
                    "stop_id": stops_dic[i],
                    "times": [],
                    "mean": 0,
                    "var": 0,
                    "N": 0,
                    "std_dev": 0
                }
                count += 1
            i += 1

    def check_trip_stop_times(self, updated_trips):
        """
        Iterates every trip exluding the one given in updated_trips and checks delete the whole stop_times if they are
        all prior to now
        :param updated_trips: a Set() containing the trips NOT to be checked:
        :return:
        """
        now = time.time()
        for trip_id, trip in self.trips.items():
            if trip_id not in updated_trips:
                delete = True
                for stop_id, stoptime in trip["stop_times"].items():
                    if stoptime["timestamp"] >= now:
                        delete = False
                        break
                if delete:
                    self.clear_trip_stop_times(trip_id)

    def clear_trip_stop_times(self, trip_id):
        """
            Deletes every instance of every stop_time of a given trip
            :param trip_id: str
            :return:
        """
        route_id = self.trips[trip_id]["route_id"]
        for stop_id in self.trips[trip_id]["stop_times"]:
            del self.stops[stop_id]["stop_times"][route_id]["times"][trip_id + "-" + stop_id]
            if len(self.stops[stop_id]["stop_times"][route_id]["times"]) == 0:
                del self.stops[stop_id]["stop_times"][route_id]

        self.trips[trip_id]["stop_times_count"] = 0
        del self.trips[trip_id]["stop_times"]
        self.trips[trip_id]["stop_times"] = {}

    def extend_stop_times(self, trip_id):
        stop_times = self.trips[trip_id]["stop_times"]
        stop_id = next(iter(stop_times))
        last_tm = stop_times[stop_id]["timestamp"]
        last_dev = 0
        timetable = self.routes[self.trips[trip_id]["route_id"]]["timetable"][self.trips[trip_id]["timetable_version"]]

        cnt = 0
        reached_pos = False
        for stop_sequence, time in timetable.items():
            if reached_pos or time["stop_id"] in stop_times:
                reached_pos = True
                last_tm += time["mean"]
                last_dev += time["std_dev"]
                self.set_stop_time(trip_id, stop_sequence, last_tm, last_dev)
                cnt += 1

        self.trips[trip_id]["stop_times_count"] += cnt
        return cnt

    def add_arrival(self, trip_id, arrivals):
        """
        Used to update the timetable by using the previous arrivals time
        :param trip_id:
        :param arrivals: list of tuples (stop_sequence, timestamp of arrival)
        :return:
        """
        recent_arrivals = self.trips[trip_id]["recent_arrivals"]

        for i in range(len(arrivals)):
            if arrivals[i][0] - 1 in recent_arrivals:
                if arrivals[i][0] not in recent_arrivals:
                    self.update_timetable(trip_id, arrivals[i][0], arrivals[i][1] - recent_arrivals[arrivals[i][0] - 1])
                if i == 0:
                    del recent_arrivals[arrivals[i][0] - 1]

            recent_arrivals[arrivals[i][0]] = arrivals[i][1]

    def clear_arrivals(self):
        for trip_id in self.trips:
            RT.trips[trip_id]["recent_arrivals"] = {}

    def update_timetable(self, trip_id, stop_sequence, timedelta):
        version = self.trips[trip_id]["timetable_version"]
        if stop_sequence in self.routes[self.trips[trip_id]["route_id"]]["timetable"][version]:
            timetable = self.routes[self.trips[trip_id]["route_id"]]["timetable"][version][stop_sequence]
            timetable["times"].append(timedelta)

            oldmean = timetable["mean"]
            N = timetable["N"]

            if N == 0:
                timetable["mean"] = timedelta
                timetable["var"] = 0
                timetable["N"] = 1
            else:
                timetable["mean"] += (timedelta - oldmean) / (N + 1)
                timetable["var"] = ((N - 1) * timetable["var"] + (timedelta - timetable["mean"]) * (
                        timedelta - oldmean)) / N
                if N >= 15:
                    # max 15 entries in times, the oldest will be deleted
                    timetable["mean"] += (timetable["mean"] - timetable["times"][0])/N
                    timetable["var"] = ((N - 2) * timetable["var"] + (timetable["times"][0] - timetable["mean"]) * (
                            timetable["times"][0] - oldmean)) / (N - 1)
                    timetable["times"].pop(0)
                else:
                    timetable["N"] += 1
            timetable["std_dev"] = math.sqrt(timetable["var"])


def getDatetimeNowStr():
    """
        Return current datetime as a string in the
        "%d/%m/%y %H:%M:%S" format.
    """
    return datetime.datetime.now().strftime("%d/%m/%y %H:%M:%S")


def getGTFS():
    """
        Retrieve GTFS data from "https://www.gtt.to.it/open_data/gtt_gtfs.zip"
        and populates RT
    """
    global runningGTFS
    runningGTFS = 1
    t = time.time()
    p = 'https://www.gtt.to.it/open_data/gtt_gtfs.zip'

    r = requests.get(p)
    if not r.ok:
        logger(f'{getDatetimeNowStr()} <b>getGTFS()<\b>\n error retrieving gtt_gtfs.zip,'
               f'\n calling getGTFS()')
        getGTFS()

    archive = zipfile.ZipFile(io.BytesIO(r.content))

    print("--- retrieveGTFS: %s seconds ---" % (time.time() - t))
    t = time.time()
    global RT
    RT = RealTimeData()

    s = archive.read('stops.txt').decode("utf-8").splitlines()
    data = {}
    key_index = []
    firstRow = True

    for row in csv.reader(s, quotechar='"', delimiter=',', quoting=csv.QUOTE_ALL, skipinitialspace=True):
        # "stop_id", "stop_code", "stop_name", "stop_desc", "stop_lat", "stop_lon", "zone_id", "stop_url", "location_type", "parent_station", "stop_timezone", "wheelchair_boarding"
        if firstRow:
            firstRow = False
            for el in row:
                data[el] = ""
                key_index.append(el)
        else:
            i = 0
            for el in row:
                data[key_index[i]] = el
                i += 1
            RT.add_stop(data)

    s = archive.read('routes.txt').decode("utf-8").splitlines()
    data = {}
    key_index = []
    firstRow = True
    for row in csv.reader(s, quotechar='"', delimiter=',', quoting=csv.QUOTE_ALL, skipinitialspace=True):
        # "route_id", "agency_id", "route_short_name", "route_long_name", "route_desc", "route_type", "route_url", "route_color", "route_text_color", "route_sort_order"
        if firstRow:
            firstRow = False
            for el in row:
                data[el] = ""
                key_index.append(el)
        else:
            i = 0
            for el in row:
                data[key_index[i]] = el
                i += 1
            RT.add_route(data)

    s = archive.read('trips.txt').decode("utf-8").splitlines()
    data = {}
    key_index = []
    firstRow = True
    for row in csv.reader(s, quotechar='"', delimiter=',', quoting=csv.QUOTE_ALL, skipinitialspace=True):
        # "route_id","service_id","trip_id","trip_headsign","trip_short_name","direction_id","block_id","shape_id","wheelchair_accessible","bikes_allowed","limited_route"
        if firstRow:
            firstRow = False
            for el in row:
                data[el] = ""
                key_index.append(el)
        else:
            i = 0
            for el in row:
                data[key_index[i]] = el
                i += 1
            RT.add_trip(data)


    s = archive.read('stop_times.txt').decode("utf-8").splitlines()
    times = {}
    dt = datetime.datetime.now()
    timetable_id = {}
    """
    {
       route_id:
       {
            trip_id: 
            {
                dic: {stop_sequence: stop_id, ...}
                set: set(stop_sequence-stop_id, ...)
            }, ...
       }, ...
    }
    """
    data = {}
    key_index = []
    firstRow = True
    for row in csv.reader(s, quotechar='"', delimiter=',', quoting=csv.QUOTE_ALL, skipinitialspace=True):
        # "trip_id","arrival_time","departure_time","stop_id","stop_sequence","stop_headsign","pickup_type","drop_off_type","shape_dist_traveled","timepoint"
        if firstRow:
            firstRow = False
            for el in row:
                data[el] = ""
                key_index.append(el)
        else:
            i = 0
            for el in row:
                data[key_index[i]] = el
                i += 1

            route_id = RT.trips[data["trip_id"]]["route_id"]

            if route_id not in timetable_id:
                timetable_id[route_id] = {}
                timetable_id[route_id][data["trip_id"]] = {
                    "dic": {int(data["stop_sequence"]): data["stop_id"]},
                    "set": {data["stop_sequence"]+"-"+data["stop_id"]}
                }
            elif data["trip_id"] not in timetable_id[route_id]:
                timetable_id[route_id][data["trip_id"]] = {
                    "dic": {int(data["stop_sequence"]): data["stop_id"]},
                    "set": {data["stop_sequence"]+"-"+data["stop_id"]}
                }
            else:
                timetable_id[route_id][data["trip_id"]]["dic"][int(data["stop_sequence"])] = data["stop_id"]
                timetable_id[route_id][data["trip_id"]]["set"].add(data["stop_sequence"]+"-"+data["stop_id"])

            ti = data["arrival_time"].split(':')
            if int(ti[0]) > 23:
                dt += datetime.timedelta(days=1)
            dt = dt.replace(hour=int(ti[0]) % 24, minute=int(ti[1]), second=int(ti[2]), microsecond=0)
            if data["trip_id"] in times:
                times[data["trip_id"]].append((int(data["stop_sequence"]), dt.timestamp()))
            else:
                times[data["trip_id"]] = [(int(data["stop_sequence"]), dt.timestamp())]
            if int(ti[0]) > 23:
                dt -= datetime.timedelta(days=1)

    archive.close()

    cnt = 0
    cnt2 = 0
    for route_id, trips in timetable_id.items():
        for trip_id_A, data_A in trips.items():
            if RT.trips[trip_id_A]["timetable_version"] == "":
                cnt += 1
                RT.add_timetable(route_id, data_A["dic"], trip_id_A)
                RT.trips[trip_id_A]["timetable_version"] = trip_id_A
                for trip_id_B, data_B in trips.items():
                    if RT.trips[trip_id_B]["timetable_version"] == "" and data_A["set"] == data_B["set"]:
                        RT.trips[trip_id_B]["timetable_version"] = trip_id_A
        cnt2 += len(trips)

    del timetable_id
    print(f"--- {cnt2} trips reduced to {cnt} timetable versions ({round(cnt2/cnt)}:1) ---")

    for trip_id, arrivals in times.items():
        RT.add_arrival(trip_id, arrivals)
    RT.clear_arrivals()
    runningGTFS = 0
    print("--- getGTFS: %s seconds ---" % (time.time() - t))


def RTmanager(i):
    if runningRT or runningGTFS:
        if i != 0 and i % 10 == 0:
            logger(f'{getDatetimeNowStr()} <b>RTmanager()<\b>\ngetRT didn\'t run for: {i * 5 + 15} seconds')
        threading.Timer(5, RTmanager, [i + 1]).start()
    else:
        getRT()

def getRT():
    global RT, runningRT
    runningRT = 1
    threading.Timer(15, RTmanager, [0]).start()

    t = time.time()
    rt = gtfs_realtime_pb2.FeedMessage()
    try:
        response = requests.get('http://percorsieorari.gtt.to.it/das_gtfsrt/trip_update.aspx')
        rt.ParseFromString(response.content)
    except ConnectionError as err:
        logger(
            f'{getDatetimeNowStr()} <b>getRT()<\b>\nrequest.get(), rt.ParseFromString(response.content) raised ConnectionError: {repr(err)},\naborting')
        runningRT = 0
        return 0
    print(getDatetimeNowStr())
    print(f"--- retrieveRT  ({len(rt.entity)} items):\t{'{:.5f}'.format(time.time() - t)} seconds\t---")
    t = time.time()

    ct = 0
    updated_trips = set()
    for entity in rt.entity:
        if entity.HasField('trip_update'):
            if RT.check_trip(entity.trip_update.trip.trip_id) == 0:
                logger(
                    f'{getDatetimeNowStr()} not such trip_id: {entity.trip_update.trip.trip_id},\ncalling getGTFS()')
                getGTFS()
                runningRT = 0
                return 0
            updated_trips.add(entity.trip_update.trip.trip_id)
            RT.clear_trip_stop_times(entity.trip_update.trip.trip_id)
            counter = 0  # used to load only one estimated time, all the other will estimated by the system
            arrivals = []
            for stopt in entity.trip_update.stop_time_update:
                if float(stopt.departure.time) >= time.time():
                    if counter == 0:
                        RT.set_stop_time(entity.trip_update.trip.trip_id, stopt.stop_sequence, stopt.departure.time, 0)
                        ct += RT.extend_stop_times(entity.trip_update.trip.trip_id) + 1
                        counter = 1
                else:
                    arrivals.append((stopt.stop_sequence, stopt.departure.time))

            if len(arrivals) > 0:
                RT.add_arrival(entity.trip_update.trip.trip_id, arrivals)

    RT.check_trip_stop_times(updated_trips)
    t = time.time() - t
    print("\033[96m", end="")
    if t > 10:
        print("\u001b[7m", end="")
        logger(f"{getDatetimeNowStr()}--- getRT ({ct} items):\t{t} seconds\t---")
    print(f"--- getRT" + ' ' * (
            10 - len(str(ct))) + f"({ct} items):\t{'{:.5f}'.format(t)} seconds\t---\u001b[0m")
    runningRT = 0
    t = time.time()
    try:
        response = requests.get('http://percorsieorari.gtt.to.it/das_gtfsrt/vehicle_position.aspx')
        rt.ParseFromString(response.content)
    except ConnectionError as err:
        logger(
            f'{getDatetimeNowStr()} <b>getRT()<\b>\nrequest.get(), rt.ParseFromString(response.content) raised ConnectionError: {repr(err)},\naborting')
        return 0

    print(f"--- retrievePos ({len(rt.entity)} items):\t{'{:.5f}'.format(time.time() - t)} seconds\t---")
    t = time.time()
    for el in rt.entity:
        if el.HasField('vehicle'):
            v = el.vehicle
            RT.update_position_trip(v.trip.trip_id, v.position.latitude, v.position.longitude, v.position.bearing, v.timestamp)

    print(f"--- getPos      ({len(rt.entity)} items):\t{'{:.5f}'.format(time.time() - t)} seconds\t---")


def getStopRT(stopcode):
    """
        :param stopcode: str
        :return: tuple: tuple[0] == -1 stopCode does not exists,
                tuple[0] == 1 -> tuple[1] is a dict
                key: trip_id-stop_id
                value: {"trip_id","route_short_name","timestamp","std_dev"}
    """
    while runningGTFS or runningRT:
        time.sleep(0.01)

    if stopcode in RT.stopcodes:
        s = (1, RT.stops[RT.stopcodes[stopcode]])
    else:
        s = (-1, {})
    return s


def getRouteRT(route_id):
    """
        :param route_id: str
        :return: tuple: tuple[0]==-1 route not found,
                tuple[0]==1 -> tuple[1] is a dict of dicts:
                key of dict: "trip_id"
                key of dicts:{"route_id","route_short_name","direction","headsign",
                "limited","position","stop_times","recent_arrivals"}
    """
    while runningGTFS or runningRT:
        time.sleep(0.01)

    if route_id not in RT.routes:
        return -1, []
    else:
        s = [1, {}]
        for trip_id in RT.routes[route_id]["trips"]:
            s[1][trip_id] = RT.trips[trip_id]
    return s

"""
def getMapRT(tripId):
    
        :param tripId: str
        :return: tuple: tuple[0] -1 trip not found,
                0 tuple[1] is a dict: "route_id", "latitude", "longitude", "bearing", "timestamp", "label"
    
    if tripId in posRT:
        pos = posRT[tripId]
        s = (0, pos)
    else:
        s = (-1, pd.DataFrame())
    BBox = ((7.6267, 7.7070,
             45.0864, 45.0454))
    ruh_m = plt.imread("map.png")
    fig, ax = plt.subplots(figsize=(8, 7))

    

    ax.scatter(pos["longitude"], pos["latitude"], zorder=1, alpha=1, c='b', s=4, label=pos["label"])
    ax.annotate(pos["label"], (pos["longitude"], pos["latitude"]))
    ax.set_xlim(BBox[0], BBox[1])
    ax.set_ylim(BBox[2], BBox[3])
    plt.axis('off')
    ax.imshow(ruh_m, zorder=0, extent=BBox, aspect='equal')
    buf = io.BytesIO()
    plt.savefig(buf, dpi=400, bbox_inches='tight', pad_inches=0)
    buf.seek(0)
    buf.name = 'image.png'
    return s
"""

def printer():
    print(getStopRT("40"))
    print(getStopRT("3445"))
    print(getStopRT("471"))

    threading.Timer(200, printer).start()


def init(l):
    global logger, runningRT, runningGTFS
    logger = l
    runningRT = 0
    runningGTFS = 0
    getGTFS()
    getRT()

#init(print)
