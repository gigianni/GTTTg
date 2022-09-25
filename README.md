# GTTtg
## _Bringing GTFS realtime to Telegram_

GTTtg use data from [aperTO](http://aperto.comune.torino.it) for a simple Telegram bot that can show the real time arrivals of public transport of the city of Turin.

The two dataset used can be found at [GTFS](http://aperto.comune.torino.it/dataset/feed-gtfs-trasporti-gtt) and [GTFS-RealTime](http://aperto.comune.torino.it/dataset/feed-gtfs-real-time-trasporti-gtt).

#The telegram bot

#The RT datastructure
All the structures are implemented with standard Python structure, usually dicts of dicts.

The function getGTFS() loads all the static part, indicated with a grey background in the sequent picture.
The function getRT() is called every 15 seconds and updates the times of arrival at the stops (stop_times) 
and the position of every trip. 
    
- routes: a dictionary of all the routes, every route is a dict with its info, related trips and timetable
- version and timetable: every route has a few different paths (2 for the 2 ways and some other with some different stops) so there's a version for every path with his own timetable which contains all the combination of stop_sequence and stop_id and the extimated time delta between the stop and the previous one. This time is loaded initialy by getGTFS with the timetable one (found in stoptimes.txt) and **it is updated every time the GTFS-RT gives an arrival (a timestamp prior to now), by this way the system has an extimated time delta between two stop based always on the last 15 registered arrivals (stored in times[ ]) and can follow the traffic flow**. Also the variance and the standard devation of this time is calculated and shown to the user.
- trips: a dictionary of all the trips (singular istance of a route), every element contains general info plus the last position and a list of stop_times
- stops: a dict with info about every stop with its stop_times
- stopcodes: a simple translation table implemented by a dict for passing from stop_codes (the numbers you see at the bus stops) to stop_id (the one used in the GTFS system)
- stop_times: the core of the realtime part, stop_time are referenced both by trips and stops: the former is a [ordered dict](https://docs.python.org/3/library/collections.html#collections.OrderedDict) which indicates for that specific trip which are its next stops and the estimated time of arrival, the latter is a dict containing every routes that has an extimated time of arrival at that stop and for that route a dict with the extimated times
- recent_arrivals: a dictionary holding the last arrivals of every trip, this is implemented to calculate the time delta between every arrival for updating the timetable mean. Every once a new time is inserted if there's the one referred to the previous stop the time delta is calculated and the previous time deleted. 
![Diagram](https://raw.githubusercontent.com/gigianni/GTTTg/main/GTTtg%20diagram.drawio.png)