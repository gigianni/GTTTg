import main as m
import logging
import telegram
import datetime as dt
import threading
from telegram import Update, ParseMode, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup
from telegram.ext import Updater, CommandHandler, MessageHandler, Filters, CallbackContext, CallbackQueryHandler, PicklePersistence

f = open("../tg.txt", "r")
TOKEN = f.read().rstrip()
f.close()

# Enable logging
logging.basicConfig(
	format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO
)

logger = logging.getLogger(__name__)


def start(update: Update, context: CallbackContext) -> None:
	"""Send a message when the command /start is issued."""
	if 'keyboard' in context.user_data:
		update.message.reply_text("Hi!", reply_markup=ReplyKeyboardMarkup(context.user_data['keyboard'], resize_keyboard=True))
	else:
		update.message.reply_text("Hi!", reply_markup=ReplyKeyboardMarkup([["+"]], resize_keyboard=True))


def routeNormalizer(text):
	"""Return route_id format from L<route_short_name> format, route_id could not be present in RT class"""
	return text[1:].upper().replace(" ", "") + 'U'


def add_command(update: Update, context: CallbackContext) -> None:
	"""Send a message when the command /add is issued."""
	if 'add_mode' not in context.user_data or context.user_data['add_mode'] == 0:
		context.user_data['add_mode'] = 1
		update.message.reply_text('Rispondi con la fermata di partenza')
	else:
		if context.user_data['add_mode'] == 1:
			context.user_data["add_content"] = update.message.text
		else:
			context.user_data["add_content"] += " "+update.message.text
		context.user_data['add_mode'] += 1
		sendTrackData(update.message.chat_id, context.user_data["add_content"].split(' '))
		keyboard = [[InlineKeyboardButton("Salva", callback_data='+'), InlineKeyboardButton("Annulla", callback_data='-')]]
		reply_markup = InlineKeyboardMarkup(keyboard)
		update.message.reply_text("Per fare una tratta seleziona una linea inviando L<codice linea>.\nPer aggiungere un'altra fermata manda il codice.", reply_markup=reply_markup)


def save_keyboard(chat_id, context):
	"""Saves the keyboard."""
	if context.user_data['add_mode'] > 0:
		keyboard = context.user_data.setdefault("keyboard", [["+"]])
		keyboard[len(keyboard)//3].insert(0, "*"+context.user_data["add_content"])
		context.user_data['add_mode'] = 0
		context.user_data['keyboard'] = keyboard
		upd.bot.sendMessage(chat_id=chat_id, text="Done.", reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True))


def discard_keyboard(chat_id, context):
	"""Saves the keyboard."""
	context.user_data['add_mode'] = 0
	upd.bot.sendMessage(chat_id=chat_id, text="Done.")


def sendTrackData(chat_id, data):
	msg = ""

	for i in range(len(data)):
		if data[i][0].lower() != 'l':

			if i+2 < len(data) and data[i+1][0].lower() == 'l':
				route_id = routeNormalizer(data[i+1])
				data[i+1] = 'L'+route_id
				stop = m.RT.get_stop_from_stopcode(data[i])
				next_stop = m.RT.get_stop_from_stopcode(data[i+2])
				if stop is not None and next_stop is not None:
					msg += f"Linea {route_id[:-1]} tratta:\n\t<b>{stop['stop_name'][8:]}"
					msg += f"\t\t-->\t\t{next_stop['stop_name'][8:]}</b>\n\n"
					stop_id = m.RT.stopcodes[data[i]]
					if route_id in m.RT.stops[stop_id]["stop_times"]:
						for key, el in m.RT.stops[stop_id]["stop_times"][route_id]["times"].items():
							arrival = m.RT.trips[key.split('-')[0]]['stop_times'][m.RT.stopcodes[data[i+2]]]
							msg += f"\t{dt.datetime.fromtimestamp(el['timestamp']).strftime('%H:%M')}" \
									f"\t\t\t±{round(el['std_dev'])} sec\t\t-->\t\t" \
									f"\t\t{dt.datetime.fromtimestamp(arrival['timestamp']).strftime('%H:%M')}" \
									f"\t\t\t±{round(arrival['std_dev'])} sec\n\n"
					else:
						msg += f"\t\tNessun arrivo per la linea {data[i+1][:-1]}\n"
			else:
				msg += getStopData(data[i])[0]

	upd.bot.sendMessage(chat_id=chat_id, text=msg, parse_mode=ParseMode.HTML)

def help_command(update: Update, context: CallbackContext) -> None:
	"""Send a message when the command /help is issued."""
	update.message.reply_text('Puoi inviare il numero della fermata oppure il numero della linea preceduto da L (per esempio: L15)')


def logs_command(update: Update, context: CallbackContext) -> None:
	f = open("../screenlog.0", "r")
	lines = f.read().splitlines()
	update.message.reply_text('\n'.join(lines[-20:]))
	f.close()


def json_command(update: Update, context: CallbackContext) -> None:
	f = open("RT.json", "w")
	f.write(m.RT.to_JSON())
	f.close()
	update.message.reply_text('Done')

def getStopData(text):
	keyboard = []
	s = m.getStopRT(text)
	reply_markup = None
	location = None
	if s[0] == -1:
		msg = "Fermata non trovata"
	elif len(s[1]["stop_times"]) == 0:
		msg = "Nessun arrivo previsto"
		location = telegram.Location(longitude=s[1]["stop_lon"], latitude=s[1]["stop_lat"])
	else:
		msg = f"<b>{s[1]['stop_name']}</b>\n{s[1]['stop_desc']}\n"
		i = 0
		for route_id, route_times in s[1]["stop_times"].items():
			msg += f"\n<b>{route_times['route_short_name']}</b>\n"
			if not i % 3:
				keyboard.append([])
			keyboard[i // 3].append(
				InlineKeyboardButton("Linea " + route_times["route_short_name"],
									 callback_data=route_id + "_" + next(iter(route_times["times"])).split('-')[0]))
			i += 1
			times = list(route_times["times"].values())
			# print in order of timestamp
			while len(times) != 0:
				min = 0
				j = 1
				while j < len(times):
					if times[j]["timestamp"] < times[min]["timestamp"]:
						min = j
					j += 1

				msg += f"\t\t{dt.datetime.fromtimestamp(times[min]['timestamp']).strftime('%H:%M')}" \
					   f"\t±{round(times[min]['std_dev'])} sec\n"
				times.pop(min)
		reply_markup = InlineKeyboardMarkup(keyboard)
		location = telegram.Location(longitude=s[1]["stop_lon"], latitude=s[1]["stop_lat"])
	return msg,location,reply_markup

def getRouteData(route_id, trip_id='-1'):
	keyboard = []
	s = m.getRouteRT(route_id)
	location = None
	if s[0] == -1:
		msg = "Linea non trovata"
	else:
		if len(s[1]) == 0:
			msg = "Nessun passaggio trovato"
		else:
			if trip_id == '-1':
				trip_id = next(iter(s[1]))
			msg = f"<b>Linea {s[1][trip_id]['route_short_name']}</b>\n"
			if s[1][trip_id]["limited"] == 1:
				msg += "<b>LIMITATO</b> vedi @gttavvisi\n"
			msg += f"Posizione aggiornata al " \
				   f"{dt.datetime.fromtimestamp(m.RT.trips[trip_id]['position']['timestamp']).strftime('%H:%M')}\n"

			for stop_id, stop_time in s[1][trip_id]["stop_times"].items():
					msg += f"{m.RT.stops[stop_id]['stop_name'][8:]} ({stop_time['stop_sequence']})\n" \
						   f"\t\t{dt.datetime.fromtimestamp(stop_time['timestamp']).strftime('%H:%M')}" \
						   f"\t\t\t±{round(stop_time['std_dev'])} sec\n"

			i = 1
			aDir = bDir = ""
			for id, trip in s[1].items():
				if trip["direction"] == 0:
					txt = "A - "
					aDir = trip["headsign"]
				else:
					txt = "B - "
					bDir = trip["headsign"]
				txt += m.RT.stops[next(iter(trip["stop_times"]))]["stop_name"][8:]
				if not (i - 1) % 2:
					keyboard.append([])
				keyboard[(i - 1) // 2].append(InlineKeyboardButton(txt, callback_data=route_id + '_' + id))
				i += 1
			if aDir != "" or bDir != "":
				msg += "\nAltri mezzi nel formato <i>Direzione - Prossima fermata</i>, con direzione:\n"
				msg += f"A: {aDir}\nB: {bDir}"

			position = s[1][trip_id]['position']
			if position["timestamp"] != 0:
				location = telegram.Location(longitude=position["longitude"],
									latitude=position["latitude"], live_period=10, heading=position["bearing"])

	reply_markup = InlineKeyboardMarkup(keyboard)
	return msg, location, reply_markup


def texthandler(update: Update, context: CallbackContext) -> None:
	init = update.message.text[0].lower()
	if init == "+" or 'add_mode' in context.user_data and context.user_data['add_mode'] != 0:
		add_command(update, context)
	elif init == '*':
		# button pressed
		sendTrackData(update.message.chat_id, update.message.text[1:].split(' '))
	else:
		if init == 'l':
			# route type
			msg, location, reply_markup = getRouteData(routeNormalizer(update.message.text))
			send_location_data(update.message.chat_id, context, msg, location, reply_markup)
		else:
			# stopcode
			msg, location, reply_markup = getStopData(update.message.text)
			send_location_data(update.message.chat_id, context, msg, location, reply_markup)


def send_location_data(chat_id, context, msg, location, reply_markup):
	if location is not None:
		if 'keyboard' in context.user_data:
			upd.bot.sendLocation(chat_id=chat_id, location=location,
								 reply_markup=ReplyKeyboardMarkup(context.user_data['keyboard'], resize_keyboard=True))
		else:
			upd.bot.sendLocation(chat_id=chat_id, location=location)

	upd.bot.sendMessage(chat_id=chat_id, text=msg, parse_mode=ParseMode.HTML, reply_markup=reply_markup)


def button(update: Update, context: CallbackContext) -> None:
	query = update.callback_query
	query.answer()
	if query.data[0] == '+':
		save_keyboard(query.message.chat_id, context)
	elif query.data[0] == '-':
		discard_keyboard(query.message.chat_id, context)
	else:
		routeId, tripId = query.data.split('_')[0], query.data.split('_')[1]
		msg, location, reply_markup = getRouteData(routeId, tripId)
		send_location_data(query.message.chat_id, context, msg, location, reply_markup)


upd = None
def main() -> None:
	persistence = PicklePersistence(filename="GTTtg_bot_persistence", store_user_data=True)
	updater = Updater(TOKEN, use_context=True, persistence=persistence)
	global upd
	upd = updater
	dispatcher = updater.dispatcher

	dispatcher.add_handler(CommandHandler("start", start))
	dispatcher.add_handler(CommandHandler("add", add_command))
	dispatcher.add_handler(CommandHandler("help", help_command))
	dispatcher.add_handler(CommandHandler("logs", logs_command, Filters.chat(chat_id=84266954)))
	dispatcher.add_handler(CommandHandler("json", json_command, Filters.chat(chat_id=84266954)))
	dispatcher.add_handler(MessageHandler(Filters.text & ~Filters.command, texthandler))
	updater.dispatcher.add_handler(CallbackQueryHandler(button))

	logMessage(f'Initialization {m.getDatetimeNowStr()}')
	updater.start_polling()
	updater.idle()

def logMessage(msg):
	print("\u001b[31;1m--- LOGGER: "+msg+"\u001b[0m")
	if upd is not None:
		upd.bot.sendMessage(chat_id='84266954', text=msg)
	else:	#if bot not loaded
		msg += "\nlog delayed of 30sec"
		threading.Timer(30, logMessage, [msg]).start()


if __name__ == '__main__':
	m.init(logMessage)
	main()
