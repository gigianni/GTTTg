import logging

import telegram

import main as m
import datetime as dt
import threading

from telegram import Update, ForceReply, ParseMode, InlineKeyboardButton, InlineKeyboardMarkup, InputMediaPhoto
from telegram.ext import Updater, CommandHandler, MessageHandler, Filters, CallbackContext, CallbackQueryHandler

f = open("../tg.txt", "r")
TOKEN = f.read().rstrip()
f.close()

# Enable logging
logging.basicConfig(
	format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO
)

logger = logging.getLogger(__name__)


# Define a few command handlers. These usually take the two arguments update and
# context.2
def start(update: Update, context: CallbackContext) -> None:
	"""Send a message when the command /start is issued."""
	user = update.effective_user
	update.message.reply_markdown_v2(
		fr'Hi {user.mention_markdown_v2()}\!',
		reply_markup=ForceReply(selective=True),
	)


def help_command(update: Update, context: CallbackContext) -> None:
	"""Send a message when the command /help is issued."""
	update.message.reply_text('Puoi inviare il numero della fermata oppure il numero della linea preceduto da L (per esempio: L15)')


def logs_command(update: Update, context: CallbackContext) -> None:
	f = open("../screenlog.0", "r")
	lines = f.read().splitlines()
	update.message.reply_text('\n'.join(lines[-20:]))
	f.close()


def sendRouteData(chat_id, route_id, trip_id='-1'):
	keyboard = []
	s = m.getRouteRT(route_id)
	if s[0] == -1:
		msg = "Linea non trovata"
	else:
		if trip_id == '-1':
			for id, trip in s[1].items():
				if trip["stop_times_count"] > 0:
					trip_id = id
					break
		if trip_id == '-1':
			msg = "Nessun passaggio trovato"
		else:
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
				if trip["stop_times_count"] > 0:
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
			if aDir != "":
				msg += "\nAltri mezzi nel formato <i>Direzione - Prossima fermata</i>, con direzione:\n"
				msg += f"A: {aDir}\nB: {bDir}"

			position = s[1][trip_id]['position']
			if position["timestamp"] != 0:
				upd.bot.sendLocation(chat_id=chat_id, location=telegram.Location(longitude=position["longitude"],
								latitude=position["latitude"], live_period=10, heading=position["bearing"]))

	reply_markup = InlineKeyboardMarkup(keyboard)
	upd.bot.sendMessage(chat_id=chat_id, text=msg, parse_mode=ParseMode.HTML, reply_markup=reply_markup)


def texthandler(update: Update, context: CallbackContext) -> None:
	if update.message.text[0].lower() == 'l':
		sendRouteData(update.message.chat_id, update.message.text[1:] + 'U')
	else:
		keyboard = []
		s = m.getStopRT(update.message.text)
		if s[0] == -1:
			msg = "Fermata non trovata"
		elif len(s[1]) == 0:
			msg = "Nessun arrivo previsto"
		else:
			msg = m.RT.stops[m.RT.stopcodes[update.message.text]]['stop_name']+"\n"
			i = 0
			for route_id, route_times in s[1].items():
				msg += f"<b>{route_times['route_short_name']}</b>\n"
				if not i % 3:
					keyboard.append([])
				keyboard[i//3].append(
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
		update.message.reply_text(msg, parse_mode=ParseMode.HTML, reply_markup=reply_markup)


def button(update: Update, context: CallbackContext) -> None:
	query = update.callback_query
	query.answer()
	routeId, tripId = query.data.split('_')[0], query.data.split('_')[1]

	sendRouteData(query.message.chat_id, routeId, tripId)


upd = None
def main() -> None:
	updater = Updater(TOKEN)
	global upd
	upd = updater
	dispatcher = updater.dispatcher

	dispatcher.add_handler(CommandHandler("start", start))
	dispatcher.add_handler(CommandHandler("help", help_command))
	dispatcher.add_handler(CommandHandler("logs", logs_command, Filters.chat(chat_id=84266954)))
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