import logging

import telegram

import main as m
import datetime as dt
import pandas as pd
import threading

from telegram import Update, ForceReply, ParseMode, InlineKeyboardButton, InlineKeyboardMarkup, InputMediaPhoto
from telegram.ext import Updater, CommandHandler, MessageHandler, Filters, CallbackContext, CallbackQueryHandler

f = open("tg.txt", "r")
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


def sendRouteData(chat_id, routeId, tripId=-1):
	keyboard = []
	s = m.getRouteRT(routeId)
	if s[0] == -2:
		msg = "Linea non trovata"
	elif s[0] == -1:
		msg = "Nessun passaggio previsto"
	else:
		if s[0] == 0:
			msg = f"<b>{s[1].route_short_name}</b>\n"
			msg += f"{s[1].stop_sequence}\t{s[1].stop_name[8:]} - {dt.datetime.fromtimestamp(s[1].timestamp).strftime('%H:%M')}"
			tripId = s[1].trip_id
		else:
			q = s[1].groupby(['trip_id'])["direction"].unique().sort_values()
			aDir = ""
			bDir = ""
			i = 1
			for trip, dir in q.items():
				if dir == 0:
					aDir = m.getTripHeadsign(trip)
					txt = "A - "
				else:
					bDir = m.getTripHeadsign(trip)
					txt = "B - "
				txt += s[1].loc[s[1]['trip_id'] == trip, "stop_name"].iloc[0].split('-')[1][1:]
				if not (i-1) % 2:
					keyboard.append([])
				keyboard[(i-1)//2].append(InlineKeyboardButton(txt, callback_data=routeId + '-' + trip))
				i += 1

			if tripId == -1:
				tripId = q.index[0]

			q = s[1].loc[s[1]['trip_id'] == tripId]
			if len(q.index) == 0:
				msg = f"{routeId[:-1]} - <b>Direzione: {m.getTripHeadsign(tripId)} </b>\n\nAL CAPOLINEA"
			else:
				msg = f"<b>{q.iat[0,5]} - Direzione: {m.getTripHeadsign(tripId)}</b>\n"

			for index, row in q.iterrows():
				msg += f"{row.stop_name[8:]}\n\t\t{dt.datetime.fromtimestamp(row.timestamp).strftime('%H:%M')}\n"

			msg += "\nAltri mezzi nel formato <i>Direzione - Prossima fermata</i>, con direzione:\n"
			if len(aDir) > 0:
				msg += f"A: {aDir}\n"
			if len(bDir) > 0:
				msg += f"B: {bDir}\n"

		s = m.getMapRT(tripId)
		if s[0] == 0:
			upd.bot.sendLocation(chat_id=chat_id, location=telegram.Location(longitude=s[1]["longitude"],
								latitude=s[1]["latitude"], live_period=10, heading=s[1]["bearing"]))

	reply_markup = InlineKeyboardMarkup(keyboard)
	if len(msg) != 0:
		upd.bot.sendMessage(chat_id=chat_id, text=msg, parse_mode=ParseMode.HTML, reply_markup=reply_markup)


def texthandler(update: Update, context: CallbackContext) -> None:
	if update.message.text[0].lower() == 'l':
		sendRouteData(update.message.chat_id, update.message.text[1:] + 'U')
	else:
		keyboard = []
		s = m.getStopRT(update.message.text)
		if s[0] == -1:
			msg = "Nessun arrivo previsto"
		elif s[0] == 0:
			msg = s[1].stop_name+"\n"
			msg += s[1].route_short_name+" - "+dt.datetime.fromtimestamp(s[1].timestamp).strftime("%H:%M")+"\n"
			keyboard.append([])
			keyboard[0].append(InlineKeyboardButton("Linea "+s[1].route_short_name, callback_data=s[1].route_id+'-'+s[1].trip_id))
		else:
			msg = s[1].iat[0, 1]+"\n"
			last = -1
			i = 0
			for index, row in s[1].iterrows():
				if last == -1 or row.route_short_name != last:
					msg += f"<b>{row.route_short_name}</b>\n"
					last = row.route_short_name
					if not i % 3:
						keyboard.append([])
					keyboard[i//3].append(
						InlineKeyboardButton("Linea " + row.route_short_name, callback_data=row.route_id+'-'+row.trip_id))
					i += 1
				msg += "\t\t"+dt.datetime.fromtimestamp(row.timestamp).strftime("%H:%M")+"\n"
		reply_markup = InlineKeyboardMarkup(keyboard)
		update.message.reply_text(msg, parse_mode=ParseMode.HTML, reply_markup=reply_markup)


def button(update: Update, context: CallbackContext) -> None:
	query = update.callback_query
	query.answer()
	routeId, tripId = query.data.split('-')[0], query.data.split('-')[1]

	sendRouteData(query.message.chat_id,routeId, tripId)


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