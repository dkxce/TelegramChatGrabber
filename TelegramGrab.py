#
# Telegram Chat Messages Grabber by dkxce
#

#
# pip3 install telethon
# pip3 install pandas
# pip3 install openpyxl
# https://my.telegram.org/apps
#

import os
import codecs
import configparser
import json
import pandas as pd
import time

from datetime import date, datetime, timedelta, timezone
from re import U

from telethon.sync import TelegramClient
from telethon import connection
from telethon.tl.functions.channels import GetParticipantsRequest
from telethon.tl.types import ChannelParticipantsSearch
from telethon.tl.functions.messages import GetHistoryRequest

# Application Data #
with open('app_data.json') as f:
	app_data = json.load(f)
save_to_full = app_data['save_to_full']
save_to_excel = app_data['save_to_excel']

# Telegram Client #
client = TelegramClient(app_data['username'], app_data['api_id'], app_data['api_hash'])
client.start()

# ----------------------------------------------- # MAIN FUNCTIONS # ----------------------------------------------- #

def utc_to_local(utc_dt):
    return utc_dt.replace(tzinfo=timezone.utc).astimezone(tz=None)

def GetUserById(uid: int) -> str:
	for ui in app_data['users']:
		if ui['uid'] == uid:
			return ui['name']
	return str(uid)

def write_file_header(text_file, ch, addit):
	text_file.write('# CHANNEL #\r\n')
	text_file.write('# ' + ch['name'] + ' #\r\n')
	text_file.write('# ' + ch['url'] + ' #\r\n')
	text_file.write('# ' + addit + ' #\r\n\r\n')
	text_file.write('MSG_ID;\tDATE;\tFROM;\tMessage\n')

def write_msg_to_file(text_file, message):
	text_file.write(str(message.id) + ";\t")
	text_file.write(utc_to_local(message.date).strftime("%H:%M:%S %d.%m.%Y") + ";\t")
	text_file.write(GetUserById(message.sender_id) + ";\t")
	if (not message.message) or (len(message.message) == 0):
		text_file.write(' ')
	else:
		text_file.write(message.message.replace('\n',' \t').replace(';','.,'))
	text_file.write('\r\n')

# ----------------------------------------------- # DUMP FUNCTIONS # ----------------------------------------------- #

async def dump_all_participants(channel, ch):
	offset_user = 0    # номер участника, с которого начинается считывание
	limit_user = 100   # максимальное число записей, передаваемых за один раз
	all_participants = []   # список всех участников канала
	filter_user = ChannelParticipantsSearch('')

	while True:
		participants = await client(GetParticipantsRequest(channel,
			filter_user, offset_user, limit_user, hash=0))

		if not participants.users:
			break

		all_participants.extend(participants.users)
		offset_user += len(participants.users)

	all_users_details = []   # список словарей с интересующими параметрами участников канала

	for participant in all_participants:
		all_users_details.append({"id": participant.id,
			"first_name": participant.first_name,
			"last_name": participant.last_name,
			"user": participant.username,
			"phone": participant.phone,
			"is_bot": participant.bot})

	with open('channel_' +str(ch['no']) +  '_users.json', 'w', encoding='utf8') as outfile:
		json.dump(all_users_details, outfile, ensure_ascii=False)

async def dump_all_messages(channel, ch):
	
	min_id = float('inf')           # мин номер записи
	max_id = 0                      # макс номер записи
	offset_msg = 0                  # номер записи, с которой начинается считывание	
	all_messages = []               # список всех сообщений
	uf = list(ch['user_filter'])    # фильтер пользователей
	min_msg_id = ch['min_msg_id']   # min mesage id	
	txt_writed = 0                  # записано сообщений в текстовый файл
	full_writed = 0                 #
	
	class DateTimeEncoder(json.JSONEncoder):
		def default(self, o):
			if isinstance(o, datetime):
				return o.isoformat()
			if isinstance(o, bytes):
				return list(o)
			return json.JSONEncoder.default(self, o)

	addit = 'From MSG_ID Only after ' + str(min_msg_id)
	prev = date.today()
	cnt = 0
	while True:		
		cnt += 1
		prev -=  timedelta(days=1)
		yd = prev.strftime("%Y-%m-%d")
		yd = 'channel_' + str(ch['no']) + '_messages_' + yd + '.txt'
		idff = 0
		if os.path.isfile(yd):
			line = ''
			with codecs.open(yd, 'r', "utf-8") as trf:
				while True:
					line = trf.readline()
					if not line:
						break
					if len(line) > 0 and line[0].isdigit():
						line = line[0:line.find(';')]
						idff = int(line)
						break;
		if idff > 0:
			min_msg_id = idff
			addit = 'From MSG_ID Only after ' + str(min_msg_id) + ' by ' + yd
			break
		if cnt == 30:
			break


	td = date.today().strftime("%Y-%m-%d")	
	ftn = 'channel_' + str(ch['no']) + '_messages_' + td + '.txt'	
	full = 'channel_' + str(ch['no']) + '_FULL_' + td + '.txt'	
	print(' >', addit)
	print(' > To Text File:', ftn)
	text_file = codecs.open(ftn, 'w', "utf-8")
	write_file_header(text_file, ch, addit)	
	if save_to_full:
		full_file = codecs.open(full, 'w', "utf-8")
		write_file_header(full_file, ch, addit)	
	print('')

	while True:
		history = await client(GetHistoryRequest(peer=channel,
			offset_id=offset_msg,offset_date=None, add_offset=0, limit=100, max_id=0, min_id=min_msg_id, hash=0))

		if not history.messages:
			break

		writed_loop = 0
		for message in history.messages:
			if message.id > max_id:
				max_id = message.id
			if message.id < min_id:
				min_id = message.id
			all_messages.append(message.to_dict())
			if save_to_full:
				write_msg_to_file(full_file, message)
				full_writed += 1
			if(message.sender_id in uf):				
				write_msg_to_file(text_file, message)
				writed_loop += 1
				txt_writed += 1				

		print(" writed", writed_loop, 'messages to text and', len(history.messages), 'to dump')
		offset_msg = history.messages[len(history.messages) - 1].id

	print('')

	text_file.close();
	if save_to_full:
		full_file.close();

	if txt_writed == 0:
		print(' File', ftn, 'has been removed because it empty')
		os.remove(ftn) 
	elif save_to_excel:
		data = pd.read_csv(ftn, header = None, delimiter = ";\t", skiprows = 6)
		data.columns = ["MSG_ID", "DATE", "FROM", "MESSAGE"]
		data.to_excel(ftn + '.xlsx')
		print(' > To Excel File', ftn + '.xlsx')

	if save_to_full:
		if full_writed == 0:
			print(' File', full, 'has been removed because it empty')
			os.remove(full) 
		elif save_to_excel:
			data = pd.read_csv(full, header = None, delimiter = ";\t", skiprows = 6)
			data.columns = ["MSG_ID", "DATE", "FROM", "MESSAGE"]
			data.to_excel(full + '.xlsx')
			os.remove(full) 
			print(' > To Excel File', full + '.xlsx')
	
	print(' MSG_ID FROM', min_id, 'TO', max_id)
	print('')	
	
	fdn = 'channel_' + str(ch['no']) + '_dump_' + td + '.json'
	print(' > To Dump File', fdn)
	with open(fdn, 'w', encoding='utf8') as outfile:
		 json.dump(all_messages, outfile, ensure_ascii=False, cls=DateTimeEncoder)

# ----------------------------------------------- # PROC FUNCTIONS # ----------------------------------------------- #


async def main():
	for ch in app_data['channels']:
		print('Dump channel', ch['url'])
		channel = await client.get_entity(ch['url'])
		#await dump_all_participants(channel, ch)
		await dump_all_messages(channel, ch)

print('dkxce Telegram Chat Messages Grabber')
print('----- Started ----\r\n\r\n')
with client:
	client.loop.run_until_complete(main())
print('\r\n\r\n----- Done    ----')
time.sleep(2)


### !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! ###
###                         HOW TO COMPILE                           ###
### !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! ###

### STEP 1
### Use: PYINSTALLER
###		https://www.geeksforgeeks.org/convert-python-script-to-exe-file/
###		https://pyinstaller.org/en/stable/usage.html#cmdoption-D        
### MSVS: Menu -> View -> Other Windows -> Python Environments ... Packages (PyPl), pip -> install
### MSVS: Menu -> View -> Other Windows -> Python Environments ... Packages (PyPl), pyinstaller -> install
### MSVS: Menu -> View -> Terminal
### > py -m pip -V
### > py -m pip install pyinstaller

### AND STEP 2
### > py -m pyinstaller --onefile TelegramGrab.py

### OR STEP 3
### Use: auto-py-to-exe
###		https://github.com/brentvollebregt/auto-py-to-exe
###		https://www.tomshardware.com/how-to/create-python-executable-applications
### MSVS: Menu -> View -> Other Windows -> Python Environments ... Packages (PyPl), pip -> install
### MSVS: Menu -> View -> Other Windows -> Python Environments ... Packages (PyPl), auto-py-to-exe -> install
### MSVS: Menu -> View -> Terminal
### > py -m pip -V
### > py -m pip install auto-py-to-exe
### Copy pyinstaller.exe to %PROGRAM_FILES\Python3\Scripts
### RUN
### %PROGRAM_FILES\Python3\Scripts\auto-py-to-exe.exe