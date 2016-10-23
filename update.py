import requests
from bs4 import BeautifulSoup
from datetime import timedelta, date
import numpy as np
import pandas  as pd
import pickle

def getTodayIndex(year, month, day):
	keys = {'syear': year, 'smonth': month, 'sday': day}
	r = requests.post("http://www.taifex.com.tw/chinese/3/3_1_1.asp", data = keys)
	soup = BeautifulSoup(r.text, "lxml")
	soup_data = soup.select('table')[2].select('table')[1].select('td')
	return soup_data

def getData(start):

	data = {'date':[], 'open':[], 'high':[], 'low':[], 'settlement':[], 'volume':[],
	 'close':[], 'open_int':[], 'close_best_bid':[], 'close_best_ask':[], 'weekday':[]}

	day = start
	today = date.today()
	span = ((today - day).days) + 1
	interval = timedelta(days=1)

	def addtoData(column, index):
		try:
			data[column].append(int(soup_data[index].text))
		except Exception as e:
			print (e)
			data[column].append(np.nan)

	for _ in range(span):
		try:
			soup_data = getTodayIndex(day.year, day.month, day.day)
			data['date'].append(day)
			data['weekday'].append(day.isoweekday())
			addtoData('open', 2)
			addtoData('high', 3)
			addtoData('low', 4)
			addtoData('settlement', 5)
			addtoData('volume', 8)
			addtoData('close', 9)
			addtoData('open_int', 10)
			addtoData('close_best_bid', 11)
			addtoData('close_best_ask', 12)
			print ('%d/%d/%d is loaded' %(day.year, day.month, day.day))
		except Exception as e:
			#print (e)
			print ('stock market was not open on %d/%d/%d' %(day.year, day.month, day.day))
		day += interval

	df = pd.DataFrame(data)

	return df

pickle_in = open('txfuture_updated.pickle', 'rb')

df = pickle.load(pickle_in)

start_day = df.index[-1] + timedelta(days=1)

df_append = getData(start_day)

df_append.set_index('date', inplace=True)

df = pd.concat([df,df_append])

df = df[['weekday', 'open', 'high', 'low', 'settlement', 'volume',
	 'close', 'open_int', 'close_best_bid', 'close_best_ask']]

print (df.tail())

with open('txfuture_updated.pickle','wb') as f:
	pickle.dump(df, f)