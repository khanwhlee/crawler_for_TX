import requests
from bs4 import BeautifulSoup
from datetime import timedelta, datetime
import numpy as np
import pandas  as pd
import logging
import json


def daily_crawler(date):

    IS_ON = True
    d_ = date.strftime("%Y%m%d")

    TODAY_DATA = False

    keys = {'syear': date.year, 'smonth': date.month, 'sday': date.day}
    try:
        r = requests.post("http://www.taifex.com.tw/chinese/3/3_1_1.asp", data = keys)
        soup = BeautifulSoup(r.text, "lxml")
        soup = soup.select('table')[2].select('table')[1].select('td')
        o = int(soup[2].text)
        h = int(soup[3].text)
        l = int(soup[4].text)
        try:
            s = int(soup[11].text)
        except:
            s = int(soup[5].text)
        v = int(soup[10].text)
        c = int(soup[5].text)
        
        r = requests.get("http://www.twse.com.tw/indicesReport/MI_5MINS_HIST?response=json&date="+d_)
        r = BeautifulSoup(r.text, "lxml").select('p')[0].text
        r = json.loads(r)
        for data in r["data"]:
            if data[0].replace("/","")[-4:] == d_[-4:]:
                so = float(data[1].replace(",",""))
                sh = float(data[2].replace(",",""))
                sl = float(data[3].replace(",",""))
                sc = float(data[4].replace(",",""))

        TODAY_DATA = [date, o, h, l, s, v, c, so, sh, sl, sc]
    except Exception as e:
        print e
        IS_ON = False
        logger_crawler.info("Stock is not open on {}.".format(date))

    if IS_ON:
        try:
            r = requests.get("http://www.tse.com.tw/exchangeReport/FMTQIK?response=json&date="+d_)
            r = BeautifulSoup(r.text, "lxml").select('p')[0].text
            r = json.loads(r)
            for data in r["data"]:
                if data[0].replace("/","")[-4:] == d_[-4:]:
                    sv = int(data[2].replace(",",""))
                    TODAY_DATA.append(sv)
        except Exception as e:
            if IS_ON:
                logger_crawler.error("[insert_volume][{}] {}".format(date, e))

        try:
            r = requests.post("http://www.taifex.com.tw/chinese/3/7_12_3.asp", data = keys)
            r = BeautifulSoup(r.text, "lxml")

            cols_f = "f_long, f_long_contract_V, f_short, f_short_contract_V, f_acu_net, f_acu_contract_V".replace(" ","").split(",")
            cols_i = "i_long, i_long_contract_V, i_short, i_short_contract_V, i_acu_net, i_acu_contract_V".replace(" ","").split(",")
            cols_s = "d_long, d_long_contract_V, d_short, d_short_contract_V, d_acu_net, d_acu_contract_V".replace(" ","").split(",")

            for n in [5,4,3]:
                r_ = r.select('table')[2].select('tr')[1].select('table')[0].select('tr')[n].select('td')
                if n == 3:
                    l = float(r_[3].text.replace(",",""))
                    lv = float(r_[4].text.replace(",",""))
                    s = float(r_[5].text.replace(",",""))
                    sv = float(r_[6].text.replace(",",""))
                    a = float(r_[13].text.replace(",",""))
                    av = float(r_[14].text.replace(",",""))
                else:
                    l = float(r_[1].text.replace(",",""))
                    lv = float(r_[2].text.replace(",",""))
                    s = float(r_[3].text.replace(",",""))
                    sv = float(r_[4].text.replace(",",""))
                    a = float(r_[11].text.replace(",",""))
                    av = float(r_[12].text.replace(",",""))
                TODAY_DATA += [l,lv,s,sv,a,av]

        except Exception as e:
            if IS_ON:
                logger_crawler.error("[insert_3_position][{}] {}".format(date, e))
        try:
            r = requests.get("http://www.tse.com.tw/fund/BFI82U?response=json&dayDate="+d_)
            r = BeautifulSoup(r.text, "lxml").select("p")[0].text
            r = json.loads(r)
            r = r["data"]

            dsb = int(r[0][1].replace(",",""))
            dss = int(r[0][2].replace(",",""))
            dhb = int(r[1][1].replace(",",""))
            dhs = int(r[1][2].replace(",",""))
            ib = int(r[2][1].replace(",",""))
            is_ = int(r[2][2].replace(",",""))
            fb = int(r[3][1].replace(",",""))
            fs = int(r[3][2].replace(",",""))

            value = [dsb, dss, dhb, dhs, ib, is_, fb, fs]
            cols = ["d_self_buy","d_self_sell","d_hedge_buy","d_hedge_sell","i_buy","i_sell","f_buy","f_sell"]
            TODAY_DATA += value

        except Exception as e:
            if IS_ON:
                logger_crawler.error("[insert_3_BS][{}] {}".format(date, e))
    return TODAY_DATA


if __name__ == "__main__":
    log_path = "my.log"
    logging.basicConfig(level=logging.INFO, format='[%(name)s][%(levelname)s] %(asctime)s - %(message)s', filename=log_path)
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    formatter = logging.Formatter('[%(levelname)-5s] %(name)-10s: %(message)s')
    console.setFormatter(formatter)
    logging.getLogger('').addHandler(console)
    logger_crawler = logging.getLogger('CRAWLER')

    df = pd.read_csv("dailyinfo.csv")
    cols = df.columns
    df.set_index('date_', inplace=True)

    LAST_UPDATE = datetime.strptime(df.index[-1], '%Y-%m-%d')
    LAST_UPDATE_ = LAST_UPDATE + timedelta(days=1)
    TODAY = datetime.today().strftime("%Y-%m-%d")

    logger_crawler.info("LAST UPDATE: {}".format(LAST_UPDATE))
    logger_crawler.info("TODAY IS: {}".format(TODAY))

    UPDATE_DATA = []

    for date in pd.date_range(start=LAST_UPDATE_, end=TODAY):
        date = date.date()
        DATA = daily_crawler(date)
        if DATA:
            UPDATE_DATA.append(DATA)

    df_crawl = pd.DataFrame(UPDATE_DATA, columns=cols)
    df_crawl.set_index('date_', inplace=True)
    df = df.append(df_crawl)
    df.to_csv('dailyinfo.csv')




