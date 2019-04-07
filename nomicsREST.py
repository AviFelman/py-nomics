#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@author: avifelman
This program is meant for interfacing with the Nomics API, and delivering data on a per-need basis.

"""

# Import needed packages
import pandas as pd, numpy as np, datetime, requests
from pandas.io.json import json_normalize

# Create the nomicsREST API class, which will contain specific functions
class nomicsREST(object):
    # Initialize the class with API key and a URL (if Nomics changes it's url, or you're using a testnet otherwise leave blank)
    def __init__ (self, key=None, url=None):
        self.key = key
        if url:
            self.url = url
        else:
            self.url = 'https://api.nomics.com/v1/'

    # This is a function for pulling the JSON from the API call. It will be used in all further functions
    def request(self, action):
        response = requests.get(self.url + action)
        # If you haven't provided an API key, this will notify you
        if self.key is None:
            raise Exception('API key is empty')

        if response.status_code != 200:
            raise Exception("Error: " + str(response.status_code) + "\n" + response.text + "\nRequest: " + self.url + action)

        json = response.json()
        return json

    # Get JSON list of all current currencies
    def get_currencies(self):
        return self.request('currencies?key={}'.format(self.key))

    # List of all currencies with current prices
    def get_current_prices(self):
        return self.request('prices?key={}'.format(self.key))

    # General overview of all currencies & their current statistics
    def get_overview(self):
        return self.request('dashboard?key={}'.format(self.key))

    # Historic overall marketcap for all cryptocurrencies
    # Begins on Start Date at 4:00pm PT / 0:00 UTC. Format is: YYYY-MM-DD
    def get_overall_marketcap(self, start_date, end_date):
        return self.request('market-cap/history?key={}&start={}T00%3A00%3A00Z&end={}T00%3A00%3A00Z'.format(self.key, start_date, end_date))

    # Gets 1d candle history for specific coins over time
    def get_price_history(self, start_date, end_date, coin):
        return self.request('candles?key={}&interval=1d&currency={}&start={}T00%3A00%3A00Z&end={}T00%3A00%3A00Z'.format(self.key, coin, start_date, end_date))

    # Pulls a historic snapshot of all currencies and displays their pricing data. This works for
    def get_sparkline_data(self, start_date, end_date=None):
        if end_date is not None:
            return self.request('currencies/sparkline?key={}&start={}T00%3A00%3A00Z&end={}T00%3A00%3A00Z'.format(self.key, start_date, end_date))
        else:
            return self.request('currencies/sparkline?key={}&start={}T00%3A00%3A00Z&end={}T00%3A00%3A00Z'.format(self.key, start_date, start_date))

    # Only works after January 2018
    def get_supply_data(self, start_date, end_date=None):
        if end_date is not None:
            return self.request('supplies/interval?key={}&start={}T00%3A00%3A00Z&end={}T00%3A00%3A00Z'.format(self.key, start_date, end_date))
        else:
            return self.request('supplies/interval?key={}&start={}T00%3A00%3A00Z&end={}T00%3A00%3A00Z'.format(self.key, start_date, start_date))

    # Get the marketcap ranking of assets on any given day in history
    # This returns the marketcap at the beginning of the start date mentioned UTC time. For example, inputting '2019-01-01' will pull the marketcap of a specific coin @ 0:00 UTC on January 1st, 2019

    def get_marketcap_data(self, start_date, coin=None):
        prices = json_normalize(self.get_sparkline_data(start_date))
        supply = json_normalize(self.get_supply_data(start_date))

        # This line extracts prices from the original dataframe to allow for easy manipulation
        prices['prices'] = [item['prices'][0] for item in self.get_sparkline_data(start_date)]

        # Merge the dataframes to make multiplication easy
        market_cap_data = prices.merge(supply, how='left', on='currency')[['currency', 'prices', 'close_available']]

        ## The only way to get market cap from Nomics data is to multiply supply and price together
        market_cap_data['market_cap'] = pd.to_numeric(market_cap_data['prices']) * pd.to_numeric(market_cap_data['close_available'])

        ## This piece of code is to pull out a specific coin's data if you only wanted 1
        if(coin is not None):
            return market_cap_data[market_cap_data['currency'] == coin]
        else:
            return market_cap_data


    # price_to_dataframe allows you to input a list of coins and get their historic prices at the open of the days you've specificied. The specific column input is there to allow you to remove the returns column or the prices column if need be.
    
    def price_to_dataframe(self, coin_list, start_date, end_date, specific_column=None):
        df_final = pd.DataFrame()
        for coin in coin_list:
            coin_open = str(coin) + 'open'
            coin_returns = str(coin) + 'returns'
            df_temp = pd.DataFrame().from_dict(self.get_price_history(start_date, end_date, coin))[['close', 'timestamp']]
            df_temp.rename(columns={'open': coin_close}, inplace=True)
            df_temp[coin_returns] = pd.to_numeric(df_temp[coin_open]).pct_change(1)

            if(specific_column =='returns'):
                df_temp.drop([coin_close], axis=1, inplace=True)
            if(specific_column == 'prices'):
                df_temp.drop([coin_returns], axis=1, inplace=True)

            if(coin == coin_list[0]):
                df_final = df_temp
            else:
                df_final = df_final.merge(df_temp, how='left', on='timestamp')
        return df_final.set_index('timestamp').fillna(method='ffill')



if __name__ == '__main__':
    '''
    Initialize the nomicsREST API, and start pulling data!
    '''

    nr = nomicsREST('')

    #coin_list = ["BTC","ETH","XRP","LTC","EOS","BCH","BNB","XLM","USDT","TRX","ADA",
    #             "BSV","XMR","DASH","MKR","ONT","NEO","ETC","XTZ","XEM","ZEC",
    #             "VET","WAVES","USDC","DOGE","BAT","BTG","QTUM","OMG","TUSD",
    #             "DCR","LSK","REP","LINK","ZIL","HOT","ZRX","RVN","ICX","DGB",
    #             "ENJ","STEEM","BCN","BTS","BTT","NANO","BCD","HT","AE","PAX",
    #             "BTM","KMD","XVG","NPXS","IOST","SC","THETA",
    #             "MXM","DAI","STRAT","SNT","GNT","PAI","PPT","ARDR",
    #             "ARK","GXC","R","CNX","GUSD","FCT","MAID","WAX","HC","LOOM",
    #             "ETN","DGTX","QASH","WTC","MANA","MCO","LRC","ELF","THR","XZC",
    #             "PIVX","NEXO","KNC","AION","POWR","WAN","ZEN","ETP","ELA","NAS",
    #             "ODE","BNT","DENT","STORJ","NULS","KIN","GRS","DGD","RDD","POLY",
    #             "BIX","MONA","QNT","QKC","SYS","ENG"]

    #prices = nr.price_to_dataframe(['XVG'], '2019-03-29', '2019-03-29', 'prices')
