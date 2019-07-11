#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@author: avifelman
This program is meant for interfacing with the Nomics API, and delivering data on a per-need basis.
This program offers additional functions that create extra constructed endpoints

"""

# Import needed packages
import pandas as pd, numpy as np, datetime, requests
from pandas.io.json import json_normalize

# Create the nomicsREST API class, which will contain specific functions for pulling data
class nomicsREST(object):

    # Initialize the class with API key and a URL
    # The URL is included just in the case that Nomics changes it's url, or you're using a testnet
    def __init__ (self, key=None, url=None):
        self.key = key
        if url:
            self.url = url
        else:
            self.url = 'https://api.nomics.com/v1/'

    # This is a function for pulling the JSON data from Nomics. It will be used in all further functions
    def request(self, action):
        response = requests.get(self.url + action)

        # Catching errors
        if self.key is None:
            raise Exception('API key is empty')
        if response.status_code != 200:
            raise Exception("Error: " + str(response.status_code) + "\n" + response.text + "\nRequest: " + self.url + action)

        json = response.json()
        return json

    # Get list of all currencies tracked by nomics (Return: JSON)
    def get_currencies(self):
        return self.request('currencies?key={}'.format(self.key))

    # List of all currencies with current prices (Return: JSON)
    def get_current_prices(self):
        return self.request('prices?key={}'.format(self.key))

    # Dashboard view of all currencies (Return: JSON)
    def get_dashboard(self):
        return self.request('dashboard?key={}'.format(self.key))

    # Historic total market capitalization (Return: JSON)
    # Begins on Start Date at 4:00pm PT / 0:00 UTC. Format for date is: YYYY-MM-DD
    def get_overall_marketcap(self, start_date, end_date):
        return self.request('market-cap/history?key={}&start={}T00%3A00%3A00Z&end={}T00%3A00%3A00Z'.format(self.key, start_date, end_date))

    # Daily candles for one currency at a time (Return: JSON)
    # Begins on Start Date at 4:00pm PT / 0:00 UTC. Format for date is: YYYY-MM-DD
    def get_price_history(self, start_date, end_date, coin):
        return self.request('candles?key={}&interval=1d&currency={}&start={}T00%3A00%3A00Z&end={}T00%3A00%3A00Z'.format(self.key, coin, start_date, end_date))

    # Pulls all currencies and their prices over a set period of time (Return: JSON)
    def get_sparkline_data(self, start_date, end_date=None):
        if end_date is None:
            end_date = start_date
        return self.request('currencies/sparkline?key={}&start={}T00%3A00%3A00Z&end={}T00%3A00%3A00Z'.format(self.key, start_date, end_date))

    # Note: Only works after January 2018 (Return: JSON)
    # Pulls all currencies and their supply data over a set period of time
    def get_supply_data(self, start_date, end_date=None):
        if end_date is None:
            end_date = start_date
        return self.request('supplies/interval?key={}&start={}T00%3A00%3A00Z&end={}T00%3A00%3A00Z'.format(self.key, start_date, end_date))

    # Pull the ranking of cryptocurrencies by marketcap at any given day in history (Return: DataFrame)
    # Inputting '2019-01-01' will pull the marketcap of a specific coin @ 0:00 UTC on January 1st, 2019
    def get_marketcap_snapshot(self, start_date, coin=None):
        # Pull both the supply and price data at a specific point in time
        prices = json_normalize(self.get_sparkline_data(start_date))
        supply = json_normalize(self.get_supply_data(start_date))

        # This line extracts prices from the original dataframe
        prices['prices'] = [item['prices'][0] for item in self.get_sparkline_data(start_date)]
        # Merge the dataframes to make multiplication easy
        market_cap_data = prices.merge(supply, how='left', on='currency')[['currency', 'prices', 'close_available']]

        # The only way to get market cap from Nomics data is to multiply supply and price together
        market_cap_data['market_cap'] = pd.to_numeric(market_cap_data['prices']) * pd.to_numeric(market_cap_data['close_available'])

        #Clean the DataFrame
        market_cap_data.sort_values(['market_cap'], ascending=False, inplace=True)
        market_cap_data.reset_index(inplace=True, drop=True)

        # If coin is specific, only pull data for that specific coin
        if(coin is not None):
            return market_cap_data[market_cap_data['currency'] == coin]
        else:
            # Sort the data for ease of use
            return market_cap_data

    # Get the marketcap for a specific coin over a period of time (Return: DataFrame)
    def get_historic_marketcap(self, coin, start_date, end_date):
        df_final = pd.DataFrame()
        date_list = pd.date_range(start_date, end_date).tolist()
        market_cap = []
        for date1 in date_list:
            date2 = date1.date()
            df_temp = self.get_marketcap_data(date2, coin)
            df_temp.reset_index(inplace=True)
            market_cap.append(df_temp['market_cap'][0])
        df_final['date'] = date_list
        df_final['marketcap'] = market_cap
        return df_final


    # Function get_multiple_coin_prices allows you to input a list of coins and get their historic prices at open (Return: DataFrame)
    # The specific column input is there to allow you to remove the returns column or the prices column if need be.
    # Also delivers total & daily returns
    def get_multiple_coin_prices(self, coin_list, start_date, end_date, specific_column=None):
        df_final = pd.DataFrame()
        for coin in coin_list:
            coin_open = str(coin) + 'open'
            coin_returns = str(coin) + 'returns'
            coin_total_returns = str(coin) + 'total_returns'
            df_temp = pd.DataFrame().from_dict(self.get_price_history(start_date, end_date, coin))

            # Check if coin was successfully passed, if not, skip the coin
            if(df_temp.empty):
                print(coin + " is not in nomics dataset")
                continue

            df_temp = df_temp[['open', 'timestamp']]
            df_temp.rename(columns={'open': coin_open}, inplace=True)
            df_temp[coin_returns] = pd.to_numeric(df_temp[coin_open]).pct_change(1)
            df_temp[coin_total_returns] = (1 + df_temp[coin_returns]).cumprod() - 1

            ## Following statements allow you to pull specific columbs
            if(specific_column =='returns'):
                df_temp.drop([coin_open], axis=1, inplace=True)
            if(specific_column == 'prices'):
                df_temp.drop([coin_returns], [coin_total_returns], axis=1, inplace=True)

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
    coin_list = ['BTC', 'DCR'] #XRB to the moon
    prices = nr.get_multiple_coin_prices(coin_list, '2016-01-01', '2019-07-010')
    print(prices)
