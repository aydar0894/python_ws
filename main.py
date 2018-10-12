from flask import Flask
from flask import request
import numpy as np
import pandas as pd
from scipy.stats.stats import pearsonr # used to calculate correlation coefficient
from pymongo import MongoClient
import json
from datetime import datetime, timedelta
from pprint import pprint
from enum import Enum
import copy
from collections import deque
import time
import csv


class MultiplierCorellationCalculator:
    def __init__(self,
                 start_time,
                 currencies_list,
                 end_time=datetime.now(),
                 return_frequency='daily'):
        self.mongo_c = None
        self.start_time       = start_time
        self.end_time         = end_time
        self.currencies_list  = currencies_list
        self.return_frequency = return_frequency

    def get_collection_name(self):
        if self.return_frequency == 'hourly':
            return 'hourly_data'
        elif self.return_frequency == 'daily':
            return 'daily_data'

    def calculate_aggregated_pairs(self):
        currencies_list  = deque(self.currencies_list)
        self._fix_currencies_time_bounds()
        pairs_multiplier_correlation = {}
        while len(currencies_list) > 1:
            benchmark_currency  = currencies_list.popleft()
            for coin_currency in currencies_list:
                pair_tag =  "%s/%s" % (benchmark_currency, coin_currency)
                multiplier, correlation = self.calculation_for_pair(benchmark_currency, coin_currency)
                pairs_multiplier_correlation[pair_tag] = { 'multiplier': multiplier,
                                                           'correlation': correlation }
        return pairs_multiplier_correlation


    def _fix_currencies_time_bounds(self):
        minln, maxln = self._return_time_bounds()
        # pprint("Start time " + str(minln))
        # pprint("End time " + str(maxln))
        self.start_time = max(datetime.fromtimestamp(minln), datetime.fromtimestamp(self.start_time))
        self.end_time   = min(datetime.fromtimestamp(maxln), datetime.fromtimestamp(self.end_time))
        if self.return_frequency == 'daily':
            self.start_time = self.start_time.replace(hour=0,minute=0,second=0)
            self.end_time   = self.end_time.replace(hour=0,minute=0,second=0)
        if self.return_frequency == 'hourly':
            self.start_time = self.start_time.replace(hour=1,minute=0,second=0)
            self.end_time   = self.end_time.replace(hour=1,minute=0,second=0)


    def _return_time_bounds(self):
        db = self._mongo_connect('bitcoin')
        collection_data = db[self.get_collection_name()]
        minln = 0
        maxln = time.time()
        # pprint(self.currencies_list)
        for data in collection_data.find({ 'Ccy': { '$in' : self.currencies_list } }):
            try:
                hist = data["history"]
                history = list(map(lambda x: x['time'], hist))
                if min(history) > minln:
                    minln = min(history)
                if max(history) < maxln:
                    maxln = max(history)
            except:
                next
        return (minln, maxln)


    def calculation_for_pair(self, benchmark_ccy, coin_ccy):
        # --- read coin ---
        arr_PnL_benchmark, arr_PnL_coin = self._calculate_timeseries(benchmark_ccy, coin_ccy)
        pprint(arr_PnL_benchmark)
        # with open('%s.csv' % (benchmark_ccy,), 'w', newline='') as csvfile:
        #     spamwriter = csv.writer(csvfile, delimiter=';',
        #                             quotechar='|', quoting=csv.QUOTE_MINIMAL)
        #     spamwriter.writerow(arr_PnL_benchmark)
        # with open('%s.csv' % (coin_ccy,), 'w', newline='') as csvfile:
        #     spamwriter = csv.writer(csvfile, delimiter=';',
        #                             quotechar='|', quoting=csv.QUOTE_MINIMAL)
        #     spamwriter.writerow(arr_PnL_coin)
        multiplier, correlation         = self._calculate_multiplier_and_correlation(arr_PnL_benchmark,
                                                                                     arr_PnL_coin)
        return (multiplier, correlation)

    def _calculate_multiplier_and_correlation(self, arr_PnL_benchmark, arr_PnL_coin):
        # arr_x = arr_PnL_benchmark
        # arr_y = arr_PnL_coin
        #          calculate multiplier           #
        # least square regression (linear): y = alpha + beta*x
        linReg = np.polyfit(x=arr_PnL_benchmark, y=arr_PnL_coin, deg=1)
        alpha = linReg[1] # this is the y-intercept, not needed
        beta  = linReg[0] # this is the slope, which also is the multiplier
        multiplier = beta
        print("multiplier            : ", multiplier)
        #          calculate correlation          #
        correlation = pearsonr(arr_PnL_benchmark, arr_PnL_coin)
        print("correlation            :", correlation[0])
        return (multiplier, correlation[0])

    #-----------------------------------------#
    #          calculate return timeseries    #
    #-----------------------------------------#
    def _calculate_timeseries(self, benchmark_ccy, coin_ccy):
        dt_previousTime    = copy.deepcopy(self.start_time)
        dt_currentTime     = copy.deepcopy(self.start_time)
        # add first interval
        dt_currentTime,     = self._increment_interval(dt_currentTime)

        df_benchmark = self._retrieve_currency_history(benchmark_ccy)
        df_coin = self._retrieve_currency_history(coin_ccy)
        # pprint(type(df_benchmark.loc[dt_currentTime]['close'][1]))
        arr_PnL_benchmark  = np.array([])
        arr_PnL_coin       = np.array([])
        # pprint(df_benchmark)
        # pprint("Current time %s" % (dt_currentTime,))
        while (dt_currentTime <= self.end_time):
            # calculate return of benchmark in period [t-1, t]
            arr_PnL_benchmark = self._calculate_PnL(arr_PnL_benchmark,
                                                df_benchmark,
                                                dt_currentTime,
                                                dt_previousTime)
            arr_PnL_coin      = self._calculate_PnL(arr_PnL_coin,
                                                df_coin,
                                                dt_currentTime,
                                                dt_previousTime)

            # move to next timepoint
            dt_previousTime, dt_currentTime = self._increment_interval(dt_previousTime,
                                                                       dt_currentTime)

        return (arr_PnL_benchmark, arr_PnL_coin)


    def _calculate_PnL(self, arr_PnL, df_data, dt_currentTime, dt_previousTime):
        # calculate return of strategy in period [t-1, t] (based on equity, i.e. MtM value of positions)
        PnL = df_data.loc[dt_currentTime]['close'][1] / df_data.loc[dt_previousTime]['close'][1] -1.0

        arr_PnL = np.append(arr_PnL, PnL)
        return arr_PnL

    def _increment_interval(self, *date_time_fields):
        if self.return_frequency == 'daily':
            return map(lambda dt: dt + timedelta(days=1), date_time_fields)
        elif self.return_frequency == 'hourly':
            return map(lambda dt: dt + timedelta(hours=1), date_time_fields)
        else:
            print('ERROR. Need to implment other frequencies')
            assert(False)

    # --- connect and preprocess utilities for mongo collection ---
    def _reconstruct_currency_date(self, cur):
        frmt = "{:%Y-%m-%d}"
        if self.return_frequency == 'hourly':
            frmt = "{:%Y-%m-%d %H:%M:%S}"
        for cur_value, index in zip(cur['history'], range(len(cur['history']))):
            #  cur['history'][index]['date'] = datetime.fromtimestamp(cur_value['time'])
            cur['history'][index]['date'] = frmt.format(datetime.fromtimestamp(cur_value['time']))
        return cur


    def _mongo_connect(self, db_name):
        if self.mongo_c == None:
            self.mongo_c = MongoClient('localhost',
                                authSource='bitcoin')
        db = self.mongo_c[db_name]
        if db:
            return db
        else:
            raise Exception("database or server not found")


    def _preprocess_collection(self, filter_params):
        db = self._mongo_connect('bitcoin')
        collection = db[self.get_collection_name()]
        if not collection:
            raise Exception('collection not found')
        return self._reconstruct_currency_date(collection.find_one(filter_params))


    def _retrieve_currency_history(self, currency):
        collection = self._preprocess_collection({'Ccy': currency})
        df_data = pd.DataFrame(collection['history'])
        # this makes indexing via date faster
        df_data = df_data.set_index(['date'])         # index: string
        df_data.index = pd.to_datetime(df_data.index)
        # pprint(df_data)
        return df_data





app = Flask(__name__)

@app.route('/', methods=['POST'])
def index():
    # pprint(request.form)
    frmt = "%Y-%m-%d %H:%M:%S"
    start_time = int(request.form['start_time'])
    end_time   = int(request.form['end_time'])
    currencies_list  = request.form['currencies_list'].split(',')
    return_frequency = request.form['return_frequency']
    # pprint(currencies_list)
    if not return_frequency:
        return_frequency = 'daily'
    data = MultiplierCorellationCalculator(start_time=start_time,
                                    end_time=end_time,
                                    currencies_list=currencies_list,
                                    return_frequency=return_frequency
                                    ).calculate_aggregated_pairs()
    response = app.response_class(
        response=json.dumps(data),
        status=200,
        mimetype='application/json'
    )
    return response
