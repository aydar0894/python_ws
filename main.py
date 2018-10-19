from flask import Flask
from flask import request
from flask_cors import CORS, cross_origin
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

MONGO_DB_NAME       = 'bitcoin'
MONGO_HOST       = 'localhost'
MONGO_COLLECTIONS        = ['daily_data_test', 'hourly_data_test']
MONGO_DB_DEFAULT_COLLECTION = 'daily_data_test'

class RequestFrequency(Enum):
    DAILY  = 0
    HOURLY = 1

class HourlyTimeIntervals(Enum):
    A_DAY       = 1
    FIVE_DAYS   = 5
    WEEK        = 7
    TEN_DAYS    = 10
    TWO_WEEKS   = 14

class DailyTimeIntervals(Enum):
    A_MONTH      = 1
    THREE_MONTHS = 3
    HALF_YEAR    = 6
    NINE_MONTHS  = 9
    A_YEAR       = 12

class MongoConnector:
    def __init__(self,
                host=MONGO_HOST,
                db_name=MONGO_DB_NAME):
        self._mongo_connection = MongoClient(host=host,
                                             authSource=db_name)
        self.db = self._mongo_connection[db_name]
        
        
    def find_one(self, 
                *params,
                collection=MONGO_DB_DEFAULT_COLLECTION):
        collection = self.db[collection]
        if not collection:
            raise Exception('collection not found')
        return collection.find_one(*params)

    def find(self, 
             *params,
             colleciton=MONGO_DB_DEFAULT_COLLECTION):
        collection = self.db[colleciton]
        if not collection:
            raise Exception('collection not found')
        return collection.find(*params)
        

class MultiplierCorrelationCalculator:
    FREQUENCY_LIST        = RequestFrequency.__members__.keys()
    HOURLY_TIME_INTERVALS = list(map(lambda x: x.value, HourlyTimeIntervals.__members__.values()))
    DAILY_TIME_INTERVALS  = list(map(lambda x: x.value, DailyTimeIntervals.__members__.values()))
    TIME_INTERVALS_DICT   = {
        'hourly': HOURLY_TIME_INTERVALS,
        'daily': DAILY_TIME_INTERVALS
    }

    TIME_INTERVALS_CALCULATOR = {
        'daily' : lambda x: x * 30,
        'hourly': lambda x: x * 24
    }

    def __init__(self,
                 horizon,
                 currencies_list=['all'],
                 return_frequency='daily',
                 db_name=MONGO_DB_NAME):
        if return_frequency.upper() not in self.FREQUENCY_LIST:
            raise Exception('Only [daily, hourly] values supports for return_frequency parameter yet...')
        if horizon not in self.TIME_INTERVALS_DICT[return_frequency]:
            msg = 'Only %s values supports for %s collection' % (','.join(self.time_points), 
                                                                 return_frequency)
            raise Exception(msg)
        bounds_normalizer     = self.TIME_INTERVALS_CALCULATOR[return_frequency]
        self.horizon          = bounds_normalizer(horizon)
        self.collection       = "%s_data_test" % return_frequency
        mongo_c               = MongoClient(host=MONGO_HOST,
                                            authSource=db_name)
        self.connector        = mongo_c[db_name][self.collection]
        self.currencies_list  = currencies_list
        if currencies_list == ['all']:
            params         = {}, {'Ccy': 1, '_id': 0}
            currencies_collection = self.connector.find(*params).limit(15)
            self.currencies_list  = [x['Ccy'] for x in currencies_collection]
    
    def calculate_pairs(self):
        df_prices = []
        params    = {'Ccy': {'$in': self.currencies_list}}, {'history.close': 1, 'Ccy': 1}
        for data in self.connector.find(*params):
            da_data = [history['close'] for history in list(reversed(data['history']))[1:self.horizon]]
            df_prices.append(da_data)
        df_prices = pd.DataFrame(list(zip(*df_prices)), columns=self.currencies_list)
        df_returns=df_prices / df_prices.shift(1) - 1
        df_correl=df_returns.corr()
        corel = pd.DataFrame.to_json(df_correl)
        df_beta=df_returns.cov()/df_returns.var()
        beta = pd.DataFrame.to_json(df_beta)
        return {'multiplier': beta, 'correlation': corel }
    
    # def calculate_pairs_for_benchmark(self,  benchmark, coins):
    #     df_data = self.connector.find_one({'Ccy': benchmark})
    #     try:
    #         df_data = df_data['m_and_c_matrix'][str(self.horizon)]
    #     except:
    #         return {}
    #     df_data = [x for x in df_data if x['ccy'] in coins]
    #     df_data = {"%s/%s" %(benchmark, obj['ccy']): {'multiplier': obj['multiplier'], 'correlation': obj['correlation']} for obj in df_data}
    #     return df_data


app = Flask(__name__)
CORS(app)
# "origins": ('*',)
@app.route('/matrix', methods=['POST'])
@cross_origin()
def matrix():
    # pprint(request.form)
    horizon          = int(request.form['horizon'])
    currencies_list  = request.form['currencies_list'].split(',')
    return_frequency = request.form['return_frequency']
    print(horizon)
    print(currencies_list)
    print(return_frequency)
    p_frequency = ['dayily', 'hourly']
    if return_frequency not in p_frequency:
        return_frequency = 'daily'
    data = MultiplierCorrelationCalculator(horizon=horizon,
                                    currencies_list=currencies_list,
                                    return_frequency=return_frequency,
                                    db_name=MONGO_DB_NAME
                                    ).calculate_pairs()
    response = app.response_class(
        response=json.dumps(data),
        status=200,
        mimetype='application/json'
    )
    return response

# "origins": ('*',)
@app.route('/currencies', methods=['GET'])
@cross_origin()
def currencies():
    selected_params   = {}, {'Ccy': 1, '_id': 0}
    connector  = MongoClient(host=MONGO_HOST,
                             authSource=MONGO_DB_NAME)
    collection = connector[MONGO_DB_NAME][MONGO_DB_DEFAULT_COLLECTION]
    data       = list([x['Ccy'] for x in collection.find(*selected_params)])
    response = app.response_class(
        response=json.dumps(data),
        status=200,
        mimetype='application/json'
    )
    return response


# TODO: create api selected selected benchmark pairs

# will callculate pairs benchmark and list of given coins

# @app.route('/currency_pairs', methods=['GET'])
# @cross_origin()
# def currency_pairs():
#     benchmark        = request.form['benchmark']
#     horizon          = int(request.form['horizon'])
#     currencies_list  = request.form['currencies_list'].split(',')
#     return_frequency = request.form['return_frequency']
#     data             = MultiplierCorrelationRetriever(horizon=horizon,
#                                     currencies_list=currencies_list,
#                                     return_frequency=return_frequency,
#                                     db_name=MONGO_DB_NAME
#                                     ).retrieve_pairs_for_benchmark(benchmark)
#     response = app.response_class(
#         response=json.dumps(data),
#         status=200,
#         mimetype='application/json'
#     )
#     return response

