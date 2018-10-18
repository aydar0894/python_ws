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

MONGODB_NAME       = 'bitcoin'
MONGODB_HOST       = 'localhost'
MONGODB_COLLECTIONS        = ['daily_data_test', 'hourly_data_test']
MONGODB_DEFAULT_COLLECTION = 'daily_data_test'

class MongoConnector:
    def __init__(self,
                host=MONGODB_HOST,
                db_name=MONGODB_NAME):
        self._mongo_connection = MongoClient(host=host,
                                             authSource=db_name)
        self.db = self._mongo_connection[db_name]
        
        
    def find_one(self, 
                filter_params,
                collection=MONGODB_DEFAULT_COLLECTION):
        collection = self.db[collection]
        if not collection:
            raise Exception('collection not found')
        return collection.find_one(filter_params)

    def find(self, 
             filter_params, 
             colleciton=MONGODB_DEFAULT_COLLECTION):
        collection = self.db[colleciton]
        if not collection:
            raise Exception('collection not found')
        return collection.find({},filter_params)
    
    def get_db(self):
        return self.db
        

class MultiplierCorrelationRetriever:
    def __init__(self,
                 horizon,
                 currencies_list=['all'],
                 return_frequency='daily',
                 db_name=MONGODB_NAME):
        self.connector        = MongoConnector(host=MONGODB_HOST,
                                               db_name=db_name)
        self.return_frequency = "%s_data_test" % return_frequency
        self.horizon          = horizon
        self.currencies_list  = currencies_list
        if currencies_list == ['all']:
            filter_params         = {'Ccy': 1, '_id': 0}
            currencies_collection = self.connector.find(filter_params,
                                                        self.return_frequency
                                                        ).limit(50)
            self.currencies_list  = [x['Ccy'] for x in currencies_collection]
        
        
    def retrieve_all_pairs(self):
        currencies_list  = deque(self.currencies_list)
        pairs_multiplier_correlation = {}
        while len(currencies_list) > 1:
            benchmark_currency  = currencies_list.popleft()
            pair = self._retrieve_multiplier_correlation(benchmark_currency, currencies_list)
            pairs_multiplier_correlation = {**pairs_multiplier_correlation, **pair}
        return pairs_multiplier_correlation

    def retrieve_pairs_for_benchmark(self, benchmark_currency):
        return self._retrieve_multiplier_correlation(benchmark_currency,
                                                     self.currencies_list)

    
    def _retrieve_multiplier_correlation(self, benchmark, coins):
        df_data = self.connector.find_one({'Ccy': benchmark}, 
                                          self.return_frequency)
        try:
            df_data = df_data['m_and_c_matrix'][str(self.horizon)]
        except:
            return {}
        df_data = [x for x in df_data if x['ccy'] in coins]
        df_data = {"%s/%s" %(benchmark, obj['ccy']): {'multiplier': obj['multiplier'], 'correlation': obj['correlation']} for obj in df_data}
        return df_data


class CurrenciesListRetriver:
    def __init__(self,
             host=MONGODB_HOST,
             db_name=MONGODB_NAME,
             collection=MONGODB_DEFAULT_COLLECTION):
        self.connector  = MongoConnector(host=host,
                                         db_name=db_name)

    def call(self, filter_params):
        return [x['Ccy'] for x in self.connector.find(filter_params)]


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
    data = MultiplierCorrelationRetriever(horizon=horizon,
                                    currencies_list=currencies_list,
                                    return_frequency=return_frequency,
                                    db_name=MONGODB_NAME
                                    ).retrieve_all_pairs()
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
    filter_params   = {'Ccy': 1, '_id': 0}
    connector       = CurrenciesListRetriver()
    data            = list(connector.call(filter_params))
    response = app.response_class(
        response=json.dumps(data),
        status=200,
        mimetype='application/json'
    )
    return response


# TODO: create api selected selected benchmark pairs

# will callculate pairs benchmark and list of given coins

@app.route('/currency_pairs', methods=['GET'])
@cross_origin()
def currency_pairs():
    benchmark        = request.form['benchmark']
    horizon          = int(request.form['horizon'])
    currencies_list  = request.form['currencies_list'].split(',')
    return_frequency = request.form['return_frequency']
    data             = MultiplierCorrelationRetriever(horizon=horizon,
                                    currencies_list=currencies_list,
                                    return_frequency=return_frequency,
                                    db_name=MONGODB_NAME
                                    ).retrieve_pairs_for_benchmark(benchmark)
    response = app.response_class(
        response=json.dumps(data),
        status=200,
        mimetype='application/json'
    )
    return response

