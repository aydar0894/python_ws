from flask import Flask
from flask import request
from flask_cors import CORS
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

class MultiplierCorrelationRetriever:
    def __init__(self,
                 horizon,
                 currencies_list='all',
                 return_frequency='daily'):
        self.mongo_c          = None
        self.db_name          = 'darqube_db'
        self._mongo_connect()
        self.return_frequency = "%s_data" % return_frequency
        self.db               = self.mongo_c[self.db_name]
        self._mongo_connect()
        self.horizon          = horizon
        self.currencies_list  = currencies_list
        if currencies_list == 'all':
            self.currencies_list = [[x['Ccy'] for x in self.db.find({},{'Ccy': 1, '_id': 0})]
        
        
    def retrieve_data(self):
        currencies_list  = deque(self.currencies_list)
        pairs_multiplier_correlation = {}
        while len(currencies_list) > 1:
            benchmark_currency  = currencies_list.popleft()
            pair = self._retrieve_multiplier_correlation(benchmark_currency, currencies_list)
            pairs_multiplier_correlation = {**pairs_multiplier_correlation, **pair}
        return pairs_multiplier_correlation
        
    def _mongo_connect(self):
        if self.mongo_c == None:
            self.mongo_c = MongoClient('localhost',
                    authSource=self.db_name)
        return self.mongo_c
        
        
    def _retrieve_collection(self, filter_params):
        collection = self.db[self.return_frequency]
        if not collection:
            raise Exception('collection not found')
        return collection.find_one(filter_params)

    
    def _retrieve_multiplier_correlation(self, benchmark, coins):
        df_data = self._retrieve_collection({'Ccy': benchmark})
        df_data = df_data['m_and_c_matrix'][str(self.horizon)]
        df_data = [x for x in df_data if x['ccy'] in coins]
        df_data = {"%s/%s" %(benchmark, obj['ccy']): {'multiplier': obj['multiplier'], 'correlation': obj['correlation']} for obj in df_data}
        return df_data




app = Flask(__name__)
CORS(app)

@app.route('/', methods=['POST'])
def index():
    # pprint(request.form)
    horizon          = int(request.form['horizon'])
    currencies_list  = request.form['currencies_list'].split(',')
    return_frequency = request.form['return_frequency']
    # pprint(currencies_list)
    if not return_frequency:
        return_frequency = 'daily'
    data = MultiplierCorrelationRetriever(horizon=horizon,
                                    currencies_list=currencies_list,
                                    return_frequency=return_frequency
                                    ).retrieve_data()
    response = app.response_class(
        response=json.dumps(data),
        status=200,
        mimetype='application/json'
    )
    return response
