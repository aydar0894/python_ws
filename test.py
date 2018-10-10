from enum import Enum
import copy
from collections import deque

class MultiplierCorellationCalculator(object):
    class RequestFrequency(Enum):
        DAILY  = 0
        HOURLY = 1  
    FREQUENCY_LIST = self.RequestFrequency.__members__.items()
    
    def __init__(start_time, 
                 end_time,
                 currencies_list, 
                 return_frequency='daily'):
        self.start_time = start_time
        self.end_time         = end_time
        self.currencies_list  = currencies_list
        self.return_frequency = return_frequency

    
    def calculate_aggregated_pairs(self):
        currencies_list  = deque(self.currencies_list)
        pairs_multiplier_corellation = {}
        while len(currencies_list) > 1:
            benchmark_currency  = currencies_list.popleft()
            for coin_currency in currencies_list:
                pair_tag = "/".join(x in list(benchmark_currency, coin_currency))
                multiplier, corellation = calculation_for_pair(benchmark_currency, coin_currency)
                pairs_multiplier_corellation[pair_tag] = { 'multiplier': multiplier, 
                                                           'correlation': corellation }
        return pairs_multiplier_corellation
        
        
    def calculation_for_pair(self, benchmark_tag, coin_tag):
        # --- read coin ---
        
        arr_PnL_benchmark, arr_PnL_coin = _calculate_timeseries()
        multiplier, corellation         = _calculate_multiplier_and_corellation(arr_PnL_benchmark, 
                                                                                arr_PnL_coin)
        return (multiplier, corellation)
    
    def _calculate_multiplier_and_corellation(self, arr_PnL_benchmark, arr_PnL_coin):
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
        return multiplier, corellation


    def calculate_corellation(self, arr_PnL_benchmark, arr_PnL_coin):
        
        return corellation   
        
    #-----------------------------------------#
    #          calculate return timeseries    # 
    #-----------------------------------------#
    def _calculate_timeseries(self):
        dt_previousTime    = copy.deepcopy(self.start_time)
        dt_currentTime     = copy.deepcopy(self.start_time)
        # add first interval
        dt_currentTime     = _increment_interval(dt_currentTime)

        df_coin = _retrive_currency_history(coin_ccy)
        df_benchmark = _retrive_currency_history(benchmark_ccy)

        arr_PnL_benchmark = np.array([])
        arr_PnL_coin       = np.array([])

        while (dt_currentTime <= dt_benchmark_endTime):
            # calculate return of benchmark in period [t-1, t]
            arr_PnL_benchmark = _calculate_PnL( df_benchmark, 
                                                dt_currentTime, 
                                                dt_previousTime)
            arr_PnL_coin      = _calculate_PnL( df_coin, 
                                                dt_currentTime, 
                                                dt_previousTime)
            # move to next timepoint
            dt_previousTime, dt_currentTime = _increment_interval(dt_previousTime, 
                                                                  dt_currentTime)
        return (arr_PnL_benchmark, arr_PnL_coin)


    def _calculate_PnL(self, df_data, dt_currentTime, dt_previousTime):
        # calculate return of strategy in period [t-1, t] (based on equity, i.e. MtM value of positions)
        PnL = df_data.loc[dt_currentTime]['close']  / \
                    df_data.loc[dt_previousTime]['close'] -1.0
        arr_PnL = np.append(arr_PnL, PnL)
        return arr_PnL
    
    def _increment_interval(self, *date_time_fields):
        if self.return_frequency == 'daily':
            return map(lambda dt: dt + datetime.timedelta(days=1), date_time_fields)
        elif self.return_frequency == 'hourly':
            return map(lambda dt: dt + datetime.timedelta(days=1), date_time_fields)
        else:
            print('ERROR. Need to implment other frequencies')
            assert(False)

    # --- connect and preprocess utilities for mongo collection --- 
    def _reconstruct_currency_date(self, cur):
        for cur_value, index in zip(cur['history'], range(len(cur['history']))):
            cur['history'][index]['date'] = datetime.fromtimestamp(cur_value['time'])
        return cur

    
    def _mongo_connect(self, db_name):
        mongo_c = MongoClient()
        db = mongo_c[db_name]
        if db:
            return db
        else:
            raise Exception("database or server not found")
        
        
    def _preprocess_collection(self, collection_name, filter_params):
        db = _mongo_connect('darqube_db')       
        collection = db[collection_name]    
        if not collection:
            raise Exception('collection not found')
        return self._reconstruct_currency_date(collection.find_one(filter_params))

    
    def _retrieve_currency_history(self, currency):
        collection = self._preprocess_collection('currencies_collection', {'Ccy': currency})
        df_data = pd.DataFrame(collection['history'])
        # this makes indexing via date faster
        df_data = df_data.set_index(['date'])         # index: string
        df_data.index = pd.to_datetime(df_benchmark.index) # index: datetime
        return df_data

        
        