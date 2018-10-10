#

# input variables

#input parameter
dt_benchmark_startTime    = datetime.strptime("2018-01-01 00:00:00", "%Y-%m-%d %H:%M:%S")

#always current time
dt_benchmark_endTime      = datetime.strptime("2018-01-31 23:00:00", "%Y-%m-%d %H:%M:%S")

#input parameter
ReturnFrequency = "daily"

#list of currencies

#END input variables
    
dt_currentTime = dt_benchmark_startTime

# add first interval
dt_previousTime = dt_currentTime
if ReturnFrequency == "hourly":
    dt_currentTime += datetime.timedelta(hours=1)
elif ReturnFrequency == "daily":
    dt_currentTime += datetime.timedelta(days=1)
else:
    print('ERROR. Need to implment other frequencies')
    assert(False)


arr_PnL_benchmark = np.array([])
arr_PnL_coin       = np.array([])
   
#-----------------------------------------#
#          calculate return timeseries    #   
#-----------------------------------------#

while (dt_currentTime <= dt_benchmark_endTime):
    # calculate return of benchmark in period [t-1, t]
    PnL_benchmark = df_benchmark.loc[dt_currentTime]['close'] / \
                    df_benchmark.loc[dt_previousTime]['close'] -1.0
    arr_PnL_benchmark = np.append(arr_PnL_benchmark, PnL_benchmark)
    
    # calculate return of strategy in period [t-1, t] (based on equity, i.e. MtM value of positions)
    PnL_coin = df_coin.loc[dt_currentTime]['close']  / \
               df_coin.loc[dt_previousTime]['close'] -1.0
    arr_PnL_coin = np.append(arr_PnL_coin, PnL_coin)
     
    # move to next timepoint
    if ReturnFrequency == "hourly":
        dt_previousTime += datetime.timedelta(hours=1)
        dt_currentTime  += datetime.timedelta(hours=1)
    elif ReturnFrequency == "daily":
        dt_previousTime += datetime.timedelta(days=1)
        dt_currentTime += datetime.timedelta(days=1)
    else:
        print('ERROR. Need to implment other frequencies')
        assert(False)

#-----------------------------------------#
#          calculate multiplier           #   
#-----------------------------------------#
arr_x = arr_PnL_benchmark
arr_y = arr_PnL_coin

# least square regression (linear): y = alpha + beta*x
linReg = np.polyfit(x=arr_PnL_benchmark, y=arr_PnL_coin, deg=1)

alpha = linReg[1] # this is the y-intercept, not needed
beta  = linReg[0] # this is the slope, which also is the multiplier
multiplier = beta
print("multiplier            : ", multiplier)

#-----------------------------------------#
#          calculate correlation          #   
#-----------------------------------------#
correlation = pearsonr(arr_PnL_benchmark, arr_PnL_coin)
print("correlation            :", correlation[0])