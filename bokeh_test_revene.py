
from math import pi

from bokeh.server.server import Server
from bokeh.application import Application
from bokeh.application.handlers import FunctionHandler

from bokeh.io import curdoc
from bokeh.layouts import column, row
from bokeh.models import ColumnDataSource, Select, DataTable, TableColumn, Div, Span, Label, DatetimeTickFormatter, TextInput
from bokeh.plotting import figure, output_file, show

import pandas as pd
import numpy as np
import yfinance as yf

import twstock
from datetime import datetime
from datetime import date
import calendar
import time
import pandas as pd
import numpy as np
import requests
import pandas as pd

# 可以研究看看 Streamlit 
# 可以研究看看 Shiny 

DEFAULT_TICKERS = ["5263", "3006", "9956", "2330"]


def get_data(stock_id):

    token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJkYXRlIjoiMjAyMy0wMi0yMyAyMjoxNzoyNiIsInVzZXJfaWQiOiJtdWxkZXIiLCJpcCI6IjIwMy4yMDQuMTkzLjEwNCJ9.K95hVEFR_KVdOG2zdeFMC2DCydLAhEP4MjS97Fvt7UQ"

    url = "https://api.finmindtrade.com/api/v4/data"
    parameter = {
        "dataset": "TaiwanStockInfo",
        "token": token, # 參考登入，獲取金鑰
    }
    resp = requests.get(url, params=parameter)
    info = resp.json()
    info = pd.DataFrame(info["data"]) #個股基本資料


    data_id = stock_id

    # 計算Revenue
    parameter = {
        "dataset": "TaiwanStockMonthRevenue",
        "data_id": data_id,
        "start_date": "2016-01-01",
        "token": token, # 參考登入，獲取金鑰
    }
    data = requests.get(url, params=parameter)
    data = data.json()
    data = pd.DataFrame(data['data'])
    #print(data.head())

    # 計算股價
    parameter = {
    "dataset": "TaiwanStockPrice",
    "data_id": data_id,
    "start_date": "2016-01-01",
    "token": token
    }
    resp2 = requests.get(url, params=parameter)
    data2 = resp2.json()
    data2 = pd.DataFrame(data2["data"])
    #print(data2.head())

    month_stock = {
        "date": [],
        "year": [],
        "month": [],
        "stock_id": [],
        "stock_name": [],
        "month_price": []
    }
    
    month_stock = pd.DataFrame(month_stock) 


    data2['Year'] = data2['date'].apply(lambda r: r.split('-')[0])
    data2['Month'] = data2['date'].apply(lambda r: r.split('-')[1])
    data2['Day'] = data2['date'].apply(lambda r: r.split('-')[2])

    #計算月均價
    month_avy = round(data2.groupby(['Year','Month'])['close'].sum()/data2.groupby(['Year','Month']).count()['date'],2)
    month_avy = month_avy.to_frame()
    month_avy = month_avy.rename(columns={0:'month_price'})

    month_avy = month_avy.reset_index()
    month_avy['ym'] = month_avy.apply(lambda r: r['Year']+'-'+r['Month'].zfill(2), axis=1)
    #month_avy.set_index(pd.to_datetime(month_avy['ym'],format='%Y-%m'), inplace=True)



    # (近12個月營收總和 / 去年同期近3個月營收總和 - 1) * 100%
    revenue_dic = {
        "year": [],
        "month": [],
        "stock_id": [],
        "stock_name": [],
        "last12month": [],
        "lastyear": []
        }

    revenue_df12 = pd.DataFrame(revenue_dic) 

    for j in range(1,len(data)-22):
        sum_revenue1 = 0
        sum_revenue2 = 0

        temp_df1 = data.loc[len(data)-j,:]
        temp_info_df = temp_df1
        stock_id = temp_info_df['stock_id']

        tmep_info = info[info['stock_id'] == stock_id]['stock_name']
        tmep_info=tmep_info.reset_index(drop=True)

        for i in range(12):
            
            temp_df1 = data.loc[len(data)-j-i,:]      
            year1 = temp_df1['revenue_year']
            month1 = temp_df1['revenue_month']
            revenue1 = int(temp_df1['revenue'])
            sum_revenue1 += revenue1

            temp_df2 = data[(data['revenue_year'] == year1-1) & (data['revenue_month'] == month1) ]
            revenue2 = int(temp_df2['revenue'].iloc[0])
            sum_revenue2 += revenue2

        new_row = pd.DataFrame({
            'year': [str(year1)],
            'month': [str(month1)],
            'stock_id': [stock_id],
            'stock_name': [tmep_info[0]],
            'last12month': [sum_revenue1],
            'lastyear': [sum_revenue2]
        })

            #Add new ROW
        revenue_df12 = pd.concat([revenue_df12, new_row], ignore_index=True)   
        # revenue_df12=revenue_df12.append({'year' : str(temp_info_df['revenue_year']) , 'month' : str(temp_info_df['revenue_month']) , 'stock_id' : stock_id , 'stock_name' : tmep_info[0],
        #                             'last12month' : sum_revenue1 ,'lastyear' : sum_revenue2} , ignore_index=True)
            
    print(revenue_df12.head())        

    revenue_df12['YoY'] = round((revenue_df12['last12month']/revenue_df12['lastyear']-1)*100,2)
    revenue_df12['ym'] = revenue_df12.apply(lambda r: r['year']+'-'+r['month'].zfill(2), axis=1)

    revenue_dic = {
        "year": [],
        "month": [],
        "stock_id": [],
        "stock_name": [],
        "last3month": [],
        "lastyear": []
        }

    # (近3個月營收總和 / 去年同期近3個月營收總和 - 1) * 100%
    revenue_df3 = pd.DataFrame(revenue_dic) 

    for j in range(1,len(data)-13):
        sum_revenue1 = 0
        sum_revenue2 = 0

        temp_df1 = data.loc[len(data)-j,:]
        temp_info_df = temp_df1
        stock_id = temp_info_df['stock_id']

        tmep_info = info[info['stock_id'] == stock_id]['stock_name']
        tmep_info=tmep_info.reset_index(drop=True)

        for i in range(3):
            
            temp_df1 = data.loc[len(data)-j-i,:]      
            year1 = temp_df1['revenue_year']
            month1 = temp_df1['revenue_month']
            revenue1 = int(temp_df1['revenue'])
            sum_revenue1 += revenue1

            temp_df2 = data[(data['revenue_year'] == year1-1) & (data['revenue_month'] == month1) ]
            revenue2 = int(temp_df2['revenue'].iloc[0])
            sum_revenue2 += revenue2

        new_row2 = pd.DataFrame({
            'year': [str(year1)],
            'month': [str(month1)],
            'stock_id': [stock_id],
            'stock_name': [tmep_info[0]],
            'last3month': [sum_revenue1],
            'lastyear': [sum_revenue2]
        })

            #Add new ROW
        revenue_df3 = pd.concat([revenue_df3, new_row2], ignore_index=True)  
        # revenue_df3=revenue_df3.append({'year' : str(temp_info_df['revenue_year']) , 'month' : str(temp_info_df['revenue_month']) , 'stock_id' : stock_id , 'stock_name' : tmep_info[0],
        #                             'last3month' : sum_revenue1 ,'lastyear' : sum_revenue2} , ignore_index=True)
            
    print(revenue_df3.head()) 
    revenue_df3['YoY'] = round((revenue_df3['last3month']/revenue_df3['lastyear']-1)*100,2)
    revenue_df3['ym'] = revenue_df3.apply(lambda r: r['year']+'-'+r['month'].zfill(2), axis=1)



    from dateutil.relativedelta import relativedelta
    # 定义一个函数来转换年月并加上11个月
    def add_months(ym, months):
        date_obj = datetime.strptime(ym, "%Y-%m")
        new_date_obj = date_obj + relativedelta(months=months)
        return new_date_obj.strftime("%Y-%m")

    # 应用转换函数到 'ym' 列
    revenue_df12['ym'] = revenue_df12['ym'].apply(lambda x: add_months(x, 11))
    revenue_df3['ym'] = revenue_df3['ym'].apply(lambda x: add_months(x, 2))

    revenue_price_df = revenue_df12.merge(revenue_df3.loc[:,['ym','YoY']],on = 'ym',how = 'inner')#內連線，取交集
    #revenue_df3要跟revenue_df12一樣長

    revenue_price_df = revenue_price_df.merge(month_avy.loc[:,['ym','month_price']],on = 'ym',how = 'inner')#內連線，取交集
    revenue_price_df = revenue_price_df.rename(columns={'YoY_x':'YoY_12','YoY_y':'YoY_3'})

    revenue_price_df=revenue_price_df.sort_values(['ym'],ascending=True)
    revenue_price_df=revenue_price_df.reset_index(drop=True)
    revenue_price_df['ym'] = pd.to_datetime(revenue_price_df['ym']) #繪圖需轉換
    return revenue_price_df.dropna()


tickers1 = Select(title="Stock_id", value="5263", options=DEFAULT_TICKERS)

cast = TextInput(title="Cast names contains")

print("tickers1")
print(tickers1.value)

# Source Data
data = get_data(tickers1.value)
print("data")
print(data)

source = ColumnDataSource(data=data)

# Descriptive stats
stats = round(data.describe().reset_index(), 2)
stats_source = ColumnDataSource(data=stats)
stat_columns = [TableColumn(field=col, title=col) for col in stats.columns]
data_table = DataTable(source=stats_source, columns=stat_columns, width=350, height=350, index_position=None)

# Plots
corr_tools = "pan, wheel_zoom, box_select, reset"
tools = "pan, wheel_zoom, xbox_select, reset"

# corr = figure(width=350, height=350, tools=corr_tools)
# # corr.circle("t1_returns", "t2_returns", size=2, source=source, selection_color="firebrick", alpha=0.6, nonselection_alpha=0.1, selection_alpha=0.4)
# corr.scatter("t1_returns", "t2_returns", size=2, source=source, marker="circle", selection_color="firebrick", alpha=0.6, nonselection_alpha=0.1, selection_alpha=0.4)


ts1 = figure(x_axis_type="datetime", width=800, height=400, title="3months_avg vs 12months_avg", 
           tools="pan,wheel_zoom,box_zoom,reset,save", toolbar_location="above")

# 添加3个月数据的线条和圆圈
ts1.line('ym', 'YoY_3', source=source, color='skyblue', legend_label='3month', line_width=2)
ts1.scatter('ym', 'YoY_3', source=source, marker="circle", color='skyblue', size=8)

# 添加12个月数据的线条和圆圈
ts1.line('ym', 'YoY_12', source=source, color='pink', legend_label='12month', line_width=2)
ts1.scatter('ym', 'YoY_12', source=source, marker="circle", color='pink', size=8)


# ts2 = figure(width=700, height=250, tools=tools, x_axis_type="datetime", active_drag="xbox_select")
# ts2.x_range = ts1.x_range
# ts2.line("Date", "t2", source=source)
# # ts2.circle("index", "t2", size=1, source=source, color=None, selection_color="firebrick")
# ts2.scatter("Date", "t2", size=1, source=source, marker="circle", color=None, selection_color="firebrick")

# show(column(ts1, ts2))

# Callbacks


def ticker1_change(attrname, old, new):
    tickers1.options = DEFAULT_TICKERS
    update()

def update():
    t1 = tickers1.value
    df = get_data(t1)
    source.data = df
    stats_source.data = round(df.describe().reset_index(), 2)
    ts1.title.text = t1

tickers1.on_change('value', ticker1_change)

# Layouts
widgets = column(cast, tickers1, data_table)
main_row = row(widgets)
series = column(ts1)
layout = column(main_row, series)

# Bokeh Server
curdoc().add_root(layout)
curdoc().title = "Stock Dashboard"


# terminal
#bokeh serve --show bokeh_test_f.py
# python -m bokeh serve --show bokeh_test_f.py      

data = {'x': [1, 2, 3, 4], 'y': [4, 3, 2, 1]}
source = ColumnDataSource(data=data)

p = figure()
p.circle(x='x', y='y', size=10, source=source)
show(p)