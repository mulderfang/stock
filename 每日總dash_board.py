from math import pi

from bokeh.server.server import Server
from bokeh.application import Application
from bokeh.application.handlers import FunctionHandler

from bokeh.io import curdoc
from bokeh.layouts import column, row
from bokeh.models import ColumnDataSource, Select, DataTable, TableColumn, Div, Span, Label, DatetimeTickFormatter, TextInput, CDSView, IndexFilter
from bokeh.plotting import figure, output_file, show

import pandas as pd
import numpy as np
import yfinance as yf

import twstock
from datetime import datetime, timedelta
from datetime import date
import calendar
import time
import pandas as pd
import numpy as np
import requests
import pandas as pd

from sqlalchemy import create_engine
import collections

DEFAULT_TICKERS = ["5263", "3006", "9956", "2330"]


# 設置MySQL資料庫連接
db_user = 'root'
db_password = '19970730'
db_host = '127.0.0.1'
db_name = 'sql_stock'

# 創建資料庫連接引擎
engine = create_engine(f'mysql+mysqlconnector://{db_user}:{db_password}@{db_host}/{db_name}')

# 計算個股3個月營收及12個月營收
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

# 查 指數與每日漲跌幅家數
def fetch_stock_data(start_date: str, end_date: str):
    # # 設置MySQL資料庫連接
    # db_user = 'root'
    # db_password = '19970730'
    # db_host = '127.0.0.1'
    # db_name = 'sql_stock'

    # # 創建資料庫連接引擎
    # engine = create_engine(f'mysql+mysqlconnector://{db_user}:{db_password}@{db_host}/{db_name}')
    
    # SQL 查詢 加權指數
    query_twse = f"""
    SELECT *
    FROM daily_twse
    WHERE date >= '{start_date}' AND date <= '{end_date}'
    """

    # SQL 查詢 個股
    query_updown = f"""
    SELECT *
    FROM daily_updown
    WHERE date >= '{start_date}' AND date <= '{end_date}'
    """

    # 執行查詢並返回 DataFrame
    twse_df_raw = pd.read_sql(query_twse, engine)
    twse_df_raw = twse_df_raw.sort_values(by='Date', ascending=False)
    twse_updown_raw = pd.read_sql(query_updown, engine)
    twse_updown_raw = twse_updown_raw.sort_values(by='Date', ascending=False)

    return twse_df_raw, twse_updown_raw

# 查 每天在 5 20 60MA上的股票個數
def calculate_MA_counts(one_year_ago_str, today_str):

    # # 設置MySQL資料庫連接
    # db_user = 'root'
    # db_password = '19970730'
    # db_host = '127.0.0.1'
    # db_name = 'sql_stock'

    # # 創建資料庫連接引擎
    # engine = create_engine(f'mysql+mysqlconnector://{db_user}:{db_password}@{db_host}/{db_name}')

    # SQL 查詢 個股
    query_stock = f"""
    SELECT *
    FROM daily_price
    WHERE date >= '{one_year_ago_str}' AND date <= '{today_str}'
    """

    # 使用 pd.read_sql 來執行查詢並讀取數據到 DataFrame


    stock_df_raw = pd.read_sql(query_stock, engine)

    # 確保日期欄位是日期格式
    stock_df_raw['Date'] = pd.to_datetime(stock_df_raw['Date'])
    
    # 統計每個日期的出現次數，並按日期降序排列
    data_date_list = collections.Counter(stock_df_raw['Date'].sort_values(ascending=False))
    
    # 初始化 MA 計數結果的 DataFrame
    MA_count_list = pd.DataFrame()

    # 遍歷每個日期，計算 MA 數據
    for day in data_date_list:
        day = day.strftime('%Y-%m-%d')  # 轉換為字符串格式
        oneday_list = stock_df_raw[stock_df_raw['Date'] == day].copy()  # 取出對應日期的數據
        MA_count = len(oneday_list)  # 當日的總記錄數
        
        if MA_count == 0:
            continue  # 如果當天沒有數據，跳過
        
        # 計算不同 MA 的符合條件的數據比例
        MA5_count = len(oneday_list[oneday_list['收盤價'] > oneday_list['5MA']])
        MA20_count = len(oneday_list[oneday_list['收盤價'] > oneday_list['20MA']])
        MA60_count = len(oneday_list[oneday_list['收盤價'] > oneday_list['60MA']])

        # 構建當天的 MA 統計數據
        MA_count_data = {
            "date": [day],
            "MA5_count": [round(100 * MA5_count / MA_count, 2)],
            "MA20_count": [round(100 * MA20_count / MA_count, 2)],
            "MA60_count": [round(100 * MA60_count / MA_count, 2)]
        }

        # 將當天的結果轉為 DataFrame 並加入總結果
        MA_count_list_oneday = pd.DataFrame(MA_count_data)
        MA_count_list = pd.concat([MA_count_list, MA_count_list_oneday], ignore_index=True)

    return MA_count_list

# 查 指數與小外資與大外資
def process_stock_data(one_year_ago_str: str, today_str: str):

    # SQL 查詢 加權指數
    query_daily_tx = f"""
    SELECT *
    FROM daily_tx
    WHERE date >= '{one_year_ago_str}' AND date <= '{today_str}'
    """

    query_tif_investors = f"""
    SELECT *
    FROM daily_tif_investors
    WHERE date >= '{one_year_ago_str}' AND date <= '{today_str}'
    """

    # 使用 pd.read_sql 來執行查詢並讀取數據到 DataFrame
    tif_investors_df_raw = pd.read_sql(query_tif_investors, engine)
    tx_df_raw = pd.read_sql(query_daily_tx, engine)

    # 五大特定淨部位計算
    top5_sp = tif_investors_df_raw['買方前五大交易人合計'] - tif_investors_df_raw['賣方前五大交易人合計']
    big_foreign = top5_sp - tif_investors_df_raw['投信未平倉餘額口數']
    small_foreign = tif_investors_df_raw['外資未平倉餘額口數'] - big_foreign

    # 添加 '大外資' 和 '小外資' 列
    tif_investors_df_raw['大外資'] = big_foreign
    tif_investors_df_raw['大外資變化'] = tif_investors_df_raw['大外資'].diff(1)
    tif_investors_df_raw['小外資'] = small_foreign
    tif_investors_df_raw['小外資變化'] = tif_investors_df_raw['小外資'].diff(1)
    tif_investors_df_raw.dropna()
    tif_investors_df_raw = tif_investors_df_raw.sort_values(by='Date', ascending=False)

    # 将 'Date' 列转换为 datetime 类型
    tif_investors_df_raw['Date'] = pd.to_datetime(tif_investors_df_raw['Date'])
    tx_df_raw['Date'] = pd.to_datetime(tx_df_raw['Date'])

    # 计算移动平均线 (MA)
    tx_df_raw['5MA'] = round(tx_df_raw['最後成交價'].rolling(5).mean(),2)
    tx_df_raw['8MA'] = round(tx_df_raw['最後成交價'].rolling(8).mean(),2)
    tx_df_raw['10MA'] = round(tx_df_raw['最後成交價'].rolling(10).mean(),2)
    tx_df_raw['20MA'] = round(tx_df_raw['最後成交價'].rolling(20).mean(),2)
    tx_df_raw['60MA'] = round(tx_df_raw['最後成交價'].rolling(60).mean(),2)
    tx_df_raw['240MA'] = round(tx_df_raw['最後成交價'].rolling(240).mean(),2)

    # 填充缺失值
    tx_df_raw = tx_df_raw.fillna(0)

    merged_df = pd.merge(tif_investors_df_raw, tx_df_raw, on='Date', how='inner')

    return merged_df


# 外部呼叫的範例
# 設置日期範圍
today = datetime.now().date()
two_years_ago = today - timedelta(days=730)

# 格式化日期
today_str = today.strftime('%Y%m%d')
two_years_ago_str = '20240101'  # 你可以靜態設置或者動態計算

# 呼叫函數來獲取資料
twse_df_raw, twse_updown_raw = fetch_stock_data(two_years_ago_str, today_str)

# 呼叫函數來獲取資料
MA_count_list= calculate_MA_counts(two_years_ago_str, today_str)

# 調用函數
merged_df = process_stock_data(two_years_ago_str, today_str)


# 文字說明

cast = TextInput(title="Input stock_id for revenue plot")


data = pd.DataFrame() 
# Source Data
if (cast.value.strip() != ""):
    data = get_data(cast.value.strip())
    print("======yes======")
else:
    data = get_data("2330")
    print("======no======")

source = ColumnDataSource(data=data)

# 看最近的漲跌家數
twse_title = Div(text="<h2>Total Number of Advancing and Declining Stocks</h2>", width=400)
twse_updown_5day =  twse_updown_raw.head(10)[['Date','總上漲家數','總漲停家數', '總下跌家數', '總跌停家數', '總持平家數', '總上漲下跌比']]
twse_updown_5day['Date'] = twse_updown_5day['Date'].astype(str)
stats_source2 = ColumnDataSource(data=twse_updown_5day)
stat_columns2 = [TableColumn(field=col, title=col) for col in twse_updown_5day.columns]
data_table2 = DataTable(source=stats_source2, columns=stat_columns2, width=1000, height=350, index_position=None)

# 看最近在60MA以上股票個數
MA_count_title = Div(text="""
<h2>Percentage of Stocks Above the Quarterly Moving Average</h2>
<br>
<p style="font-size:16px;"> 觀察MA60_count < 26，隔天開盤進場，放26個交易日後出場(putday =25所以是第26)</p>
""", width=400)
MA_count_list_5day =  MA_count_list.head(10)
today_count = MA_count_list_5day['MA60_count'].iloc[0]
# 確認今天的盤勢狀態
if today_count > 26:
    situation_text = "今日收盤在季線上的個股數大於26%"
    color = "red"
else:
    situation_text = "今日收盤在季線上的個股數小於26%"
    color = "green"

# 创建 Div
today_count = Div(text=f"""
<p style="font-size:16px; color:{color};">{situation_text}</p>
""", width=400)

stats_source3 = ColumnDataSource(data=MA_count_list_5day)
stat_columns3 = [TableColumn(field=col, title=col) for col in MA_count_list_5day.columns]
data_table3 = DataTable(source=stats_source3, columns=stat_columns3, width=1000, height=350, index_position=None)

# 小外資增減
tif_investors_title = Div(text="""
<h2>Daily Foreign Investor Trends</h2>
<br>
<p style="font-size:16px;">直接看大盤十日線，十日以上買，以下賣。做空多看季線，季線以上不做空</p>
""", width=400)

merged_df =  merged_df.head(10)[['Date','大外資','大外資變化', '小外資', '小外資變化','最後成交價','10MA','20MA','60MA']]

today_price = merged_df['最後成交價'].iloc[0]
today_60MA = merged_df['60MA'].iloc[0]
today_10MA = merged_df['10MA'].iloc[0]

# 確認今天的盤勢狀態
if today_price > today_60MA:
    if today_price > today_10MA:
        situation_text = "今日收盤在季線之上，10日線上"
        color = "red"
    else:
        situation_text = "今日收盤在季線之上，10日線下"
        color = "orange"
else:
    if today_price > today_10MA:
        situation_text = "今日收盤在季線之下，10日線上"
        color = "green"
    else:
        situation_text = "今日收盤在季線之下，10日線下"
        color = "lime"

# 创建 Div
today_tif= Div(text=f"""
<p style="font-size:16px; color:{color};">{situation_text}</p>
""", width=400)
merged_df['Date'] = merged_df['Date'].astype(str)
stats_source4 = ColumnDataSource(data=merged_df)
stat_columns4 = [TableColumn(field=col, title=col) for col in merged_df.columns]
data_table4 = DataTable(source=stats_source4, columns=stat_columns4, width=1000, height=350, index_position=None)


# Descriptive stats
Descriptiv_title = Div(text="""
<h2>Descriptive stats</h2>
<br>
<p style="font-size:16px;">三個月營收和十二個月營收年增率敘述統計</p>
""", width=400)

describ_data = data.describe().reset_index()
stats = round(describ_data, 2)
stats_source = ColumnDataSource(data=stats)
stat_columns = [TableColumn(field=col, title=col) for col in stats.columns]
data_table = DataTable(source=stats_source, columns=stat_columns, width=1000, height=350, index_position=None)

# Plots
corr_tools = "pan, wheel_zoom, box_select, reset"
tools = "pan, wheel_zoom, xbox_select, reset"

# view = CDSView(source=source, filters = [IndexFilter([0,2,4])])

ts1 = figure(x_axis_type="datetime", width=800, height=400, title="3months_avg revenue vs 12months_avg revenue", 
           tools="pan,wheel_zoom,box_zoom,reset,save", toolbar_location="above")

# 添加3个月数据的线条和圆圈
ts1.line('ym', 'YoY_3', source=source, color='skyblue', legend_label='3month', line_width=2)
ts1.scatter('ym', 'YoY_3', source=source, marker="circle", color='skyblue', size=8)

# 添加12个月数据的线条和圆圈
ts1.line('ym', 'YoY_12', source=source, color='pink', legend_label='12month', line_width=2)
ts1.scatter('ym', 'YoY_12', source=source, marker="circle", color='pink', size=8)


# Callbacks function
def update():
    t1 = ""
    if (cast.value.strip() != ""):
        t1 = cast.value.strip()
        print("======yes==t1====")
        print(t1)
    else:
        t1 = "2330"
        print("======no======")

    print("======t1=======")    
    print(t1)
    df = get_data(t1)
    source.data = df
    stats_source.data = round(df.describe().reset_index(), 2)
    ts1.title.text = t1

cast.on_change('value', lambda attr, old, new: update())

update()

# Layouts
# Add data_table2 to the widgets column
widgets = column(twse_title, data_table2, MA_count_title,today_count, data_table3, tif_investors_title,today_tif, data_table4, Descriptiv_title, cast , data_table, spacing=0)
main_row = row(widgets)
series = column(ts1, spacing=1)
layout = column(main_row, series, spacing=1)

# Bokeh Server
curdoc().add_root(layout)
curdoc().title = "Stock Dashboard"


# terminal
#bokeh serve --show bokeh_test_f.py
# python -m bokeh serve --show bokeh_test_input.py      

   