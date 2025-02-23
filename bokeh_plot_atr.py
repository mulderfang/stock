from bokeh.plotting import figure, show
from bokeh.models import ColumnDataSource, LinearAxis, Range1d
from bokeh.layouts import gridplot
from bokeh.io import output_notebook
import pandas as pd
import warnings
# 忽略 FutureWarning
warnings.simplefilter(action='ignore', category=FutureWarning)

start_date_his = '20210101'

# SQL 查詢 個股
query_stock = f"""
SELECT *
FROM daily_price
WHERE date >= '{start_date_his}' 
"""

# 使用 pd.read_sql 來執行查詢並讀取數據到 DataFrame

stock_df_raw = pd.read_sql(query_stock, engine)

# 確保日期欄位是日期格式
stock_df_raw['Date'] = pd.to_datetime(stock_df_raw['Date'])

plot_size = 100
x_lab = 2

n = 20
stock_id = '5263'

stock_2330 = stock_df_raw[stock_df_raw['證券代號'] == stock_id].copy()
stock_2330.loc[:, '前一天收盤價'] = stock_2330['收盤價'].shift(1)

# 計算當天的最高價與最低價之差
stock_2330['diff'] = stock_2330['最高價'] - stock_2330['最低價']
# 計算三種可能的TR值
range1 = stock_2330['diff']
range2 = (stock_2330['最高價'] - stock_2330['前一天收盤價']).abs()
range3 = (stock_2330['最低價'] - stock_2330['前一天收盤價']).abs()

# 取三者中的最大值作為TR值
stock_2330['TR'] = pd.concat([range1, range2, range3], axis=1).max(axis=1)
stock_2330 = stock_2330.dropna()
stock_2330['ATR'] = round(stock_2330['TR'].rolling(window=n).mean() , 2)

# 計算 ATR
# stock_2330['ATR'] = round((stock_2330['ATR'].shift(1) * (n - 1) + stock_2330['TR']) / n , 2)


stock_2330 = stock_2330.dropna()

stock_2330['ATR_high'] = stock_2330['20MA'] + 1.5 * stock_2330['ATR']
stock_2330['ATR_low']  = stock_2330['20MA'] - 1.5 * stock_2330['ATR']


tx_df_forplot = stock_2330.copy()

tx_df_forplot.rename(columns={'開盤價': 'Open', '最高價': 'High', '最低價': 'Low', '收盤價': 'Close'}, inplace=True)
tx_df_forplot.set_index('Date', inplace=True)

stock_2330_forplot = stock_2330.copy()
stock_2330_forplot.set_index('Date', inplace=True)
stock_2330_forplot.index = stock_2330_forplot.index.strftime('%Y-%m-%d')

tx_df_raw_100 = tx_df_forplot.tail(plot_size)
stock_2330_100 = stock_2330_forplot.tail(plot_size)

ohlc_data = tx_df_raw_100.reset_index()
stock_2330_100 = stock_2330_100.reset_index()

# Set up ColumnDataSource for Bokeh
source = ColumnDataSource(data=dict(
    date=ohlc_data['Date'],
    open=ohlc_data['Open'],
    high=ohlc_data['High'],
    low=ohlc_data['Low'],
    close=ohlc_data['Close'],
    tr=stock_2330_100['TR'],
    atr=stock_2330_100['ATR'],
    ma20=stock_2330_100['20MA'],
    atr_high=stock_2330_100['ATR_high'],
    atr_low=stock_2330_100['ATR_low']
))

# Configure the main figure for the candlestick and line charts
p = figure(x_axis_type="datetime", width=1000, height=500, title="ATR and Stock Price Analysis",
           toolbar_location="above")

# Set up the second y-axis for the ATR data
p.extra_y_ranges = {"ATR": Range1d(start=0, end=stock_2330_100['ATR'].max() * 1.2)}
p.add_layout(LinearAxis(y_range_name="ATR", axis_label="ATR"), 'right')

# Draw the candlestick chart
width = 12*60*60*1000  # 12 hours in ms for half-day width candles
inc = ohlc_data['Close'] > ohlc_data['Open']
dec = ohlc_data['Open'] > ohlc_data['Close']

# Rising candles
p.segment(x0='date', y0='high', x1='date', y1='low', source=source, color="red")
p.vbar(x='date', width=width, top='Open', bottom='Close', source=source, color="red", line_color="red", 
       view=source.to_df().query("Close > Open").index)

# Falling candles
p.segment(x0='date', y0='high', x1='date', y1='low', source=source, color="green")
p.vbar(x='date', width=width, top='Close', bottom='Open', source=source, color="green", line_color="green", 
       view=source.to_df().query("Open > Close").index)

# Add moving averages and ATR bands
p.line('date', 'ma20', source=source, color="blue", legend_label="20MA", line_width=2, alpha=0.5)
p.line('date', 'atr_high', source=source, color="orange", legend_label="ATR High", line_width=2, alpha=0.5)
p.line('date', 'atr_low', source=source, color="green", legend_label="ATR Low", line_width=2, alpha=0.5)

# Add ATR bars on the second y-axis
p.vbar(x='date', top='tr', width=width, source=source, color="purple", alpha=0.2, y_range_name="ATR", 
       legend_label="TR")
p.line('date', 'atr', source=source, color="#2C3539", legend_label="ATR", line_width=2, alpha=0.5, 
       y_range_name="ATR")

# Customize plot aesthetics
p.xaxis.major_label_orientation = 3.14 / 4  # Rotate x-axis labels
p.xaxis.axis_label = "Date"
p.yaxis.axis_label = stock_id
p.legend.location = "top_left"

# Show plot
show(p)