# from math import pi

# from bokeh.io import curdoc
# from bokeh.layouts import column, row
# from bokeh.models import ColumnDataSource, Select, DataTable, TableColumn, Div, Span, Label
# from bokeh.plotting import  figure, output_file, show

# import pandas as pd
# import numpy as np
# import yfinance as yf

# DEFAULT_TICKERS = ["AAPL", "GOOG", "MSFT", "NFLX", "TSLA"]

# START, END = "2018-01-01" , "2021-01-01"


# def load_ticker(tickers) :
#     df = yf.download(tickers, start=START, end=END)
#     return df["Close"].dropna()


# def get_data( t1, t2):
#     d = load_ticker(DEFAULT_TICKERS)
#     df = d[[t1, t2]]
#     returns = df.pct_change().add_suffix("_returns")

#     df = pd.concat([df, returns], axis = 1)
#     df.rename(columns={t1:"t1", t2:"t2" , t1+"_returns" : "t1_returns", t2+"_returns" : "t2_returns"})
#     return df.dropna()

# def nix(val, lst):
#     return [x for x in lst if x!= val]

# tickers1 = Select(value = "AAPL", options = nix("GOOG", DEFAULT_TICKERS))
# tickers2 = Select(value = "GOOG", options = nix("AAPL", DEFAULT_TICKERS))

# # Source Date 
# data = get_data( tickers1.value, tickers2.value)

# print(data.head())

# source = ColumnDataSource(data=data)

# ## Descriptive stats

# stats = round(data.describe().reset_index(), 2)
# stats_source = ColumnDataSource(data=data)
# stat_columns = [TableColumn(field=col, title=col) for col in stats.columns]
# data_table = DataTable(source= stats_source, columns=stat_columns, width = 350, height=350, index_position = None)
# # Plots

# corr_tools = "pan ,wheel_zoom, box_select, reset"
# tools = "pen , wheel_zoom, xbox_select, reset"

# corr = figure(width=350, height=350, tools=corr_tools)

# corr.circle("t1_returns", "t2_returns", size=2, source=source, selection_color="firbrick", alpha=0.6, nonselection_alpha=0.1, selection_alpha=0.4)

# show(corr)

# ts1 = figure(width=700, height=250, tools=tools, x_axis_type="datetime", 
#              active_drag="xbox_select")

# ts1.line("Date", "t1", source=source)
# ts1.circle("Date", "t1", size=1, source=source, color=None, selection_color= "firebrick")

# ts2 = figure(width=700, height=250, tools=tools, x_axis_type="datetime", active_drag="xbox_select")
# ts2.x_range = ts1.x_range
# ts2.line("Date" , "t2", source=source)
# ts2.circle("Date", "t2", size=1, source=source, color=None, selection_color= "firebrick")

# show(column(ts1,ts2))

# # CallBack

# def ticker1_change(attrname, old, new):
#     tickers2.options = nix(new, DEFAULT_TICKERS)
#     update()

# def ticker2_change(attrname, old , new):
#     tickers1.options = nix(new, DEFAULT_TICKERS)

# def update():
#     t1, t2 = tickers1.value , tickers2.value
#     df = get_data(t1, t2)
#     source.data = df
#     stats_source.data = round(df.describe().reset_index(), 2)
#     corr.title.text = "%s returns vs. %s returns" % (t1, t2)
#     ts1.title.text, ts2.title.text = t1, t2

# tickers1.on_change('value', ticker1_change)
# tickers2.on_change('value', ticker2_change)  

# # Layouts

# widgets = column(tickers1, tickers2, data_table)
# main_row = row(corr, widgets)
# series = column(ts1,ts2)
# layout = column(main_row, series)

# # Bokeh Server

# curdoc().add_root(layout)
# curdoc().title = "Stock Dashboard"

from math import pi

from bokeh.server.server import Server
from bokeh.application import Application
from bokeh.application.handlers import FunctionHandler

from bokeh.io import curdoc
from bokeh.layouts import column, row
from bokeh.models import ColumnDataSource, Select, DataTable, TableColumn, Div, Span, Label
from bokeh.plotting import figure, output_file, show

import pandas as pd
import numpy as np
import yfinance as yf

DEFAULT_TICKERS = ["AAPL", "GOOG", "MSFT", "NFLX", "TSLA"]

START, END = "2018-01-01", "2021-01-01"

def load_ticker(tickers):
    df = yf.download(tickers, start=START, end=END)
    return df["Close"].dropna()

def get_data(t1, t2):
    d = load_ticker(DEFAULT_TICKERS)
    df = d[[t1, t2]]
    returns = df.pct_change().add_suffix("_returns")
    df = pd.concat([df, returns], axis=1)
    df = df.rename(columns={t1: "t1", t2: "t2", t1 + "_returns": "t1_returns", t2 + "_returns": "t2_returns"})
    return df.dropna()

def nix(val, lst):
    return [x for x in lst if x != val]

tickers1 = Select(value="AAPL", options=nix("GOOG", DEFAULT_TICKERS))
tickers2 = Select(value="GOOG", options=nix("AAPL", DEFAULT_TICKERS))

print("tickers1")
print(tickers1.value)

print("tickers2")
print(tickers2.value)
# Source Data
data = get_data(tickers1.value, tickers2.value)
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

corr = figure(width=350, height=350, tools=corr_tools)
# corr.circle("t1_returns", "t2_returns", size=2, source=source, selection_color="firebrick", alpha=0.6, nonselection_alpha=0.1, selection_alpha=0.4)
corr.scatter("t1_returns", "t2_returns", size=2, source=source, marker="circle", selection_color="firebrick", alpha=0.6, nonselection_alpha=0.1, selection_alpha=0.4)

ts1 = figure(width=700, height=250, tools=tools, x_axis_type="datetime", active_drag="xbox_select")
ts1.line("Date", "t1", source=source)
# ts1.circle("index", "t1", size=1, source=source, color=None, selection_color="firebrick")
ts1.scatter("Date", "t1", size=1, source=source, marker="circle", color=None, selection_color="firebrick")

ts2 = figure(width=700, height=250, tools=tools, x_axis_type="datetime", active_drag="xbox_select")
ts2.x_range = ts1.x_range
ts2.line("Date", "t2", source=source)
# ts2.circle("index", "t2", size=1, source=source, color=None, selection_color="firebrick")
ts2.scatter("Date", "t2", size=1, source=source, marker="circle", color=None, selection_color="firebrick")

# show(column(ts1, ts2))

# Callbacks
def ticker1_change(attrname, old, new):
    tickers2.options = nix(new, DEFAULT_TICKERS)
    update()

def ticker2_change(attrname, old, new):
    tickers1.options = nix(new, DEFAULT_TICKERS)
    update()

def update():
    t1, t2 = tickers1.value, tickers2.value
    df = get_data(t1, t2)
    source.data = df
    stats_source.data = round(df.describe().reset_index(), 2)
    corr.title.text = f"{t1} returns vs. {t2} returns"
    ts1.title.text, ts2.title.text = t1, t2

tickers1.on_change('value', ticker1_change)
tickers2.on_change('value', ticker2_change)

# Layouts
widgets = column(tickers1, tickers2, data_table)
main_row = row(corr, widgets)
series = column(ts1, ts2)
layout = column(main_row, series)

# Bokeh Server
curdoc().add_root(layout)
curdoc().title = "Stock Dashboard"


# terminal
#bokeh serve --show bokeh_test_f.py
# python -m bokeh serve --show bokeh_test_f.py      

   