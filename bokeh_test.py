from math import pi

from bokeh.io import curdoc
from bokeh.layouts import column
from bokeh.models import ColumnDataSource, Select, Div, Span, Label
from bokeh.plotting import  figure, output_file, show
import pandas as pd
import numpy as np

# 示例数据框（替换为你的实际数据）
data = {
    '證券代號': ['2330', '2330', '2330', '2303', '2303', '2303'],
    '開盤價': [100, 101, 102, 50, 51, 52],
    '最高價': [105, 106, 107, 55, 56, 57],
    '最低價': [95, 96, 97, 45, 46, 47],
    '日期': pd.date_range(start='2020-01-01', periods=6)
}
stock_df_raw = pd.DataFrame(data)

# 创建ColumnDataSource
source = ColumnDataSource(data=dict(left=[], right=[], top=[]))

# 创建图表
p = figure(height=350, width=800, title="Stock Amplitude", x_axis_label="Amplitude (%)", y_axis_label="Count")
p.quad(top='top', bottom=0, left='left', right='right', source=source, fill_color='lightblue', line_color='black', alpha=0.7)

# 创建下拉菜单
select = Select(title="Select Stock No:", value="2330", options=stock_df_raw['證券代號'].unique().tolist())

# 更新函数
def update(attr, old, new):
    stock_no = select.value
    stock = stock_df_raw[stock_df_raw['證券代號'] == stock_no]
    stock['震幅'] = 100 * (stock['最高價'] - stock['最低價']) / stock['開盤價']
    
    mean_amplitude = stock['震幅'].mean()
    std_amplitude = stock['震幅'].std()
    
    hist, edges = np.histogram(stock['震幅'], bins=100)
    
    new_data = {
        'top': hist,
        'left': edges[:-1],
        'right': edges[1:]
    }
    source.data = new_data
    
    # 更新图表标题
    p.title.text = f"{stock_no} Amplitude Plot"
    
    # 绘制均值和标准差
    p.renderers = p.renderers[:1]  # 保留直方图，删除其他
    mean_span = Span(location=mean_amplitude, dimension='height', line_color='red', line_dash='dashed', line_width=1)
    std_span = Span(location=mean_amplitude + std_amplitude, dimension='height', line_color='blue', line_dash='dashed', line_width=1)
    
    p.add_layout(mean_span)
    p.add_layout(Label(x=mean_amplitude, y=max(hist) * 0.9, text=f'μ\n{mean_amplitude:.2f}', text_color='red', text_align='center'))
    p.add_layout(std_span)
    p.add_layout(Label(x=mean_amplitude + std_amplitude, y=max(hist) * 0.8, text=f'μ + σ\n{mean_amplitude + std_amplitude:.2f}', text_color='blue', text_align='center'))

select.on_change('value', lambda attr, old, new: update())

# 初始更新
update(None, None, None)

# 创建布局
layout = column(Div(text="<h1>Interactive Stock Amplitude Plot</h1>"), select, p)
controls = column(select, width=300)
# 添加到当前文档
curdoc().add_root(layout,controls)
curdoc().title = "Stock Amplitude Plot"
